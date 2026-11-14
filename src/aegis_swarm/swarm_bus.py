"""Qdrant-backed Shared Episodic & Procedural Memory Bus + 8-pattern secret/PII sanitizer."""

from __future__ import annotations

import asyncio
import hashlib
import math
import re
import uuid
from typing import Any

from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    FieldCondition,
    Filter,
    MatchValue,
    PointStruct,
    VectorParams,
)

from aegis_swarm.config import settings
from aegis_swarm.schemas import SwarmRuleRecord, SwarmRuleOutcomeOutput

_SECRET_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (
        re.compile(r"(?i)(authorization\s*[:=]\s*['\"]?bearer\s+)[a-zA-Z0-9_\-\.~+/]+=*"),
        r"\1[REDACTED_TOKEN]",
    ),
    (re.compile(r"(?i)(bearer\s+)[a-zA-Z0-9_\-\.~+/]{8,}=*"), r"\1[REDACTED_TOKEN]"),
    (re.compile(r"\bsk-[a-zA-Z0-9_\-]{10,}\b"), "[REDACTED_API_KEY]"),
    (re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"), "[REDACTED_AWS_KEY]"),
    (
        re.compile(
            r"(?i)\b(api[_-]?key|secret[_-]?key|access[_-]?token|auth[_-]?token|password|passwd|pwd|client[_-]?secret)"
            r"(\s*[:=]\s*['\"]?)(?!\[REDACTED_)([^'\"\s,&;}{]+)(['\"]?)"
        ),
        r"\1\2[REDACTED_SECRET]\4",
    ),
    (re.compile(r"([a-zA-Z0-9+.-]+://[^:\s/]+:)([^@\s/]+)(@)"), r"\1[REDACTED_PASSWORD]\3"),
    (re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"), "[REDACTED_EMAIL]"),
    (re.compile(r"\b\d{3}-\d{2}-\d{4}\b"), "[REDACTED_SSN]"),
]


def sanitize_swarm_text(raw_text: str) -> tuple[str, int]:
    """Scrub tokens, API keys, AWS keys, passwords, connection URIs, emails, and SSNs."""
    sanitized = raw_text
    total_replacements = 0
    for pattern, replacement in _SECRET_PATTERNS:
        sanitized, count = pattern.subn(replacement, sanitized)
        total_replacements += count
    return sanitized, total_replacements


def _embed_swarm_text(text: str, dim: int = 64) -> list[float]:
    """Deterministic sub-millisecond local embedding for zero-cost offline Qdrant vector search."""
    vec = [0.0] * dim
    tokens = [
        tok.strip(".,:;!?()[]{}\"'`-_/=*").lower()
        for tok in re.split(r"[\s,().=/]+", text)
        if tok.strip()
    ]
    if not tokens:
        tokens = [text.lower() or "swarm"]
    for tok in tokens:
        digest = hashlib.sha256(tok.encode("utf-8")).digest()
        for i in range(dim):
            vec[i] += (digest[i % len(digest)] / 127.5) - 1.0
    norm = math.sqrt(sum(v * v for v in vec)) or 1.0
    return [v / norm for v in vec]


def normalize_rule_length(rule: str, max_chars: int = 200) -> str:
    """Enforce single-line <= 200-character concise procedural rules."""
    first_line = next((ln.strip() for ln in rule.splitlines() if ln.strip()), "")
    cleaned = first_line.strip(" \"'`*-")
    if len(cleaned) > max_chars:
        cleaned = cleaned[: max_chars - 3].rstrip() + "..."
    return cleaned


def synthesize_swarm_rule(
    *,
    error_type: str,
    sanitized_error: str,
    sanitized_payload: str,
    scope_tag: str,
) -> str:
    """Distill a deterministic <=200-char cross-agent procedural rule from a failure."""
    err_low = sanitized_error.lower()
    if "syntaxerror" in err_low or "ast_syntax" in err_low or "rejected_syntax" in err_low:
        return normalize_rule_length(
            f"Run Python ast.parse verification before submitting artifacts on [{scope_tag}]; avoid truncated blocks or unmatched brackets."
        )
    if "429" in err_low or "rate limit" in err_low or "rate_limit" in err_low:
        return normalize_rule_length(
            f"HTTP 429 rate-limit encountered on [{scope_tag}]; downgrade non-critical subtasks to FAST_CLOUD or LOCAL_WORKER."
        )
    if "lock_conflict" in err_low or "collision" in err_low:
        return normalize_rule_length(
            f"Parallel lock conflict on [{scope_tag}]; serialize overlapping file mutations into separate DAG waves."
        )
    if "secret" in err_low or "redacted" in err_low:
        return normalize_rule_length(
            f"Secret/PII detected on [{scope_tag}]; load credentials from env vars (os.environ) rather than inline literals."
        )
    if "budget" in err_low:
        return normalize_rule_length(
            f"Token/USD budget ceiling hit on [{scope_tag}]; split node objective and route boilerplate/tests to LOCAL_WORKER."
        )
    snippet = sanitized_error.strip().replace("\n", " ")[:90]
    return normalize_rule_length(
        f"Prevent {error_type} on [{scope_tag}] ({snippet}): validate interface signatures and constraints first."
    )


class SharedEpisodicBus:
    """Shared Qdrant procedural memory across all DAG waves and swarm sub-agents."""

    def __init__(
        self,
        *,
        in_memory: bool | None = None,
        qdrant_host: str | None = None,
        qdrant_port: int | None = None,
        collection_name: str | None = None,
        prune_threshold: float | None = None,
    ) -> None:
        use_mem = settings.qdrant_in_memory if in_memory is None else in_memory
        self._collection = collection_name or settings.collection_name
        self._prune_threshold = (
            settings.prune_threshold if prune_threshold is None else prune_threshold
        )
        self._vector_dim = 64

        if use_mem:
            self._qdrant = QdrantClient(location=":memory:")
        else:
            self._qdrant = QdrantClient(
                host=qdrant_host or settings.qdrant_host,
                port=qdrant_port or settings.qdrant_port,
            )

        self._ensure_collection_sync()
        self.memory_queries: int = 0
        self.memory_hits: int = 0
        self.cumulative_recall_ms: float = 0.0
        self.pruned_rules_count: int = 0

    def _ensure_collection_sync(self) -> None:
        existing = [c.name for c in self._qdrant.get_collections().collections]
        if self._collection not in existing:
            self._qdrant.create_collection(
                collection_name=self._collection,
                vectors_config=VectorParams(size=self._vector_dim, distance=Distance.COSINE),
            )

    def _find_by_fingerprint_sync(self, fingerprint: str) -> str | None:
        pts, _ = self._qdrant.scroll(
            collection_name=self._collection,
            scroll_filter=Filter(
                must=[FieldCondition(key="fingerprint", match=MatchValue(value=fingerprint))]
            ),
            limit=1,
        )
        return str(pts[0].id) if pts else None

    def _upsert_rule_sync(
        self,
        *,
        point_id: str,
        agent_id: str,
        scope_tag: str,
        category: str,
        rule_text: str,
        utility_score: float,
        fingerprint: str,
    ) -> None:
        vector = _embed_swarm_text(f"{scope_tag} {category} {rule_text}", self._vector_dim)
        payload = {
            "agent_id": agent_id,
            "scope_tag": scope_tag,
            "category": category,
            "rule_text": rule_text,
            "utility_score": float(utility_score),
            "fingerprint": fingerprint,
        }
        self._qdrant.upsert(
            collection_name=self._collection,
            points=[PointStruct(id=point_id, vector=vector, payload=payload)],
        )

    async def upsert_procedural_rule(
        self,
        *,
        agent_id: str,
        scope_tag: str,
        category: str,
        rule_text: str,
        fingerprint: str,
    ) -> SwarmRuleRecord:
        """Upsert a <=200-char normalized procedural rule into the shared Qdrant bus."""
        clean_rule = normalize_rule_length(rule_text)
        existing_id = await asyncio.to_thread(self._find_by_fingerprint_sync, fingerprint)
        point_id = existing_id or str(uuid.uuid4())
        await asyncio.to_thread(
            self._upsert_rule_sync,
            point_id=point_id,
            agent_id=agent_id,
            scope_tag=scope_tag,
            category=category,
            rule_text=clean_rule,
            utility_score=1.0,
            fingerprint=fingerprint,
        )
        return SwarmRuleRecord(
            memory_id=point_id,
            rule_text=clean_rule,
            category=category,
            scope_tag=scope_tag,
            utility_score=1.0,
            score=1.0,
        )

    def _recall_sync(
        self,
        query_intent: str,
        scope_tags: list[str] | None,
        top_k: int,
    ) -> list[SwarmRuleRecord]:
        vector = _embed_swarm_text(query_intent, self._vector_dim)
        res = self._qdrant.query_points(
            collection_name=self._collection,
            query=vector,
            limit=max(top_k * 3, 15),
        )
        scope_set = {s.lower() for s in (scope_tags or []) if s.strip()}
        records: list[SwarmRuleRecord] = []
        for pt in res.points:
            payload = pt.payload or {}
            pt_scope = str(payload.get("scope_tag", "global"))
            utility = float(payload.get("utility_score", 1.0))
            sim = float(pt.score)
            bonus = 0.25 if (scope_set and pt_scope.lower() in scope_set) else 0.0
            records.append(
                SwarmRuleRecord(
                    memory_id=str(pt.id),
                    rule_text=str(payload.get("rule_text", "")),
                    category=str(payload.get("category", "swarm_remediation")),
                    scope_tag=pt_scope,
                    utility_score=utility,
                    score=round(sim + bonus + (utility * 0.05), 4),
                )
            )
        records.sort(key=lambda r: r.score, reverse=True)
        return records[:top_k]

    async def recall_swarm_rules(
        self,
        *,
        query_intent: str,
        scope_tags: list[str] | None = None,
        top_k: int = 5,
    ) -> tuple[list[SwarmRuleRecord], float]:
        """Recall top-k shared swarm rules in <5ms from Qdrant."""
        import time

        t0 = time.perf_counter()
        records = await asyncio.to_thread(self._recall_sync, query_intent, scope_tags, top_k)
        recall_ms = round((time.perf_counter() - t0) * 1000.0, 3)

        self.memory_queries += 1
        if records:
            self.memory_hits += 1
        self.cumulative_recall_ms += recall_ms
        return records, recall_ms

    def _update_outcome_sync(
        self,
        agent_id: str,
        memory_id: str,
        succeeded: bool,
        prune_threshold: float,
    ) -> tuple[SwarmRuleOutcomeOutput, str | None]:
        pts = self._qdrant.retrieve(
            collection_name=self._collection,
            ids=[memory_id],
            with_payload=True,
        )
        if not pts:
            raise ValueError(f"Swarm rule '{memory_id}' not found in Qdrant.")
        payload: dict[str, Any] = dict(pts[0].payload or {})
        current_score = float(payload.get("utility_score", 1.0))
        new_score = min(2.0, round(current_score + (0.1 if succeeded else -0.4), 4))

        if new_score < prune_threshold:
            self._qdrant.delete(
                collection_name=self._collection,
                points_selector=[memory_id],
            )
            self.pruned_rules_count += 1
            fp = payload.get("fingerprint")
            return (
                SwarmRuleOutcomeOutput(
                    memory_id=memory_id,
                    agent_id=agent_id,
                    succeeded=succeeded,
                    utility_score=new_score,
                    pruned=True,
                ),
                str(fp) if isinstance(fp, str) else None,
            )

        self._qdrant.set_payload(
            collection_name=self._collection,
            payload={"utility_score": new_score},
            points=[memory_id],
        )
        return (
            SwarmRuleOutcomeOutput(
                memory_id=memory_id,
                agent_id=agent_id,
                succeeded=succeeded,
                utility_score=new_score,
                pruned=False,
            ),
            None,
        )

    async def record_rule_outcome(
        self,
        *,
        agent_id: str,
        memory_id: str,
        succeeded: bool,
        prune_threshold: float | None = None,
    ) -> tuple[SwarmRuleOutcomeOutput, str | None]:
        """Update utility (+0.1 success, -0.4 failure) and prune stale rules below threshold."""
        eff_threshold = (
            self._prune_threshold if prune_threshold is None else prune_threshold
        )
        return await asyncio.to_thread(
            self._update_outcome_sync,
            agent_id,
            memory_id,
            succeeded,
            eff_threshold,
        )

    def _count_active_rules_sync(self) -> int:
        res = self._qdrant.count(collection_name=self._collection, exact=True)
        return int(res.count)

    async def count_active_rules(self) -> int:
        return await asyncio.to_thread(self._count_active_rules_sync)
