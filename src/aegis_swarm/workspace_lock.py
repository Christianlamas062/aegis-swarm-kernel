"""Transactional Mutex Workspace Locks & Shadow Quarantine Gate (AST + Secret/PII Gate)."""

from __future__ import annotations

import ast
import json
import time
from dataclasses import dataclass
from typing import Literal

from aegis_swarm.config import settings
from aegis_swarm.schemas import WorkspaceLockRequest, WorkspaceLockResponse
from aegis_swarm.swarm_bus import sanitize_swarm_text


def normalize_workspace_path(raw_path: str) -> str:
    """Normalize slash variants and leading './' so path conflict checks are deterministic."""
    cleaned = raw_path.strip().replace("\\", "/")
    while cleaned.startswith("./"):
        cleaned = cleaned[2:]
    return cleaned.lstrip("/").lower()


@dataclass
class _ActiveLock:
    dag_id: str
    node_id: str
    agent_id: str
    raw_path: str
    expires_at: float


class TransactionalWorkspaceCoordinator:
    """Atomic file/resource mutex manager preventing parallel sub-agent race conditions."""

    def __init__(self, default_ttl_seconds: float | None = None) -> None:
        self._default_ttl = (
            settings.lock_ttl_sec if default_ttl_seconds is None else default_ttl_seconds
        )
        self._locks: dict[str, _ActiveLock] = {}

    def _purge_expired(self, now_mono: float) -> None:
        expired = [k for k, lock in self._locks.items() if now_mono >= lock.expires_at]
        for key in expired:
            self._locks.pop(key, None)

    def acquire_lock(self, req: WorkspaceLockRequest) -> WorkspaceLockResponse:
        """Atomically acquire mutex locks for all requested paths or return LOCK_CONFLICT."""
        now_mono = time.monotonic()
        self._purge_expired(now_mono)
        ttl = self._default_ttl if req.ttl_seconds is None else req.ttl_seconds

        norm_pairs: list[tuple[str, str]] = []
        seen_norm: set[str] = set()
        for raw_p in req.file_paths:
            norm_p = normalize_workspace_path(raw_p)
            if not norm_p or norm_p in seen_norm:
                continue
            seen_norm.add(norm_p)
            norm_pairs.append((norm_p, raw_p))

        if not norm_pairs:
            raise ValueError("file_paths must contain at least one non-empty path.")

        conflicts: list[str] = []
        holder_node: str | None = None
        holder_agent: str | None = None
        for norm_p, raw_p in norm_pairs:
            existing = self._locks.get(norm_p)
            if existing is None:
                continue
            same_owner = (
                existing.dag_id == req.dag_id
                and existing.node_id == req.node_id
                and existing.agent_id == req.agent_id
            )
            if not same_owner:
                conflicts.append(raw_p)
                if holder_node is None:
                    holder_node = existing.node_id
                    holder_agent = existing.agent_id

        if conflicts:
            return WorkspaceLockResponse(
                status="LOCK_CONFLICT",
                dag_id=req.dag_id,
                node_id=req.node_id,
                agent_id=req.agent_id,
                locked_paths=[],
                conflicting_paths=conflicts,
                held_by_node=holder_node,
                held_by_agent=holder_agent,
                expires_in_sec=0.0,
                message=(
                    f"LOCK_CONFLICT: Path(s) {conflicts} already locked by node '{holder_node}' "
                    f"(agent '{holder_agent}'). Wait or split into a subsequent DAG wave."
                ),
            )

        expires_at = now_mono + ttl
        acquired_raw: list[str] = []
        for norm_p, raw_p in norm_pairs:
            self._locks[norm_p] = _ActiveLock(
                dag_id=req.dag_id,
                node_id=req.node_id,
                agent_id=req.agent_id,
                raw_path=raw_p,
                expires_at=expires_at,
            )
            acquired_raw.append(raw_p)

        return WorkspaceLockResponse(
            status="ACQUIRED",
            dag_id=req.dag_id,
            node_id=req.node_id,
            agent_id=req.agent_id,
            locked_paths=acquired_raw,
            conflicting_paths=[],
            held_by_node=req.node_id,
            held_by_agent=req.agent_id,
            expires_in_sec=round(ttl, 2),
            message=f"Acquired transactional mutex lock on {len(acquired_raw)} path(s).",
        )

    def release_node_locks(
        self,
        *,
        dag_id: str,
        node_id: str,
        paths: list[str] | None = None,
    ) -> list[str]:
        """Release locks held by `(dag_id, node_id)` and return the released paths."""
        now_mono = time.monotonic()
        self._purge_expired(now_mono)
        target_norms = (
            {normalize_workspace_path(p) for p in paths if normalize_workspace_path(p)}
            if paths
            else None
        )

        to_delete: list[str] = []
        released_raw: list[str] = []
        for norm_p, lock in self._locks.items():
            if lock.dag_id == dag_id and lock.node_id == node_id:
                if target_norms is None or norm_p in target_norms:
                    to_delete.append(norm_p)
                    released_raw.append(lock.raw_path)

        for key in to_delete:
            self._locks.pop(key, None)
        return released_raw


class ShadowQuarantineGate:
    """Validates sub-agent code/JSON syntax and scans/redacts secrets before VERIFIED promotion."""

    @staticmethod
    def inspect_artifact(
        *,
        artifact_type: Literal["python", "json", "text"],
        artifact_path: str,
        content: str,
    ) -> tuple[bool, str | None, str, int]:
        """Validate AST/JSON syntax and scrub 8 secret/PII patterns.

        Returns:
            (syntax_valid, syntax_error_message, redacted_content, secrets_found_count)
        """
        redacted_content, secrets_found = sanitize_swarm_text(content)

        eff_type = artifact_type
        lower_path = artifact_path.strip().lower()
        if lower_path.endswith(".py"):
            eff_type = "python"
        elif lower_path.endswith(".json"):
            eff_type = "json"

        if eff_type == "python":
            try:
                ast.parse(content)
            except SyntaxError as exc:
                err_msg = (
                    f"AST_SYNTAX_ERROR at line {exc.lineno}, col {exc.offset}: {exc.msg}"
                )
                return False, err_msg, redacted_content, secrets_found
        elif eff_type == "json":
            try:
                json.loads(content)
            except json.JSONDecodeError as exc:
                err_msg = (
                    f"JSON_SYNTAX_ERROR at line {exc.lineno}, col {exc.colno}: {exc.msg}"
                )
                return False, err_msg, redacted_content, secrets_found

        return True, None, redacted_content, secrets_found

