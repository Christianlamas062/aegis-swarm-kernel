"""Financial Circuit Breaker, Rate-Limit Fallback Router, and SHA-256 Anti-Loop Guard."""

from __future__ import annotations

import hashlib
import re
import time

from aegis_swarm.config import ModelTier, calculate_cost_usd, settings
from aegis_swarm.schemas import RoutedNodePlan, SwarmRuleRecord
from aegis_swarm.swarm_bus import sanitize_swarm_text

_UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)
_ISO_TS_RE = re.compile(
    r"\b\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?\b"
)
_HEX_RE = re.compile(r"\b0x[0-9a-fA-F]+\b")
_LINE_RE = re.compile(r"\bline\s+\d+\b", re.IGNORECASE)
_VOLATILE_NUM_RE = re.compile(r"\b\d{2,}\b")


class FinancialCircuitBreaker:
    """Tracks token/USD spend per DAG and node, enforces caps, and trips SHA-256 failure loops."""

    def __init__(
        self,
        *,
        default_dag_budget_usd: float | None = None,
        default_node_token_cap: int | None = None,
        cooldown_seconds: float | None = None,
    ) -> None:
        self._default_budget_usd = (
            settings.default_dag_budget_usd
            if default_dag_budget_usd is None
            else default_dag_budget_usd
        )
        self._default_node_cap = (
            settings.default_node_token_cap
            if default_node_token_cap is None
            else default_node_token_cap
        )
        self._cooldown_seconds = (
            settings.cooldown_sec if cooldown_seconds is None else cooldown_seconds
        )

        self._dag_budgets: dict[str, float] = {}
        self._dag_spend_usd: dict[str, float] = {}
        self._node_tokens: dict[tuple[str, str], int] = {}
        self._node_caps: dict[tuple[str, str], int] = {}
        self._node_tiers: dict[tuple[str, str], ModelTier] = {}

        # SHA-256 error deduplication cache: fingerprint -> monotonic timestamp
        self._dedup_cache: dict[str, float] = {}
        self._dedup_rules: dict[str, SwarmRuleRecord] = {}
        # Normalized (agent_id, artifact/payload) hash -> fingerprint for pre-execution loop block
        self._payload_breaker_map: dict[str, str] = {}

    def register_dag(
        self,
        *,
        dag_id: str,
        max_budget_usd: float | None,
        routed_nodes: list[RoutedNodePlan],
    ) -> float:
        """Initialize financial budget and node token ceilings for a compiled DAG."""
        effective_budget = (
            self._default_budget_usd if max_budget_usd is None else max_budget_usd
        )
        self._dag_budgets[dag_id] = effective_budget
        self._dag_spend_usd.setdefault(dag_id, 0.0)
        for node in routed_nodes:
            key = (dag_id, node.node_id)
            self._node_caps[key] = node.token_cap
            self._node_tiers[key] = node.assigned_tier
            self._node_tokens.setdefault(key, 0)
        return effective_budget

    def get_dag_budget(self, dag_id: str) -> float:
        return self._dag_budgets.get(dag_id, self._default_budget_usd)

    def get_dag_spend(self, dag_id: str) -> float:
        return round(self._dag_spend_usd.get(dag_id, 0.0), 8)

    def get_node_tier(self, dag_id: str, node_id: str) -> ModelTier:
        return self._node_tiers.get((dag_id, node_id), "FAST_CLOUD")

    def record_usage(
        self,
        *,
        dag_id: str,
        node_id: str,
        tier: ModelTier | None = None,
        input_tokens: int = 0,
        output_tokens: int = 0,
        thinking_tokens: int = 0,
    ) -> tuple[bool, str | None, int, float, float, float]:
        """Accumulate input/output/thinking tokens and USD spend; flag budget/token breaches."""
        key = (dag_id, node_id)
        eff_tier = tier or self.get_node_tier(dag_id, node_id)
        dag_budget = self.get_dag_budget(dag_id)
        node_cap = self._node_caps.get(key, self._default_node_cap)

        added_tokens = max(0, input_tokens) + max(0, output_tokens) + max(0, thinking_tokens)
        new_node_tokens = self._node_tokens.get(key, 0) + added_tokens
        self._node_tokens[key] = new_node_tokens

        increment_cost = calculate_cost_usd(
            eff_tier,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            thinking_tokens=thinking_tokens,
        )
        new_dag_spend = round(self._dag_spend_usd.get(dag_id, 0.0) + increment_cost, 8)
        self._dag_spend_usd[dag_id] = new_dag_spend

        if new_node_tokens > node_cap:
            return (
                True,
                f"BUDGET_EXCEEDED: Node '{node_id}' consumed {new_node_tokens} tokens, exceeding cap of {node_cap}.",
                new_node_tokens,
                increment_cost,
                new_dag_spend,
                dag_budget,
            )
        if new_dag_spend > dag_budget:
            return (
                True,
                f"BUDGET_EXCEEDED: DAG '{dag_id}' spend ${new_dag_spend:.6f} exceeded ceiling of ${dag_budget:.6f}.",
                new_node_tokens,
                increment_cost,
                new_dag_spend,
                dag_budget,
            )
        return (
            False,
            None,
            new_node_tokens,
            increment_cost,
            new_dag_spend,
            dag_budget,
        )

    @staticmethod
    def recommend_fallback_tier(current_tier: ModelTier, error_message: str) -> tuple[ModelTier, bool]:
        """Downgrade FRONTIER_CLOUD -> FAST_CLOUD -> LOCAL_WORKER on 429 / rate-limit errors."""
        low = error_message.lower()
        is_rate_limit = any(
            sig in low for sig in ("429", "rate limit", "rate_limit", "quota", "overloaded")
        )
        if not is_rate_limit:
            return current_tier, False
        if current_tier == "FRONTIER_CLOUD":
            return "FAST_CLOUD", True
        if current_tier == "FAST_CLOUD":
            return "LOCAL_WORKER", True
        return "LOCAL_WORKER", True

    @staticmethod
    def normalize_text(raw: str) -> str:
        """Normalize volatile UUIDs, timestamps, hex addresses, line numbers, and large numbers."""
        out = _UUID_RE.sub("<UUID>", raw)
        out = _ISO_TS_RE.sub("<TS>", out)
        out = _HEX_RE.sub("<HEX>", out)
        out = _LINE_RE.sub("line <NUM>", out)
        out = _VOLATILE_NUM_RE.sub("<NUM>", out)
        return re.sub(r"\s+", " ", out).strip().lower()

    @classmethod
    def compute_error_fingerprint(
        cls,
        *,
        agent_id: str,
        error_type: str,
        error_message: str,
        payload: str = "",
    ) -> str:
        """Compute normalized SHA-256 error fingerprint scoped by agent_id and error type."""
        clean_err, _ = sanitize_swarm_text(error_message)
        clean_payload, _ = sanitize_swarm_text(payload)
        raw_key = (
            f"{agent_id.strip()}:{error_type.strip().upper()}:"
            f"{cls.normalize_text(clean_err)}:{cls.normalize_text(clean_payload)}"
        )
        return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()

    @classmethod
    def compute_payload_key(cls, *, agent_id: str, scope_tag: str, payload: str) -> str:
        clean_payload, _ = sanitize_swarm_text(payload)
        raw_key = f"{agent_id.strip()}:{scope_tag.strip().lower()}:{cls.normalize_text(clean_payload)}"
        return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()

    def check_loop_breaker(
        self,
        *,
        fingerprint: str,
        cooldown_seconds: float | None = None,
    ) -> SwarmRuleRecord | None:
        """Return cached procedural rule if identical normalized error fingerprint repeats within TTL."""
        eff_cooldown = (
            self._cooldown_seconds if cooldown_seconds is None else cooldown_seconds
        )
        if eff_cooldown <= 0:
            return None
        last_seen = self._dedup_cache.get(fingerprint)
        cached_rule = self._dedup_rules.get(fingerprint)
        if last_seen is None or cached_rule is None:
            return None
        if (time.monotonic() - last_seen) < eff_cooldown:
            return cached_rule
        self._dedup_cache.pop(fingerprint, None)
        self._dedup_rules.pop(fingerprint, None)
        return None

    def check_payload_loop_breaker(
        self,
        *,
        agent_id: str,
        scope_tag: str,
        payload: str,
        cooldown_seconds: float | None = None,
    ) -> SwarmRuleRecord | None:
        """Short-circuit repeated submission of previously failed artifact payload within TTL."""
        if not payload.strip():
            return None
        key = self.compute_payload_key(agent_id=agent_id, scope_tag=scope_tag, payload=payload)
        fingerprint = self._payload_breaker_map.get(key)
        if not fingerprint:
            return None
        rule = self.check_loop_breaker(
            fingerprint=fingerprint, cooldown_seconds=cooldown_seconds
        )
        if rule is None:
            self._payload_breaker_map.pop(key, None)
        return rule

    def record_failure_fingerprint(
        self,
        *,
        fingerprint: str,
        rule: SwarmRuleRecord,
        agent_id: str,
        scope_tag: str,
        payload: str = "",
    ) -> None:
        """Cache failure fingerprint and payload pattern key in TTL deduplication state."""
        self._dedup_cache[fingerprint] = time.monotonic()
        self._dedup_rules[fingerprint] = rule
        if payload.strip():
            pkey = self.compute_payload_key(agent_id=agent_id, scope_tag=scope_tag, payload=payload)
            self._payload_breaker_map[pkey] = fingerprint

    def invalidate_fingerprint(self, fingerprint: str | None) -> None:
        """Remove auto-pruned rule fingerprint from TTL deduplication caches."""
        if not fingerprint:
            return
        self._dedup_cache.pop(fingerprint, None)
        self._dedup_rules.pop(fingerprint, None)
        stale_keys = [k for k, v in self._payload_breaker_map.items() if v == fingerprint]
        for key in stale_keys:
            self._payload_breaker_map.pop(key, None)

