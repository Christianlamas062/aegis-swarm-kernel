"""Topological DAG Compiler, Hybrid Model Router, and ContextPacket Compressor."""

from __future__ import annotations

from collections import deque
from typing import Final

from aegis_swarm.config import ModelTier, calculate_cost_usd, settings
from aegis_swarm.schemas import (
    ContextPacket,
    RoutedNodePlan,
    SwarmRuleRecord,
    TaskCategory,
    TaskNodeSpec,
)


class DAGCycleError(ValueError):
    """Raised when a submitted task graph contains cyclic or unresolved dependencies."""


_FRONTIER_CATEGORIES: Final[set[TaskCategory]] = {"architecture", "debugging", "security"}
_FAST_CATEGORIES: Final[set[TaskCategory]] = {"summarization", "docs", "schema_mapping"}
_LOCAL_CATEGORIES: Final[set[TaskCategory]] = {"boilerplate", "unit_tests", "refactoring"}

_FRONTIER_KEYWORDS: Final[tuple[str, ...]] = (
    "architect",
    "security",
    "debug",
    "concurrency",
    "deadlock",
    "race condition",
    "auth",
    "cryptograph",
    "orchestrat",
    "circuit breaker",
    "schema design",
)
_LOCAL_KEYWORDS: Final[tuple[str, ...]] = (
    "boilerplate",
    "unit test",
    "pytest",
    "test suite",
    "scaffold",
    "stub",
    "refactor",
    "lint",
    "type hint",
    "crud",
)
_FAST_KEYWORDS: Final[tuple[str, ...]] = (
    "summariz",
    "doc",
    "readme",
    "changelog",
    "schema map",
    "inspect",
    "report",
    "manifest",
)


def _approx_tokens(text: str) -> int:
    if not text:
        return 0
    return max(1, (len(text) + 3) // 4)


class DAGCompilerAndRouter:
    """Validates task DAGs via Kahn's algorithm, routes hybrid model tiers, and compresses context."""

    @staticmethod
    def compile_waves(nodes: list[TaskNodeSpec]) -> list[list[str]]:
        """Topologically sort nodes into parallelizable execution waves via Kahn's algorithm."""
        node_map: dict[str, TaskNodeSpec] = {}
        for node in nodes:
            if node.node_id in node_map:
                raise DAGCycleError(f"Duplicate node_id '{node.node_id}' in DAG specification.")
            node_map[node.node_id] = node

        in_degree: dict[str, int] = {node.node_id: 0 for node in nodes}
        dependents: dict[str, list[str]] = {node.node_id: [] for node in nodes}

        for node in nodes:
            seen_deps: set[str] = set()
            for dep in node.depends_on:
                dep_id = dep.strip()
                if dep_id not in node_map:
                    raise DAGCycleError(
                        f"Node '{node.node_id}' depends on unknown node '{dep_id}'."
                    )
                if dep_id == node.node_id:
                    raise DAGCycleError(f"Self-dependency cycle detected on node '{node.node_id}'.")
                if dep_id not in seen_deps:
                    seen_deps.add(dep_id)
                    in_degree[node.node_id] += 1
                    dependents[dep_id].append(node.node_id)

        queue: deque[str] = deque(
            sorted(nid for nid, deg in in_degree.items() if deg == 0)
        )
        waves: list[list[str]] = []
        visited = 0

        while queue:
            current_wave = list(queue)
            queue.clear()
            waves.append(current_wave)
            next_ready: list[str] = []
            for nid in current_wave:
                visited += 1
                for child_id in dependents[nid]:
                    in_degree[child_id] -= 1
                    if in_degree[child_id] == 0:
                        next_ready.append(child_id)
            for child_id in sorted(next_ready):
                queue.append(child_id)

        if visited != len(nodes):
            cyclic_nodes = sorted(nid for nid, deg in in_degree.items() if deg > 0)
            raise DAGCycleError(
                f"DAG_CYCLE_ERROR: Cyclic dependency detected involving nodes: {', '.join(cyclic_nodes)}."
            )

        return waves

    @staticmethod
    def classify_tier(node: TaskNodeSpec) -> tuple[ModelTier, str]:
        """Classify node complexity and route to FRONTIER_CLOUD, FAST_CLOUD, or LOCAL_WORKER."""
        if node.preferred_tier is not None:
            return node.preferred_tier, f"Explicit preferred_tier='{node.preferred_tier}' honored."

        if node.task_type in _FRONTIER_CATEGORIES:
            return (
                "FRONTIER_CLOUD",
                f"Task category '{node.task_type}' requires deep reasoning (FRONTIER_CLOUD).",
            )
        if node.task_type in _FAST_CATEGORIES:
            return (
                "FAST_CLOUD",
                f"Task category '{node.task_type}' optimized for low-cost speed (FAST_CLOUD).",
            )
        if node.task_type in _LOCAL_CATEGORIES:
            return (
                "LOCAL_WORKER",
                f"Task category '{node.task_type}' offloaded at zero API cost (LOCAL_WORKER).",
            )

        haystack = f"{node.title} {node.objective}".lower()
        if any(kw in haystack for kw in _FRONTIER_KEYWORDS):
            return (
                "FRONTIER_CLOUD",
                "High-complexity architecture/security/debugging keywords detected.",
            )
        if any(kw in haystack for kw in _LOCAL_KEYWORDS):
            return (
                "LOCAL_WORKER",
                "Boilerplate/unit-test/refactor workload routed to zero-cost local worker.",
            )
        if any(kw in haystack for kw in _FAST_KEYWORDS):
            return (
                "FAST_CLOUD",
                "Documentation/schema/summarization workload routed to fast cloud tier.",
            )
        return "FAST_CLOUD", "Balanced general-purpose subtask routed to FAST_CLOUD."

    @staticmethod
    def build_compressed_packet(
        *,
        dag_id: str,
        node: TaskNodeSpec,
        assigned_tier: ModelTier,
        recalled_rules: list[SwarmRuleRecord],
    ) -> ContextPacket:
        """Strip verbose chat filler into a minimal ContextPacket with signatures and rules."""
        rules_text = [r.rule_text for r in recalled_rules[:4]]
        raw_chat_tokens = _approx_tokens(node.raw_context_history)
        base_tokens = node.estimated_input_tokens
        raw_total = base_tokens + raw_chat_tokens

        # Compressed packet keeps only objective, file paths, signatures, constraints, and rules
        compact_parts = [
            node.objective,
            " ".join(node.target_files),
            " ".join(node.target_file_signatures),
            " ".join(node.constraints),
            " ".join(rules_text),
        ]
        compressed_payload_tokens = max(
            32,
            _approx_tokens(" | ".join(p for p in compact_parts if p)) + 24,
        )
        effective_compressed = min(raw_total, compressed_payload_tokens)
        ratio = (
            round(max(0.0, min(1.0, 1.0 - (effective_compressed / raw_total))), 4)
            if raw_total > 0
            else 0.0
        )

        return ContextPacket(
            dag_id=dag_id,
            node_id=node.node_id,
            agent_role=f"{assigned_tier.lower()}_executor",
            target_files=list(node.target_files),
            target_file_signatures=list(node.target_file_signatures),
            constraints=list(node.constraints),
            recalled_procedural_rules=rules_text,
            raw_tokens_before_compression=raw_total,
            compressed_tokens=effective_compressed,
            compression_ratio=ratio,
        )

    def build_routed_plan(
        self,
        *,
        dag_id: str,
        nodes: list[TaskNodeSpec],
        waves: list[list[str]],
        recalled_by_node: dict[str, list[SwarmRuleRecord]],
        default_node_cap: int | None = None,
    ) -> list[RoutedNodePlan]:
        """Produce ordered RoutedNodePlan entries across compiled topological waves."""
        node_lookup = {n.node_id: n for n in nodes}
        fallback_cap = settings.default_node_token_cap if default_node_cap is None else default_node_cap
        routed: list[RoutedNodePlan] = []

        for wave_idx, wave_nodes in enumerate(waves):
            for nid in wave_nodes:
                spec = node_lookup[nid]
                tier, reason = self.classify_tier(spec)
                packet = self.build_compressed_packet(
                    dag_id=dag_id,
                    node=spec,
                    assigned_tier=tier,
                    recalled_rules=recalled_by_node.get(nid, []),
                )
                est_in = packet.compressed_tokens
                est_out = spec.estimated_output_tokens
                est_cost = calculate_cost_usd(tier, est_in, est_out)
                frontier_baseline = calculate_cost_usd(
                    "FRONTIER_CLOUD",
                    packet.raw_tokens_before_compression,
                    est_out,
                )
                routed.append(
                    RoutedNodePlan(
                        node_id=spec.node_id,
                        title=spec.title,
                        wave_index=wave_idx,
                        assigned_tier=tier,
                        routing_reason=reason,
                        token_cap=spec.token_cap if spec.token_cap is not None else fallback_cap,
                        estimated_input_tokens=est_in,
                        estimated_output_tokens=est_out,
                        estimated_cost_usd=est_cost,
                        all_frontier_baseline_usd=frontier_baseline,
                        context_packet=packet,
                    )
                )
        return routed

