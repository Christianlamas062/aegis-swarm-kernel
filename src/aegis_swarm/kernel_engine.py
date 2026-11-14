"""Unified L7 Multi-Agent DAG Orchestrator, Cost Control Plane, Lock Manager, and Memory Kernel."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any

from aegis_swarm.budget_guard import FinancialCircuitBreaker
from aegis_swarm.config import SwarmSettings, calculate_cost_usd, settings
from aegis_swarm.dag_router import DAGCompilerAndRouter
from aegis_swarm.schemas import (
    DAGPlanRequest,
    DAGPlanResponse,
    NodeSubmissionRequest,
    QuarantineVerificationResult,
    SwarmFailureInput,
    SwarmFailureOutput,
    SwarmRuleOutcomeInput,
    SwarmRuleOutcomeOutput,
    SwarmRuleRecord,
    SwarmTelemetrySnapshot,
    WorkspaceLockRequest,
    WorkspaceLockResponse,
)
from aegis_swarm.swarm_bus import (
    SharedEpisodicBus,
    sanitize_swarm_text,
    synthesize_swarm_rule,
)
from aegis_swarm.workspace_lock import (
    ShadowQuarantineGate,
    TransactionalWorkspaceCoordinator,
)


@dataclass
class _KernelTelemetryState:
    """Concurrency-safe L7 kernel telemetry state guarded by `asyncio.Lock`."""

    total_dags_compiled: int = 0
    total_nodes_routed: int = 0
    parallel_collisions_prevented: int = 0
    quarantine_submissions: int = 0
    quarantine_verified: int = 0
    quarantine_rejections: int = 0
    quarantine_syntax_rejections: int = 0
    quarantine_secret_detections: int = 0
    loop_interceptions: int = 0
    budget_trips: int = 0
    rate_limit_fallbacks: int = 0
    total_actual_cost_usd: float = 0.0
    total_baseline_cost_usd: float = 0.0
    raw_context_tokens: int = 0
    compressed_context_tokens: int = 0
    dag_estimates: dict[str, tuple[float, float]] = field(default_factory=dict)
    dag_real_costs: dict[str, tuple[float, float]] = field(default_factory=dict)
    node_baseline_tokens: dict[tuple[str, str], tuple[int, int]] = field(default_factory=dict)
    audit_log: list[dict[str, Any]] = field(default_factory=list)


class AegisSwarmKernel:
    """Deterministic L7 Control Plane unifying DAG routing, locks, quarantine, budget, and Qdrant memory."""

    def __init__(
        self,
        *,
        config: SwarmSettings = settings,
        bus: SharedEpisodicBus | None = None,
        cooldown_seconds: float | None = None,
        lock_ttl_seconds: float | None = None,
        default_dag_budget_usd: float | None = None,
        default_node_token_cap: int | None = None,
    ) -> None:
        self._config = config
        self._router = DAGCompilerAndRouter()
        self._bus = bus or SharedEpisodicBus(
            in_memory=config.qdrant_in_memory,
            qdrant_host=config.qdrant_host,
            qdrant_port=config.qdrant_port,
            collection_name=config.collection_name,
            prune_threshold=config.prune_threshold,
        )
        self._budget = FinancialCircuitBreaker(
            default_dag_budget_usd=default_dag_budget_usd or config.default_dag_budget_usd,
            default_node_token_cap=default_node_token_cap or config.default_node_token_cap,
            cooldown_seconds=config.cooldown_sec if cooldown_seconds is None else cooldown_seconds,
        )
        self._locks = TransactionalWorkspaceCoordinator(
            default_ttl_seconds=config.lock_ttl_sec if lock_ttl_seconds is None else lock_ttl_seconds
        )
        self._gate = ShadowQuarantineGate()
        self._lock = asyncio.Lock()
        self._telemetry = _KernelTelemetryState()

    @property
    def shared_bus(self) -> SharedEpisodicBus:
        return self._bus

    async def _append_audit(self, event: str, details: dict[str, Any]) -> None:
        async with self._lock:
            self._telemetry.audit_log.append({"event": event, **details})
            if len(self._telemetry.audit_log) > 250:
                self._telemetry.audit_log.pop(0)

    def _recompute_cost_totals_unlocked(self) -> None:
        actual = 0.0
        baseline = 0.0
        all_dags = set(self._telemetry.dag_estimates) | set(self._telemetry.dag_real_costs)
        for dag_id in all_dags:
            if dag_id in self._telemetry.dag_real_costs:
                real_a, real_b = self._telemetry.dag_real_costs[dag_id]
                actual += real_a
                baseline += real_b
            else:
                est_a, est_b = self._telemetry.dag_estimates[dag_id]
                actual += est_a
                baseline += est_b
        self._telemetry.total_actual_cost_usd = round(actual, 8)
        self._telemetry.total_baseline_cost_usd = round(baseline, 8)

    async def compile_and_route_dag(self, request: DAGPlanRequest) -> DAGPlanResponse:
        """Validate DAG topology, compress context packets, assign hybrid tiers, and recall swarm rules."""
        t0 = time.perf_counter()
        waves = self._router.compile_waves(request.nodes)

        recalled_by_node: dict[str, list[SwarmRuleRecord]] = {}
        recall_latencies: list[float] = []
        for node in request.nodes:
            intent = f"{node.task_type} {node.title} {' '.join(node.target_files)} {node.objective[:160]}"
            scopes = [*node.target_files, node.task_type, node.node_id]
            rules, rec_ms = await self._bus.recall_swarm_rules(
                query_intent=intent,
                scope_tags=scopes,
                top_k=4,
            )
            recalled_by_node[node.node_id] = rules
            recall_latencies.append(rec_ms)

        routed_nodes = self._router.build_routed_plan(
            dag_id=request.dag_id,
            nodes=request.nodes,
            waves=waves,
            recalled_by_node=recalled_by_node,
            default_node_cap=self._config.default_node_token_cap,
        )

        async with self._lock:
            eff_budget = self._budget.register_dag(
                dag_id=request.dag_id,
                max_budget_usd=request.max_budget_usd,
                routed_nodes=routed_nodes,
            )
            total_est = round(sum(n.estimated_cost_usd for n in routed_nodes), 8)
            total_base = round(sum(n.all_frontier_baseline_usd for n in routed_nodes), 8)
            raw_sum = sum(n.context_packet.raw_tokens_before_compression for n in routed_nodes)
            comp_sum = sum(n.context_packet.compressed_tokens for n in routed_nodes)

            self._telemetry.total_dags_compiled += 1
            self._telemetry.total_nodes_routed += len(routed_nodes)
            self._telemetry.raw_context_tokens += raw_sum
            self._telemetry.compressed_context_tokens += comp_sum
            self._telemetry.dag_estimates[request.dag_id] = (total_est, total_base)
            for rn in routed_nodes:
                self._telemetry.node_baseline_tokens[(request.dag_id, rn.node_id)] = (
                    rn.context_packet.raw_tokens_before_compression,
                    rn.estimated_output_tokens,
                )
            self._recompute_cost_totals_unlocked()

        savings_usd = round(max(0.0, total_base - total_est), 8)
        savings_pct = round((savings_usd / total_base) * 100.0, 2) if total_base > 0 else 0.0
        mean_comp = (
            round(
                sum(n.context_packet.compression_ratio for n in routed_nodes) / len(routed_nodes),
                4,
            )
            if routed_nodes
            else 0.0
        )
        mean_recall_ms = (
            round(sum(recall_latencies) / len(recall_latencies), 3) if recall_latencies else 0.0
        )

        await self._append_audit(
            "dag_compiled",
            {
                "dag_id": request.dag_id,
                "orchestrator_id": request.orchestrator_id,
                "nodes": len(routed_nodes),
                "waves": len(waves),
                "estimated_cost_usd": total_est,
                "baseline_cost_usd": total_base,
                "compile_ms": round((time.perf_counter() - t0) * 1000.0, 3),
            },
        )
        return DAGPlanResponse(
            dag_id=request.dag_id,
            orchestrator_id=request.orchestrator_id,
            execution_waves=waves,
            routed_nodes=routed_nodes,
            max_budget_usd=eff_budget,
            total_estimated_cost_usd=total_est,
            all_frontier_baseline_usd=total_base,
            estimated_savings_usd=savings_usd,
            estimated_savings_pct=savings_pct,
            memory_recall_ms=mean_recall_ms,
            mean_compression_ratio=mean_comp,
        )

    async def acquire_workspace_lock(
        self, request: WorkspaceLockRequest
    ) -> WorkspaceLockResponse:
        """Acquire atomic file mutex locks or return LOCK_CONFLICT to prevent agent race conditions."""
        async with self._lock:
            res = self._locks.acquire_lock(request)
            if res.status == "LOCK_CONFLICT":
                self._telemetry.parallel_collisions_prevented += 1
        await self._append_audit(
            f"lock_{res.status.lower()}",
            {
                "dag_id": request.dag_id,
                "node_id": request.node_id,
                "agent_id": request.agent_id,
                "paths": request.file_paths,
                "conflicting_paths": res.conflicting_paths,
            },
        )
        return res

    async def release_workspace_lock(
        self, *, dag_id: str, node_id: str, file_paths: list[str] | None = None
    ) -> list[str]:
        """Release workspace mutex locks held by `(dag_id, node_id)`."""
        async with self._lock:
            return self._locks.release_node_locks(
                dag_id=dag_id, node_id=node_id, paths=file_paths
            )

    def _record_real_cost_delta_unlocked(
        self,
        *,
        dag_id: str,
        node_id: str,
        node_cost: float,
        input_tokens: int,
        output_tokens: int,
        thinking_tokens: int,
    ) -> None:
        raw_in, est_out = self._telemetry.node_baseline_tokens.get((dag_id, node_id), (0, 0))
        eff_base_in = max(raw_in, input_tokens)
        eff_base_out = max(est_out, output_tokens + thinking_tokens)
        frontier_equiv = calculate_cost_usd("FRONTIER_CLOUD", eff_base_in, eff_base_out)
        prev_a, prev_b = self._telemetry.dag_real_costs.get(dag_id, (0.0, 0.0))
        self._telemetry.dag_real_costs[dag_id] = (
            round(prev_a + node_cost, 8),
            round(prev_b + frontier_equiv, 8),
        )
        self._recompute_cost_totals_unlocked()

    async def submit_node_artifact(
        self,
        request: NodeSubmissionRequest,
        *,
        cooldown_seconds: float | None = None,
    ) -> QuarantineVerificationResult:
        """Run Shadow Quarantine Gate (AST + 8-pattern secret/PII + budget + SHA-256 loop check)."""
        scope_tag = request.artifact_path.strip() or request.node_id

        # 1. Pre-flight SHA-256 payload loop breaker (<2ms)
        async with self._lock:
            self._telemetry.quarantine_submissions += 1
            tripped_rule = self._budget.check_payload_loop_breaker(
                agent_id=request.agent_id,
                scope_tag=scope_tag,
                payload=request.content,
                cooldown_seconds=cooldown_seconds,
            )
            if tripped_rule is not None:
                self._telemetry.loop_interceptions += 1
                self._telemetry.quarantine_rejections += 1
                dag_spend = self._budget.get_dag_spend(request.dag_id)
                dag_budget = self._budget.get_dag_budget(request.dag_id)
                return QuarantineVerificationResult(
                    dag_id=request.dag_id,
                    node_id=request.node_id,
                    agent_id=request.agent_id,
                    status="LOOP_DETECTED",
                    ast_valid=False,
                    secrets_found_count=0,
                    redacted_content="",
                    released_paths=[],
                    dag_spend_usd=dag_spend,
                    dag_budget_usd=dag_budget,
                    error_fingerprint=tripped_rule.memory_id,
                    distilled_rule=tripped_rule.rule_text,
                    circuit_breaker_tripped=True,
                    error="LOOP_DETECTED: Identical failed artifact resubmitted within SHA-256 cooldown TTL.",
                )

            # 2. Financial & token cap enforcement
            (
                exceeded,
                budget_err,
                node_tokens,
                node_cost,
                dag_spend,
                dag_budget,
            ) = self._budget.record_usage(
                dag_id=request.dag_id,
                node_id=request.node_id,
                input_tokens=request.input_tokens,
                output_tokens=request.output_tokens,
                thinking_tokens=request.thinking_tokens,
            )
            self._record_real_cost_delta_unlocked(
                dag_id=request.dag_id,
                node_id=request.node_id,
                node_cost=node_cost,
                input_tokens=request.input_tokens,
                output_tokens=request.output_tokens,
                thinking_tokens=request.thinking_tokens,
            )
            if exceeded:
                self._telemetry.budget_trips += 1
                self._telemetry.quarantine_rejections += 1

        if exceeded and budget_err:
            fp = self._budget.compute_error_fingerprint(
                agent_id=request.agent_id,
                error_type="BUDGET_EXCEEDED",
                error_message=budget_err,
            )
            rule_text = synthesize_swarm_rule(
                error_type="BUDGET_EXCEEDED",
                sanitized_error=budget_err,
                sanitized_payload="",
                scope_tag=scope_tag,
            )
            rule_rec = await self._bus.upsert_procedural_rule(
                agent_id=request.agent_id,
                scope_tag=scope_tag,
                category="budget_guardrail",
                rule_text=rule_text,
                fingerprint=fp,
            )
            return QuarantineVerificationResult(
                dag_id=request.dag_id,
                node_id=request.node_id,
                agent_id=request.agent_id,
                status="BUDGET_EXCEEDED",
                ast_valid=False,
                node_tokens_used=node_tokens,
                node_cost_usd=node_cost,
                dag_spend_usd=dag_spend,
                dag_budget_usd=dag_budget,
                error_fingerprint=fp,
                distilled_rule=rule_rec.rule_text,
                circuit_breaker_tripped=True,
                error=budget_err,
            )

        # 3. Recall shared rules & inspect artifact inside Shadow Quarantine Gate
        recalled_rules, _ = await self._bus.recall_swarm_rules(
            query_intent=f"{request.artifact_type} {scope_tag} {request.content[:120]}",
            scope_tags=[scope_tag, request.node_id, "global"],
            top_k=4,
        )
        syntax_valid, syntax_err, redacted_content, secrets_count = self._gate.inspect_artifact(
            artifact_type=request.artifact_type,
            artifact_path=request.artifact_path,
            content=request.content,
        )

        if secrets_count > 0:
            async with self._lock:
                self._telemetry.quarantine_secret_detections += 1

        if not syntax_valid and syntax_err:
            fp = self._budget.compute_error_fingerprint(
                agent_id=request.agent_id,
                error_type="REJECTED_SYNTAX",
                error_message=syntax_err,
                payload=request.content,
            )
            rule_text = synthesize_swarm_rule(
                error_type="REJECTED_SYNTAX",
                sanitized_error=syntax_err,
                sanitized_payload=redacted_content,
                scope_tag=scope_tag,
            )
            rule_rec = await self._bus.upsert_procedural_rule(
                agent_id=request.agent_id,
                scope_tag=scope_tag,
                category="quarantine_syntax",
                rule_text=rule_text,
                fingerprint=fp,
            )
            async with self._lock:
                self._telemetry.quarantine_rejections += 1
                self._telemetry.quarantine_syntax_rejections += 1
                self._budget.record_failure_fingerprint(
                    fingerprint=fp,
                    rule=rule_rec,
                    agent_id=request.agent_id,
                    scope_tag=scope_tag,
                    payload=request.content,
                )
            await self._append_audit(
                "quarantine_rejected_syntax",
                {"dag_id": request.dag_id, "node_id": request.node_id, "error": syntax_err},
            )
            return QuarantineVerificationResult(
                dag_id=request.dag_id,
                node_id=request.node_id,
                agent_id=request.agent_id,
                status="REJECTED_SYNTAX",
                ast_valid=False,
                secrets_found_count=secrets_count,
                redacted_content=redacted_content,
                released_paths=[],
                node_tokens_used=node_tokens,
                node_cost_usd=node_cost,
                dag_spend_usd=dag_spend,
                dag_budget_usd=dag_budget,
                error_fingerprint=fp,
                distilled_rule=rule_rec.rule_text,
                circuit_breaker_tripped=False,
                recalled_rules=recalled_rules,
                error=syntax_err,
            )

        if secrets_count > 0 and request.reject_on_secret:
            sec_err = (
                f"REJECTED_SECRET: Detected {secrets_count} secret/PII literal(s) in quarantined artifact."
            )
            fp = self._budget.compute_error_fingerprint(
                agent_id=request.agent_id,
                error_type="REJECTED_SECRET",
                error_message=sec_err,
                payload=redacted_content,
            )
            rule_text = synthesize_swarm_rule(
                error_type="REJECTED_SECRET",
                sanitized_error=sec_err,
                sanitized_payload=redacted_content,
                scope_tag=scope_tag,
            )
            rule_rec = await self._bus.upsert_procedural_rule(
                agent_id=request.agent_id,
                scope_tag=scope_tag,
                category="quarantine_security",
                rule_text=rule_text,
                fingerprint=fp,
            )
            async with self._lock:
                self._telemetry.quarantine_rejections += 1
                self._budget.record_failure_fingerprint(
                    fingerprint=fp,
                    rule=rule_rec,
                    agent_id=request.agent_id,
                    scope_tag=scope_tag,
                    payload=request.content,
                )
            return QuarantineVerificationResult(
                dag_id=request.dag_id,
                node_id=request.node_id,
                agent_id=request.agent_id,
                status="REJECTED_SECRET",
                ast_valid=True,
                secrets_found_count=secrets_count,
                redacted_content=redacted_content,
                released_paths=[],
                node_tokens_used=node_tokens,
                node_cost_usd=node_cost,
                dag_spend_usd=dag_spend,
                dag_budget_usd=dag_budget,
                error_fingerprint=fp,
                distilled_rule=rule_rec.rule_text,
                circuit_breaker_tripped=False,
                recalled_rules=recalled_rules,
                error=sec_err,
            )

        # 4. Promote to VERIFIED and release node locks
        async with self._lock:
            self._telemetry.quarantine_verified += 1
            released = (
                self._locks.release_node_locks(dag_id=request.dag_id, node_id=request.node_id)
                if request.release_locks_on_verify
                else []
            )
        await self._append_audit(
            "quarantine_verified",
            {
                "dag_id": request.dag_id,
                "node_id": request.node_id,
                "secrets_redacted": secrets_count,
                "released_paths": released,
            },
        )
        return QuarantineVerificationResult(
            dag_id=request.dag_id,
            node_id=request.node_id,
            agent_id=request.agent_id,
            status="VERIFIED",
            ast_valid=True,
            secrets_found_count=secrets_count,
            redacted_content=redacted_content,
            released_paths=released,
            node_tokens_used=node_tokens,
            node_cost_usd=node_cost,
            dag_spend_usd=dag_spend,
            dag_budget_usd=dag_budget,
            recalled_rules=recalled_rules,
        )

    async def record_swarm_failure(
        self,
        request: SwarmFailureInput,
        *,
        cooldown_seconds: float | None = None,
    ) -> SwarmFailureOutput:
        """Check budget, rate-limit fallback tier, and SHA-256 loop breaker; distill shared rule."""
        async with self._lock:
            exceeded, budget_err, _, _, dag_spend, _ = self._budget.record_usage(
                dag_id=request.dag_id,
                node_id=request.node_id,
                tier=request.current_tier,
                input_tokens=request.input_tokens,
                output_tokens=request.output_tokens,
                thinking_tokens=request.thinking_tokens,
            )
            if exceeded:
                self._telemetry.budget_trips += 1

        clean_err, _ = sanitize_swarm_text(request.error_message)
        clean_payload, _ = sanitize_swarm_text(request.failed_code_or_payload)
        next_tier, is_rate_limit = self._budget.recommend_fallback_tier(
            request.current_tier, clean_err
        )

        fp = self._budget.compute_error_fingerprint(
            agent_id=request.agent_id,
            error_type="BUDGET_EXCEEDED" if exceeded else request.error_type,
            error_message=budget_err or clean_err,
            payload=clean_payload,
        )

        if exceeded and budget_err:
            rule_text = synthesize_swarm_rule(
                error_type="BUDGET_EXCEEDED",
                sanitized_error=budget_err,
                sanitized_payload=clean_payload,
                scope_tag=request.scope_tag,
            )
            rule_rec = await self._bus.upsert_procedural_rule(
                agent_id=request.agent_id,
                scope_tag=request.scope_tag,
                category="budget_guardrail",
                rule_text=rule_text,
                fingerprint=fp,
            )
            return SwarmFailureOutput(
                dag_id=request.dag_id,
                node_id=request.node_id,
                agent_id=request.agent_id,
                status="BUDGET_EXCEEDED",
                error_fingerprint=fp,
                rule_id=rule_rec.memory_id,
                distilled_rule=rule_rec.rule_text,
                circuit_breaker_tripped=True,
                recommended_tier="LOCAL_WORKER",
                dag_spend_usd=dag_spend,
                tokens_saved_estimate=750,
            )

        async with self._lock:
            cached = self._budget.check_loop_breaker(
                fingerprint=fp, cooldown_seconds=cooldown_seconds
            )
            if cached is not None:
                self._telemetry.loop_interceptions += 1
                return SwarmFailureOutput(
                    dag_id=request.dag_id,
                    node_id=request.node_id,
                    agent_id=request.agent_id,
                    status="LOOP_DETECTED",
                    error_fingerprint=fp,
                    rule_id=cached.memory_id,
                    distilled_rule=cached.rule_text,
                    circuit_breaker_tripped=True,
                    recommended_tier=next_tier if is_rate_limit else request.current_tier,
                    dag_spend_usd=dag_spend,
                    tokens_saved_estimate=750,
                )

        rule_text = synthesize_swarm_rule(
            error_type=request.error_type,
            sanitized_error=clean_err,
            sanitized_payload=clean_payload,
            scope_tag=request.scope_tag,
        )
        rule_rec = await self._bus.upsert_procedural_rule(
            agent_id=request.agent_id,
            scope_tag=request.scope_tag,
            category="rate_limit_fallback" if is_rate_limit else "swarm_remediation",
            rule_text=rule_text,
            fingerprint=fp,
        )
        async with self._lock:
            self._budget.record_failure_fingerprint(
                fingerprint=fp,
                rule=rule_rec,
                agent_id=request.agent_id,
                scope_tag=request.scope_tag,
                payload=request.failed_code_or_payload,
            )
            if is_rate_limit:
                self._telemetry.rate_limit_fallbacks += 1

        return SwarmFailureOutput(
            dag_id=request.dag_id,
            node_id=request.node_id,
            agent_id=request.agent_id,
            status="RATE_LIMIT_FALLBACK" if is_rate_limit else "RULE_DISTILLED",
            error_fingerprint=fp,
            rule_id=rule_rec.memory_id,
            distilled_rule=rule_rec.rule_text,
            circuit_breaker_tripped=False,
            recommended_tier=next_tier if is_rate_limit else request.current_tier,
            dag_spend_usd=dag_spend,
            tokens_saved_estimate=0,
        )

    async def report_swarm_rule_outcome(
        self, request: SwarmRuleOutcomeInput
    ) -> SwarmRuleOutcomeOutput:
        """Apply +0.1/-0.4 adaptive utility scoring and auto-prune rules below threshold."""
        outcome, pruned_fp = await self._bus.record_rule_outcome(
            agent_id=request.agent_id,
            memory_id=request.memory_id,
            succeeded=request.succeeded,
            prune_threshold=request.prune_threshold,
        )
        if outcome.pruned and pruned_fp:
            async with self._lock:
                self._budget.invalidate_fingerprint(pruned_fp)
        return outcome

    async def get_swarm_telemetry(self) -> SwarmTelemetrySnapshot:
        """Return unified L7 financial, context compression, collision, quarantine, and memory metrics."""
        active_rules = await self._bus.count_active_rules()
        async with self._lock:
            actual = self._telemetry.total_actual_cost_usd
            baseline = self._telemetry.total_baseline_cost_usd
            savings = round(max(0.0, baseline - actual), 8)
            savings_pct = round((savings / baseline) * 100.0, 2) if baseline > 0 else 0.0

            raw_tok = self._telemetry.raw_context_tokens
            comp_tok = self._telemetry.compressed_context_tokens
            comp_ratio = (
                round(max(0.0, min(1.0, 1.0 - (comp_tok / raw_tok))), 4) if raw_tok > 0 else 0.0
            )

            mem_q = self._bus.memory_queries
            mem_h = self._bus.memory_hits
            hit_rate = round(mem_h / mem_q, 4) if mem_q > 0 else 0.0
            avg_recall_ms = (
                round(self._bus.cumulative_recall_ms / mem_q, 4) if mem_q > 0 else 0.0
            )

            return SwarmTelemetrySnapshot(
                total_dags_compiled=self._telemetry.total_dags_compiled,
                total_nodes_routed=self._telemetry.total_nodes_routed,
                parallel_collisions_prevented=self._telemetry.parallel_collisions_prevented,
                quarantine_submissions=self._telemetry.quarantine_submissions,
                quarantine_verified=self._telemetry.quarantine_verified,
                quarantine_rejections=self._telemetry.quarantine_rejections,
                quarantine_syntax_rejections=self._telemetry.quarantine_syntax_rejections,
                quarantine_secret_detections=self._telemetry.quarantine_secret_detections,
                loop_interceptions=self._telemetry.loop_interceptions,
                budget_trips=self._telemetry.budget_trips,
                rate_limit_fallbacks=self._telemetry.rate_limit_fallbacks,
                total_actual_cost_usd=actual,
                total_baseline_cost_usd=baseline,
                hybrid_cost_savings_usd=savings,
                hybrid_cost_savings_pct=savings_pct,
                raw_context_tokens=raw_tok,
                compressed_context_tokens=comp_tok,
                context_compression_ratio=comp_ratio,
                memory_queries=mem_q,
                memory_hits=mem_h,
                memory_hit_rate=hit_rate,
                average_recall_latency_ms=avg_recall_ms,
                active_rules_count=active_rules,
                pruned_rules_count=self._bus.pruned_rules_count,
                recent_audit_events=list(self._telemetry.audit_log[-25:]),
            )

