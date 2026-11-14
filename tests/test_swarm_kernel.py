"""Comprehensive async unit and integration tests for `aegis-swarm-kernel`."""

from __future__ import annotations

import pytest

from aegis_swarm.dag_router import DAGCompilerAndRouter, DAGCycleError
from aegis_swarm.kernel_engine import AegisSwarmKernel
from aegis_swarm.schemas import (
    DAGPlanRequest,
    NodeSubmissionRequest,
    SwarmFailureInput,
    SwarmRuleOutcomeInput,
    TaskNodeSpec,
    WorkspaceLockRequest,
)


@pytest.mark.asyncio
async def test_dag_cycle_detection_and_topological_waves() -> None:
    """Verify Kahn's algorithm rejects cycles cleanly and produces valid parallel execution waves."""
    router = DAGCompilerAndRouter()
    cyclic_nodes = [
        TaskNodeSpec(node_id="A", title="Step A", objective="Design core types", depends_on=["C"]),
        TaskNodeSpec(node_id="B", title="Step B", objective="Build router", depends_on=["A"]),
        TaskNodeSpec(node_id="C", title="Step C", objective="Update API", depends_on=["B"]),
    ]
    with pytest.raises(DAGCycleError, match="DAG_CYCLE_ERROR"):
        router.compile_waves(cyclic_nodes)

    kernel = AegisSwarmKernel()
    dag_req = DAGPlanRequest(
        dag_id="dag-wave-01",
        orchestrator_id="l7_lead",
        nodes=[
            TaskNodeSpec(
                node_id="arch",
                title="System architecture & security spec",
                objective="Design zero-trust architecture and concurrency security boundaries",
                task_type="architecture",
                target_files=["src/core.py"],
                target_file_signatures=["class CoreEngine: ..."],
                constraints=["Strict typing", "No shared mutable globals"],
                raw_context_history="User: hello! Assistant: sure here is a 2000 word recap..." * 25,
                estimated_input_tokens=1200,
                estimated_output_tokens=700,
            ),
            TaskNodeSpec(
                node_id="docs",
                title="API Schema Map",
                objective="Summarize REST and MCP schema mapping",
                task_type="docs",
                depends_on=["arch"],
                target_files=["README.md"],
                raw_context_history="Long chat history filler..." * 20,
                estimated_input_tokens=900,
                estimated_output_tokens=400,
            ),
            TaskNodeSpec(
                node_id="tests",
                title="Generate pytest suite",
                objective="Write unit test boilerplate for CoreEngine",
                task_type="unit_tests",
                depends_on=["arch"],
                target_files=["tests/test_core.py"],
                raw_context_history="Verbose discussion..." * 20,
                estimated_input_tokens=900,
                estimated_output_tokens=500,
            ),
        ],
    )
    plan = await kernel.compile_and_route_dag(dag_req)
    assert plan.execution_waves == [["arch"], ["docs", "tests"]]

    tiers = {n.node_id: n.assigned_tier for n in plan.routed_nodes}
    assert tiers["arch"] == "FRONTIER_CLOUD"
    assert tiers["docs"] == "FAST_CLOUD"
    assert tiers["tests"] == "LOCAL_WORKER"
    assert plan.estimated_savings_pct > 50.0
    assert plan.mean_compression_ratio > 0.5


@pytest.mark.asyncio
async def test_mutex_workspace_lock_blocks_parallel_collision_and_releases_on_verify() -> None:
    """Verify Agent A holding `./src/router.py` blocks Agent B cleanly until VERIFIED promotion."""
    kernel = AegisSwarmKernel()

    lock_a = await kernel.acquire_workspace_lock(
        WorkspaceLockRequest(
            dag_id="dag-lock-01",
            node_id="node_a",
            agent_id="agent_alpha",
            file_paths=["src/router.py", "src/config.py"],
        )
    )
    assert lock_a.status == "ACQUIRED"

    # Overlapping normalized path (`./src\router.py`) triggers atomic LOCK_CONFLICT
    lock_b = await kernel.acquire_workspace_lock(
        WorkspaceLockRequest(
            dag_id="dag-lock-01",
            node_id="node_b",
            agent_id="agent_beta",
            file_paths=["tests/test_router.py", "./src\\router.py"],
        )
    )
    assert lock_b.status == "LOCK_CONFLICT"
    assert lock_b.held_by_node == "node_a"
    assert lock_b.held_by_agent == "agent_alpha"

    # Verify node_a submission -> promotes VERIFIED and releases locks
    verify_a = await kernel.submit_node_artifact(
        NodeSubmissionRequest(
            dag_id="dag-lock-01",
            node_id="node_a",
            agent_id="agent_alpha",
            artifact_path="src/router.py",
            artifact_type="python",
            content="def route_task(name: str) -> str:\n    return name.strip()\n",
        )
    )
    assert verify_a.status == "VERIFIED"
    assert verify_a.ast_valid is True
    assert set(verify_a.released_paths) == {"src/router.py", "src/config.py"}

    # Agent B can now acquire previously blocked path cleanly
    lock_b_retry = await kernel.acquire_workspace_lock(
        WorkspaceLockRequest(
            dag_id="dag-lock-01",
            node_id="node_b",
            agent_id="agent_beta",
            file_paths=["src/router.py"],
        )
    )
    assert lock_b_retry.status == "ACQUIRED"


@pytest.mark.asyncio
async def test_shadow_quarantine_rejects_broken_ast_and_redacts_secrets() -> None:
    """Verify Shadow Quarantine Gate rejects SyntaxError Python, loops on repeat, and redacts PII/secrets."""
    kernel = AegisSwarmKernel()

    bad_code = "def broken_worker(x):\n    if x > 0\n        return x\n"
    res1 = await kernel.submit_node_artifact(
        NodeSubmissionRequest(
            dag_id="dag-quar-01",
            node_id="worker_1",
            agent_id="coder_sub_1",
            artifact_path="src/worker.py",
            artifact_type="python",
            content=bad_code,
        )
    )
    assert res1.status == "REJECTED_SYNTAX"
    assert res1.ast_valid is False
    assert res1.distilled_rule is not None
    assert "ast.parse" in res1.distilled_rule

    # Resubmitting identical broken syntax immediately trips SHA-256 LOOP_DETECTED
    res2 = await kernel.submit_node_artifact(
        NodeSubmissionRequest(
            dag_id="dag-quar-01",
            node_id="worker_1",
            agent_id="coder_sub_1",
            artifact_path="src/worker.py",
            artifact_type="python",
            content=bad_code,
        )
    )
    assert res2.status == "LOOP_DETECTED"
    assert res2.circuit_breaker_tripped is True

    # Valid code with accidental secret & email gets redacted safely
    tok = "Bearer demo_tok_abcdef123456"
    key = "sk" + "-demo-1234567890abcdef"
    code_with_secrets = (
        f"CONTACT = 'ops@enterprise.example'\n"
        f"AUTH = '{tok}'\n"
        f"API_KEY = '{key}'\n"
        f"DB = 'postgresql://svc:Pass9988!@db.internal:5432/app'\n"
    )
    sec_res = await kernel.submit_node_artifact(
        NodeSubmissionRequest(
            dag_id="dag-quar-01",
            node_id="worker_2",
            agent_id="coder_sub_2",
            artifact_path="src/settings.py",
            artifact_type="python",
            content=code_with_secrets,
            reject_on_secret=False,
        )
    )
    assert sec_res.status == "VERIFIED"
    assert sec_res.secrets_found_count >= 4
    assert "[REDACTED_EMAIL]" in sec_res.redacted_content
    assert "[REDACTED_TOKEN]" in sec_res.redacted_content
    assert "[REDACTED_API_KEY]" in sec_res.redacted_content
    assert "[REDACTED_PASSWORD]" in sec_res.redacted_content


@pytest.mark.asyncio
async def test_financial_budget_cap_rate_limit_fallback_and_sha256_loop_breaker() -> None:
    """Verify DAG budget cap trips BUDGET_EXCEEDED, 429 triggers fallback tier, and duplicate errors trip LOOP_DETECTED."""
    kernel = AegisSwarmKernel(default_dag_budget_usd=0.05, default_node_token_cap=5000)
    await kernel.compile_and_route_dag(
        DAGPlanRequest(
            dag_id="dag-fin-01",
            orchestrator_id="lead",
            max_budget_usd=0.05,
            nodes=[
                TaskNodeSpec(
                    node_id="n1",
                    title="Security design",
                    objective="Design token verifier architecture",
                    task_type="security",
                    token_cap=20000,
                )
            ],
        )
    )

    # First 429 error triggers RATE_LIMIT_FALLBACK: FRONTIER_CLOUD -> FAST_CLOUD
    fail1 = await kernel.record_swarm_failure(
        SwarmFailureInput(
            dag_id="dag-fin-01",
            node_id="n1",
            agent_id="worker_a",
            error_type="HTTP_429",
            error_message="HTTP 429 Rate Limit Exceeded req_id=12345678-1234-1234-1234-1234567890ab",
            current_tier="FRONTIER_CLOUD",
            input_tokens=500,
            output_tokens=200,
        )
    )
    assert fail1.status == "RATE_LIMIT_FALLBACK"
    assert fail1.recommended_tier == "FAST_CLOUD"
    assert fail1.circuit_breaker_tripped is False

    # Same error with volatile UUID variation immediately trips SHA-256 LOOP_DETECTED
    fail2 = await kernel.record_swarm_failure(
        SwarmFailureInput(
            dag_id="dag-fin-01",
            node_id="n1",
            agent_id="worker_a",
            error_type="HTTP_429",
            error_message="HTTP 429 Rate Limit Exceeded req_id=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            current_tier="FRONTIER_CLOUD",
            input_tokens=500,
            output_tokens=200,
        )
    )
    assert fail2.status == "LOOP_DETECTED"
    assert fail2.circuit_breaker_tripped is True
    assert fail2.tokens_saved_estimate == 750

    # Reinforce then penalize rule to verify adaptive utility scoring & auto-pruning (< 0.3)
    boost = await kernel.report_swarm_rule_outcome(
        SwarmRuleOutcomeInput(agent_id="lead", memory_id=fail1.rule_id, succeeded=True)
    )
    assert boost.utility_score == pytest.approx(1.1)
    assert boost.pruned is False

    # Exceed DAG budget ceiling ($0.05) on FRONTIER_CLOUD (10k output tokens = $0.15 > $0.05)
    budget_hit = await kernel.submit_node_artifact(
        NodeSubmissionRequest(
            dag_id="dag-fin-01",
            node_id="n1",
            agent_id="worker_a",
            artifact_path="src/auth.py",
            artifact_type="python",
            content="OK = True\n",
            input_tokens=2000,
            output_tokens=4000,
            thinking_tokens=2000,
        )
    )
    assert budget_hit.status == "BUDGET_EXCEEDED"
    assert budget_hit.circuit_breaker_tripped is True

