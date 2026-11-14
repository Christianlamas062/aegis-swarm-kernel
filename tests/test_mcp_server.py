"""Integration tests for all 6 FastMCP tools, resource, prompt, and dual-transport CLI."""

from __future__ import annotations

import json

import pytest

from aegis_swarm.server import (
    acquire_workspace_lock,
    compile_and_route_dag,
    get_swarm_telemetry,
    l7_swarm_orchestrator_prompt,
    parse_cli_args,
    record_swarm_failure,
    report_swarm_rule_outcome,
    reset_active_kernel,
    resource_swarm_telemetry,
    submit_node_artifact,
)


@pytest.mark.asyncio
async def test_mcp_server_six_tools_resource_prompt_and_cli() -> None:
    """Exercise all 6 MCP tools, `aegisswarm://telemetry`, prompt, and dual CLI parser."""
    await reset_active_kernel(default_dag_budget_usd=1.00, default_node_token_cap=6000)

    fn_compile = getattr(compile_and_route_dag, "fn", compile_and_route_dag)
    fn_lock = getattr(acquire_workspace_lock, "fn", acquire_workspace_lock)
    fn_submit = getattr(submit_node_artifact, "fn", submit_node_artifact)
    fn_fail = getattr(record_swarm_failure, "fn", record_swarm_failure)
    fn_outcome = getattr(report_swarm_rule_outcome, "fn", report_swarm_rule_outcome)
    fn_telemetry = getattr(get_swarm_telemetry, "fn", get_swarm_telemetry)

    # 1. Compile DAG
    plan = await fn_compile(
        dag_id="mcp-dag-1",
        orchestrator_id="l7_architect",
        nodes=[
            {
                "node_id": "design",
                "title": "Design schema",
                "objective": "Design modular architecture and security contracts",
                "task_type": "architecture",
                "target_files": ["src/schema.py"],
                "raw_context_history": "Verbose chat logs " * 40,
            },
            {
                "node_id": "tests",
                "title": "Write pytest suite",
                "objective": "Generate unit tests for schema",
                "task_type": "unit_tests",
                "depends_on": ["design"],
                "target_files": ["tests/test_schema.py"],
            },
        ],
    )
    assert plan["execution_waves"] == [["design"], ["tests"]]

    # 2. Acquire Mutex Lock
    lock_res = await fn_lock(
        dag_id="mcp-dag-1",
        node_id="design",
        agent_id="architect_agent",
        file_paths=["src/schema.py"],
    )
    assert lock_res["status"] == "ACQUIRED"

    # 3. Record Swarm Failure -> Distill Rule
    fail_res = await fn_fail(
        dag_id="mcp-dag-1",
        node_id="tests",
        agent_id="qa_agent",
        error_type="EXECUTION_ERROR",
        error_message="Missing strict extra='forbid' check on Pydantic model",
        current_tier="LOCAL_WORKER",
        scope_tag="src/schema.py",
    )
    assert fail_res["status"] == "RULE_DISTILLED"

    # 4. Report Rule Outcome (Penalize x2 -> Auto-prune below 0.3)
    out1 = await fn_outcome(agent_id="l7_architect", memory_id=fail_res["rule_id"], succeeded=False)
    assert out1["utility_score"] == pytest.approx(0.6)
    assert out1["pruned"] is False

    out2 = await fn_outcome(agent_id="l7_architect", memory_id=fail_res["rule_id"], succeeded=False)
    assert out2["utility_score"] == pytest.approx(0.2)
    assert out2["pruned"] is True

    # 5. Submit Node Artifact -> Promoted VERIFIED + Lock Released
    sub_res = await fn_submit(
        dag_id="mcp-dag-1",
        node_id="design",
        agent_id="architect_agent",
        artifact_path="src/schema.py",
        artifact_type="python",
        content="from pydantic import BaseModel\n\nclass Item(BaseModel):\n    id: str\n",
        input_tokens=300,
        output_tokens=200,
    )
    assert sub_res["status"] == "VERIFIED"
    assert sub_res["released_paths"] == ["src/schema.py"]

    # 6. Telemetry Tool + Resource + Prompt + CLI
    telem = await fn_telemetry()
    assert telem["total_dags_compiled"] == 1
    assert telem["quarantine_verified"] == 1
    assert telem["pruned_rules_count"] == 1

    res_json = await resource_swarm_telemetry()
    parsed = json.loads(res_json)
    assert parsed["server_name"] == "Aegis-Swarm-Kernel"

    prompt_txt = l7_swarm_orchestrator_prompt("l7_architect", "Build billing microservice")
    assert "compile_and_route_dag" in prompt_txt
    assert "submit_node_artifact" in prompt_txt

    stdio_args = parse_cli_args([])
    assert stdio_args.transport == "stdio"

    sse_args = parse_cli_args(["--transport", "sse", "--host", "0.0.0.0", "--port", "8080"])
    assert sse_args.transport == "sse"
    assert sse_args.host == "0.0.0.0"
    assert sse_args.port == 8080

