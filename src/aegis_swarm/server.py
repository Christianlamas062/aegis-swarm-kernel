"""Dual-Transport FastMCP Server for `aegis-swarm-kernel` (`stdio` and `sse`)."""

from __future__ import annotations

import argparse
import asyncio
import json
import warnings
from typing import Any, Literal

warnings.filterwarnings("ignore", message=".*Field 'lifespan' has an incomplete definition.*")

from fastmcp import FastMCP

from aegis_swarm.config import ModelTier, SwarmSettings, settings
from aegis_swarm.kernel_engine import AegisSwarmKernel
from aegis_swarm.schemas import (
    DAGPlanRequest,
    NodeSubmissionRequest,
    SwarmFailureInput,
    SwarmRuleOutcomeInput,
    TaskNodeSpec,
    WorkspaceLockRequest,
)

mcp: FastMCP = FastMCP("Aegis-Swarm-Kernel")


class _KernelRegistry:
    """Concurrency-safe registry managing the active `AegisSwarmKernel` instance."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._kernel = AegisSwarmKernel(config=settings)

    async def get_kernel(self) -> AegisSwarmKernel:
        async with self._lock:
            return self._kernel

    async def reset_kernel(
        self,
        *,
        default_dag_budget_usd: float | None = None,
        default_node_token_cap: int | None = None,
        cooldown_seconds: float | None = None,
        lock_ttl_seconds: float | None = None,
    ) -> AegisSwarmKernel:
        async with self._lock:
            self._kernel = AegisSwarmKernel(
                config=settings,
                default_dag_budget_usd=default_dag_budget_usd,
                default_node_token_cap=default_node_token_cap,
                cooldown_seconds=cooldown_seconds,
                lock_ttl_seconds=lock_ttl_seconds,
            )
            return self._kernel


_REGISTRY = _KernelRegistry()


async def reset_active_kernel(
    *,
    default_dag_budget_usd: float | None = None,
    default_node_token_cap: int | None = None,
    cooldown_seconds: float | None = None,
    lock_ttl_seconds: float | None = None,
) -> AegisSwarmKernel:
    """Reinitialize the active kernel instance with optional override limits."""
    return await _REGISTRY.reset_kernel(
        default_dag_budget_usd=default_dag_budget_usd,
        default_node_token_cap=default_node_token_cap,
        cooldown_seconds=cooldown_seconds,
        lock_ttl_seconds=lock_ttl_seconds,
    )


async def get_active_kernel() -> AegisSwarmKernel:
    """Return the active `AegisSwarmKernel` instance."""
    return await _REGISTRY.get_kernel()


@mcp.tool()
async def compile_and_route_dag(
    dag_id: str,
    orchestrator_id: str,
    nodes: list[dict[str, Any]],
    max_budget_usd: float | None = None,
) -> dict[str, Any]:
    """Compile a multi-agent DAG (Kahn topological waves), assign hybrid model tiers, and compress ContextPackets.

    Rejects cyclic graphs deterministically, classifies each node to `FRONTIER_CLOUD`,
    `FAST_CLOUD`, or `LOCAL_WORKER`, computes upfront USD savings vs. an all-frontier baseline,
    and injects <5ms shared Qdrant rules into each compressed `ContextPacket`.

    Args:
        dag_id: Unique workflow DAG identifier.
        orchestrator_id: Unique identifier of the orchestrating parent agent.
        nodes: List of `TaskNodeSpec` dictionaries defining node objectives and `depends_on`.
        max_budget_usd: Optional USD ceiling for this DAG (defaults to `AEGIS_SWARM_DEFAULT_DAG_BUDGET_USD`).
    """
    kernel = await _REGISTRY.get_kernel()
    parsed_nodes = [TaskNodeSpec.model_validate(n) for n in nodes]
    req = DAGPlanRequest(
        dag_id=dag_id,
        orchestrator_id=orchestrator_id,
        nodes=parsed_nodes,
        max_budget_usd=max_budget_usd,
    )
    res = await kernel.compile_and_route_dag(req)
    return res.model_dump()


@mcp.tool()
async def acquire_workspace_lock(
    dag_id: str,
    node_id: str,
    agent_id: str,
    file_paths: list[str],
    ttl_seconds: float | None = None,
) -> dict[str, Any]:
    """Acquire atomic mutex locks on target workspace paths for a DAG node before file edits.

    Returns `LOCK_CONFLICT` if any requested path is held by another active node or agent,
    preventing parallel file race conditions across concurrent wave workers.

    Args:
        dag_id: Workflow DAG identifier.
        node_id: Node requesting exclusive write access.
        agent_id: Sub-agent identifier executing the node.
        file_paths: Workspace-relative paths to lock atomically.
        ttl_seconds: Optional lease duration in seconds (default `AEGIS_SWARM_LOCK_TTL_SEC`).
    """
    kernel = await _REGISTRY.get_kernel()
    req = WorkspaceLockRequest(
        dag_id=dag_id,
        node_id=node_id,
        agent_id=agent_id,
        file_paths=file_paths,
        ttl_seconds=ttl_seconds,
    )
    res = await kernel.acquire_workspace_lock(req)
    return res.model_dump()


@mcp.tool()
async def submit_node_artifact(
    dag_id: str,
    node_id: str,
    agent_id: str,
    content: str,
    artifact_path: str = "",
    artifact_type: Literal["python", "json", "text"] = "python",
    input_tokens: int = 0,
    output_tokens: int = 0,
    thinking_tokens: int = 0,
    reject_on_secret: bool = False,
    release_locks_on_verify: bool = True,
) -> dict[str, Any]:
    """Validate a sub-agent artifact through the Shadow Quarantine Gate before workspace promotion.

    Checks SHA-256 payload loop deduplication, per-node token caps and DAG USD budgets,
    Python AST or JSON syntax validity, and 8 secret/PII redaction rules, automatically
    releasing held workspace locks once `VERIFIED`.

    Args:
        dag_id: Workflow DAG identifier.
        node_id: Node submitting the artifact.
        agent_id: Sub-agent identifier.
        content: Generated source code, JSON, or text artifact payload.
        artifact_path: Target workspace path for extension inference and scope tagging.
        artifact_type: Artifact grammar (`python`, `json`, or `text`).
        input_tokens: Consumed input tokens to bill against node/DAG budget.
        output_tokens: Consumed output tokens to bill against node/DAG budget.
        thinking_tokens: Consumed reasoning tokens to bill against node/DAG budget.
        reject_on_secret: If True, returns `REJECTED_SECRET` when secrets/PII are present; otherwise redacts inline.
        release_locks_on_verify: Automatically release held mutex locks upon `VERIFIED` promotion.
    """
    kernel = await _REGISTRY.get_kernel()
    req = NodeSubmissionRequest(
        dag_id=dag_id,
        node_id=node_id,
        agent_id=agent_id,
        artifact_path=artifact_path,
        artifact_type=artifact_type,
        content=content,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        thinking_tokens=thinking_tokens,
        reject_on_secret=reject_on_secret,
        release_locks_on_verify=release_locks_on_verify,
    )
    res = await kernel.submit_node_artifact(req)
    return res.model_dump()


@mcp.tool()
async def record_swarm_failure(
    dag_id: str,
    node_id: str,
    agent_id: str,
    error_message: str,
    error_type: str = "EXECUTION_ERROR",
    failed_code_or_payload: str = "",
    current_tier: ModelTier = "FRONTIER_CLOUD",
    input_tokens: int = 0,
    output_tokens: int = 0,
    thinking_tokens: int = 0,
    scope_tag: str = "global",
) -> dict[str, Any]:
    """Intercept sub-agent errors, check budget/SHA-256 loop breaker, and distill shared Qdrant rules.

    On HTTP 429 or quota errors, recommends immediate fallback tier (`FRONTIER_CLOUD` -> `FAST_CLOUD`
    -> `LOCAL_WORKER`). On normalized duplicate failures within the 300s TTL, trips `LOOP_DETECTED`.

    Args:
        dag_id: Workflow DAG identifier.
        node_id: Node encountering the error.
        agent_id: Sub-agent identifier.
        error_message: Exception or error description (automatically scrubbed of secrets/PII).
        error_type: Failure classification label.
        failed_code_or_payload: Optional snippet causing the failure (scrubbed before hashing/storage).
        current_tier: Current model tier assigned to the failing node.
        input_tokens: Input tokens consumed on the failed call.
        output_tokens: Output tokens consumed on the failed call.
        thinking_tokens: Thinking tokens consumed on the failed call.
        scope_tag: File or domain tag scoping Qdrant recall bonus.
    """
    kernel = await _REGISTRY.get_kernel()
    req = SwarmFailureInput(
        dag_id=dag_id,
        node_id=node_id,
        agent_id=agent_id,
        error_type=error_type,
        error_message=error_message,
        failed_code_or_payload=failed_code_or_payload,
        current_tier=current_tier,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        thinking_tokens=thinking_tokens,
        scope_tag=scope_tag,
    )
    res = await kernel.record_swarm_failure(req)
    return res.model_dump()


@mcp.tool()
async def report_swarm_rule_outcome(
    agent_id: str,
    memory_id: str,
    succeeded: bool,
    prune_threshold: float = 0.3,
) -> dict[str, Any]:
    """Reinforce (+0.1 up to 2.0) or penalize (-0.4) a shared swarm rule and auto-prune stale rules.

    Automatically deletes any rule from the Qdrant shared bus whose utility score drops below `prune_threshold`.

    Args:
        agent_id: Identifier of the agent reporting the outcome.
        memory_id: Qdrant vector UUID of the recalled rule.
        succeeded: True if the recalled rule prevented errors or improved execution; False if ineffective.
        prune_threshold: Floor utility score below which rules are purged (default `0.3`).
    """
    kernel = await _REGISTRY.get_kernel()
    req = SwarmRuleOutcomeInput(
        agent_id=agent_id,
        memory_id=memory_id,
        succeeded=succeeded,
        prune_threshold=prune_threshold,
    )
    res = await kernel.report_swarm_rule_outcome(req)
    return res.model_dump()


@mcp.tool()
async def get_swarm_telemetry() -> dict[str, Any]:
    """Return full L7 financial savings, context compression, lock collision, quarantine, and Qdrant metrics."""
    kernel = await _REGISTRY.get_kernel()
    snap = await kernel.get_swarm_telemetry()
    return snap.model_dump()


@mcp.resource("aegisswarm://telemetry")
async def resource_swarm_telemetry() -> str:
    """Expose live Aegis-Swarm-Kernel financial, collision, and memory telemetry as JSON resource."""
    snap = await get_swarm_telemetry()
    return json.dumps(snap, indent=2)


@mcp.prompt()
def l7_swarm_orchestrator_prompt(orchestrator_id: str, project_goal: str) -> str:
    """Generate the L7 deterministic swarm orchestration protocol frame."""
    return (
        f"You are Principal Swarm Orchestrator `{orchestrator_id}` operating through `Aegis-Swarm-Kernel`.\n"
        "Protocol:\n"
        "1. Call `compile_and_route_dag` first to validate DAG acyclicity, compute parallel waves, assign hybrid tiers, and build compressed `ContextPacket`s.\n"
        "2. Before modifying any workspace files, require each node to call `acquire_workspace_lock` (`LOCK_CONFLICT` blocks races).\n"
        "3. Submit all generated artifacts through `submit_node_artifact` (Shadow Quarantine Gate) to verify AST/secrets and release locks.\n"
        "4. On sub-agent errors or HTTP 429s, call `record_swarm_failure` and honor `LOOP_DETECTED`, `BUDGET_EXCEEDED`, or `recommended_tier` fallbacks.\n\n"
        f"Project Goal: {project_goal}"
    )


def parse_cli_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for dual-transport FastMCP startup (`stdio` or `sse`)."""
    parser = argparse.ArgumentParser(description="Aegis-Swarm-Kernel L7 MCP Control Plane")
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse"],
        default=settings.transport,
        help="MCP transport mode ('stdio' default for local IDEs, 'sse' for HTTP/SSE service)",
    )
    parser.add_argument(
        "--host",
        default=settings.host,
        help="Bind host when running in SSE mode (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=settings.port,
        help="Bind port when running in SSE mode (default: 8080)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """Entrypoint supporting `stdio` and Streamable HTTP/SSE (`--transport sse --port 8080`)."""
    args = parse_cli_args(argv)
    if args.transport == "sse":
        mcp.run(transport="sse", host=args.host, port=args.port)
    else:
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()

