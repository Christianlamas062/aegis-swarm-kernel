"""Strict Pydantic v2 data contracts for `aegis-swarm-kernel`."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from aegis_swarm.config import ModelTier

TaskCategory = Literal[
    "architecture",
    "debugging",
    "security",
    "summarization",
    "docs",
    "schema_mapping",
    "boilerplate",
    "unit_tests",
    "refactoring",
    "auto",
]

QuarantineStatus = Literal[
    "QUARANTINED",
    "VERIFIED",
    "REJECTED_SYNTAX",
    "REJECTED_SECRET",
    "LOOP_DETECTED",
    "BUDGET_EXCEEDED",
]

LockStatus = Literal["ACQUIRED", "LOCK_CONFLICT", "RELEASED"]


def _require_non_blank(value: str, field_name: str) -> str:
    cleaned = value.strip()
    if not cleaned:
        raise ValueError(f"{field_name} cannot be blank or whitespace only")
    return cleaned


class TaskNodeSpec(BaseModel):
    """Specification of a single node inside a multi-agent execution DAG."""

    model_config = ConfigDict(extra="forbid")

    node_id: str = Field(..., min_length=1, max_length=128)
    title: str = Field(..., min_length=1, max_length=200)
    objective: str = Field(..., min_length=3, max_length=2000)
    task_type: TaskCategory = Field(default="auto")
    depends_on: list[str] = Field(default_factory=list)
    target_files: list[str] = Field(default_factory=list)
    target_file_signatures: list[str] = Field(default_factory=list)
    constraints: list[str] = Field(default_factory=list)
    raw_context_history: str = Field(default="")
    estimated_input_tokens: int = Field(default=600, ge=1, le=200000)
    estimated_output_tokens: int = Field(default=500, ge=1, le=200000)
    token_cap: int | None = Field(default=None, ge=100, le=250000)
    preferred_tier: ModelTier | None = Field(default=None)

    @field_validator("node_id", "title")
    @classmethod
    def check_non_blank(cls, value: str, info: Any) -> str:
        return _require_non_blank(value, info.field_name)


class SwarmRuleRecord(BaseModel):
    """A distilled procedural rule retrieved from the Qdrant shared swarm bus."""

    model_config = ConfigDict(extra="ignore")

    memory_id: str
    rule_text: str
    category: str = Field(default="swarm_remediation")
    scope_tag: str = Field(default="global")
    utility_score: float = Field(default=1.0)
    score: float = Field(default=0.0)


class ContextPacket(BaseModel):
    """Compressed structured handoff containing target signatures, constraints, and recalled rules."""

    model_config = ConfigDict(extra="forbid")

    dag_id: str
    node_id: str
    agent_role: str
    target_files: list[str] = Field(default_factory=list)
    target_file_signatures: list[str] = Field(default_factory=list)
    constraints: list[str] = Field(default_factory=list)
    recalled_procedural_rules: list[str] = Field(default_factory=list)
    raw_tokens_before_compression: int = Field(default=0, ge=0)
    compressed_tokens: int = Field(default=0, ge=0)
    compression_ratio: float = Field(default=0.0, ge=0.0, le=1.0)


class RoutedNodePlan(BaseModel):
    """Compiled routing assignment, budget allocation, and compressed handoff for one DAG node."""

    model_config = ConfigDict(extra="forbid")

    node_id: str
    title: str
    wave_index: int = Field(..., ge=0)
    assigned_tier: ModelTier
    routing_reason: str
    token_cap: int = Field(..., ge=100)
    estimated_input_tokens: int = Field(..., ge=1)
    estimated_output_tokens: int = Field(..., ge=1)
    estimated_cost_usd: float = Field(..., ge=0.0)
    all_frontier_baseline_usd: float = Field(..., ge=0.0)
    context_packet: ContextPacket


class DAGPlanRequest(BaseModel):
    """Input request for compiling and routing a multi-agent workflow DAG."""

    model_config = ConfigDict(extra="forbid")

    dag_id: str = Field(..., min_length=1, max_length=128)
    orchestrator_id: str = Field(..., min_length=1, max_length=128)
    nodes: list[TaskNodeSpec] = Field(..., min_length=1)
    max_budget_usd: float | None = Field(default=None, gt=0.0, le=10000.0)

    @field_validator("dag_id", "orchestrator_id")
    @classmethod
    def check_ids(cls, value: str, info: Any) -> str:
        return _require_non_blank(value, info.field_name)


class DAGPlanResponse(BaseModel):
    """Compiled DAG topological execution waves, hybrid routing plan, and cost savings."""

    model_config = ConfigDict(extra="forbid")

    dag_id: str
    orchestrator_id: str
    execution_waves: list[list[str]]
    routed_nodes: list[RoutedNodePlan]
    max_budget_usd: float
    total_estimated_cost_usd: float
    all_frontier_baseline_usd: float
    estimated_savings_usd: float
    estimated_savings_pct: float
    memory_recall_ms: float
    mean_compression_ratio: float


class WorkspaceLockRequest(BaseModel):
    """Request to acquire transactional mutex locks on workspace file paths for a DAG node."""

    model_config = ConfigDict(extra="forbid")

    dag_id: str = Field(..., min_length=1, max_length=128)
    node_id: str = Field(..., min_length=1, max_length=128)
    agent_id: str = Field(..., min_length=1, max_length=128)
    file_paths: list[str] = Field(..., min_length=1)
    ttl_seconds: float | None = Field(default=None, gt=0.0, le=86400.0)

    @field_validator("dag_id", "node_id", "agent_id")
    @classmethod
    def check_ids(cls, value: str, info: Any) -> str:
        return _require_non_blank(value, info.field_name)


class WorkspaceLockResponse(BaseModel):
    """Result of acquiring or releasing transactional workspace file locks."""

    model_config = ConfigDict(extra="forbid")

    status: LockStatus
    dag_id: str
    node_id: str
    agent_id: str
    locked_paths: list[str] = Field(default_factory=list)
    conflicting_paths: list[str] = Field(default_factory=list)
    held_by_node: str | None = None
    held_by_agent: str | None = None
    expires_in_sec: float = Field(default=0.0, ge=0.0)
    message: str


class NodeSubmissionRequest(BaseModel):
    """Sub-agent artifact submission entering the Shadow Quarantine Gate."""

    model_config = ConfigDict(extra="forbid")

    dag_id: str = Field(..., min_length=1, max_length=128)
    node_id: str = Field(..., min_length=1, max_length=128)
    agent_id: str = Field(..., min_length=1, max_length=128)
    artifact_path: str = Field(default="")
    artifact_type: Literal["python", "json", "text"] = Field(default="python")
    content: str = Field(..., min_length=1)
    input_tokens: int = Field(default=0, ge=0)
    output_tokens: int = Field(default=0, ge=0)
    thinking_tokens: int = Field(default=0, ge=0)
    reject_on_secret: bool = Field(default=False)
    release_locks_on_verify: bool = Field(default=True)

    @field_validator("dag_id", "node_id", "agent_id")
    @classmethod
    def check_ids(cls, value: str, info: Any) -> str:
        return _require_non_blank(value, info.field_name)


class QuarantineVerificationResult(BaseModel):
    """Verification outcome from the Shadow Quarantine Gate + Financial Circuit Breaker."""

    model_config = ConfigDict(extra="forbid")

    dag_id: str
    node_id: str
    agent_id: str
    status: QuarantineStatus
    ast_valid: bool
    secrets_found_count: int = Field(default=0, ge=0)
    redacted_content: str = Field(default="")
    released_paths: list[str] = Field(default_factory=list)
    node_tokens_used: int = Field(default=0, ge=0)
    node_cost_usd: float = Field(default=0.0, ge=0.0)
    dag_spend_usd: float = Field(default=0.0, ge=0.0)
    dag_budget_usd: float = Field(default=0.50, ge=0.0)
    error_fingerprint: str | None = None
    distilled_rule: str | None = None
    circuit_breaker_tripped: bool = False
    recalled_rules: list[SwarmRuleRecord] = Field(default_factory=list)
    error: str | None = None


class SwarmFailureInput(BaseModel):
    """Failure event submitted when a sub-agent hits an execution/rate-limit error."""

    model_config = ConfigDict(extra="forbid")

    dag_id: str = Field(..., min_length=1, max_length=128)
    node_id: str = Field(..., min_length=1, max_length=128)
    agent_id: str = Field(..., min_length=1, max_length=128)
    error_type: str = Field(default="EXECUTION_ERROR", min_length=1, max_length=64)
    error_message: str = Field(..., min_length=1)
    failed_code_or_payload: str = Field(default="")
    current_tier: ModelTier = Field(default="FRONTIER_CLOUD")
    input_tokens: int = Field(default=0, ge=0)
    output_tokens: int = Field(default=0, ge=0)
    thinking_tokens: int = Field(default=0, ge=0)
    scope_tag: str = Field(default="global", min_length=1, max_length=128)

    @field_validator("dag_id", "node_id", "agent_id")
    @classmethod
    def check_ids(cls, value: str, info: Any) -> str:
        return _require_non_blank(value, info.field_name)


class SwarmFailureOutput(BaseModel):
    """Structured action returned after evaluating loop breaker, fallback tier, and budget."""

    model_config = ConfigDict(extra="forbid")

    dag_id: str
    node_id: str
    agent_id: str
    status: Literal["RULE_DISTILLED", "LOOP_DETECTED", "RATE_LIMIT_FALLBACK", "BUDGET_EXCEEDED"]
    error_fingerprint: str
    rule_id: str
    distilled_rule: str
    circuit_breaker_tripped: bool = False
    recommended_tier: ModelTier
    dag_spend_usd: float = Field(default=0.0, ge=0.0)
    tokens_saved_estimate: int = Field(default=0, ge=0)


class SwarmRuleOutcomeInput(BaseModel):
    """Utility feedback payload for `report_swarm_rule_outcome`."""

    model_config = ConfigDict(extra="forbid")

    agent_id: str = Field(..., min_length=1, max_length=128)
    memory_id: str = Field(..., min_length=1)
    succeeded: bool
    prune_threshold: float = Field(default=0.3, ge=0.0, le=2.0)


class SwarmRuleOutcomeOutput(BaseModel):
    """Updated utility score and auto-pruning status for a shared swarm rule."""

    model_config = ConfigDict(extra="forbid")

    memory_id: str
    agent_id: str
    succeeded: bool
    utility_score: float
    pruned: bool = False


class SwarmTelemetrySnapshot(BaseModel):
    """Unified L7 control plane telemetry across routing, locking, quarantine, budget, and memory."""

    model_config = ConfigDict(extra="forbid")

    server_name: str = Field(default="Aegis-Swarm-Kernel")
    total_dags_compiled: int = Field(default=0, ge=0)
    total_nodes_routed: int = Field(default=0, ge=0)
    parallel_collisions_prevented: int = Field(default=0, ge=0)
    quarantine_submissions: int = Field(default=0, ge=0)
    quarantine_verified: int = Field(default=0, ge=0)
    quarantine_rejections: int = Field(default=0, ge=0)
    quarantine_syntax_rejections: int = Field(default=0, ge=0)
    quarantine_secret_detections: int = Field(default=0, ge=0)
    loop_interceptions: int = Field(default=0, ge=0)
    budget_trips: int = Field(default=0, ge=0)
    rate_limit_fallbacks: int = Field(default=0, ge=0)
    total_actual_cost_usd: float = Field(default=0.0, ge=0.0)
    total_baseline_cost_usd: float = Field(default=0.0, ge=0.0)
    hybrid_cost_savings_usd: float = Field(default=0.0, ge=0.0)
    hybrid_cost_savings_pct: float = Field(default=0.0, ge=0.0)
    raw_context_tokens: int = Field(default=0, ge=0)
    compressed_context_tokens: int = Field(default=0, ge=0)
    context_compression_ratio: float = Field(default=0.0, ge=0.0, le=1.0)
    memory_queries: int = Field(default=0, ge=0)
    memory_hits: int = Field(default=0, ge=0)
    memory_hit_rate: float = Field(default=0.0, ge=0.0)
    average_recall_latency_ms: float = Field(default=0.0, ge=0.0)
    active_rules_count: int = Field(default=0, ge=0)
    pruned_rules_count: int = Field(default=0, ge=0)
    recent_audit_events: list[dict[str, Any]] = Field(default_factory=list)

