"""Typed configuration and hybrid model pricing for `aegis-swarm-kernel`."""

from __future__ import annotations

from typing import Final, Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

ModelTier = Literal["FRONTIER_CLOUD", "FAST_CLOUD", "LOCAL_WORKER"]

MODEL_PRICING_PER_1M: Final[dict[ModelTier, dict[str, float]]] = {
    "FRONTIER_CLOUD": {"input_usd": 3.00, "output_usd": 15.00},
    "FAST_CLOUD": {"input_usd": 0.15, "output_usd": 0.60},
    "LOCAL_WORKER": {"input_usd": 0.00, "output_usd": 0.00},
}


def calculate_cost_usd(
    tier: ModelTier,
    input_tokens: int,
    output_tokens: int,
    thinking_tokens: int = 0,
) -> float:
    """Compute USD cost for a model tier given input, output, and thinking token counts."""
    rates = MODEL_PRICING_PER_1M[tier]
    billed_input = max(0, input_tokens) * (rates["input_usd"] / 1_000_000.0)
    billed_output = max(0, output_tokens + thinking_tokens) * (rates["output_usd"] / 1_000_000.0)
    return round(billed_input + billed_output, 8)


class SwarmSettings(BaseSettings):
    """Centralized settings loaded from environment variables prefixed with `AEGIS_SWARM_`."""

    model_config = SettingsConfigDict(
        env_prefix="AEGIS_SWARM_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Financial & Execution Control Bounds
    default_dag_budget_usd: float = Field(default=0.50, gt=0.0, le=10000.0)
    default_node_token_cap: int = Field(default=4000, ge=100, le=250000)
    cooldown_sec: float = Field(default=300.0, ge=0.0)
    lock_ttl_sec: float = Field(default=120.0, gt=0.0)
    prune_threshold: float = Field(default=0.3, ge=0.0, le=2.0)

    # Shared Episodic Vector Bus (Qdrant)
    qdrant_in_memory: bool = Field(
        default=True,
        description="Run embedded in-memory Qdrant (:memory:) with zero external server required.",
    )
    qdrant_host: str = Field(default="localhost")
    qdrant_port: int = Field(default=6333, ge=1, le=65535)
    collection_name: str = Field(default="aegis_swarm_shared_memory")

    # Dual MCP Transport Configuration
    transport: Literal["stdio", "sse"] = Field(default="stdio")
    host: str = Field(default="127.0.0.1")
    port: int = Field(default=8080, ge=1, le=65535)


settings = SwarmSettings()

