"""领域模型：失败分类、熔断器状态、降级完整度与结果结构。"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


class FailureKind(str, enum.Enum):
    """可分类、可解释的失败原因（底层异常不会原样透出）。"""

    TIMEOUT = "timeout"
    UPSTREAM_ERROR = "upstream_error"
    RATE_LIMITED = "rate_limited"
    CIRCUIT_OPEN = "circuit_open"
    NO_HEALTHY_UPSTREAM = "no_healthy_upstream"
    DEADLINE_EXCEEDED = "deadline_exceeded"
    RETRY_BUDGET_EXHAUSTED = "retry_budget_exhausted"


class BreakerState(str, enum.Enum):
    """熔断器三态。"""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class Completeness(str, enum.Enum):
    """响应数据完整程度。"""

    FULL = "full"            # 所有分片成功
    DEGRADED = "degraded"    # 部分分片成功
    UNAVAILABLE = "unavailable"  # 全部分片失败


@dataclass(frozen=True)
class AttemptRecord:
    """单次调用尝试的记录，用于日志与判定解释。"""

    replica: str
    shard: str
    ok: bool
    failure_kind: FailureKind | None
    latency: float
    probe: bool = False


@dataclass
class ShardOutcome:
    """单个分片（一次逻辑路由目标）的最终结论。"""

    shard: str
    ok: bool
    replica: str | None = None
    data: Any = None
    attempts: int = 0
    failure_kind: FailureKind | None = None
    attempt_records: list[AttemptRecord] = field(default_factory=list)


@dataclass
class GatewayResult:
    """网关对外响应：结论稳定、字段可解释。"""

    request_id: str
    completeness: Completeness
    data: dict[str, Any] = field(default_factory=dict)
    shard_outcomes: list[ShardOutcome] = field(default_factory=list)
    failure_reason: FailureKind | None = None
    elapsed: float = 0.0

    @property
    def succeeded_shards(self) -> list[str]:
        return [o.shard for o in self.shard_outcomes if o.ok]

    @property
    def failed_shards(self) -> list[str]:
        return [o.shard for o in self.shard_outcomes if not o.ok]
