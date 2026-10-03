"""领域类型与配置定义。

本模块只包含对外稳定的数据结构：状态枚举、配置对象、请求/响应模型。
所有内部组件之间通过这些类型通信，避免底层异常直接透出到响应层。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class UpstreamState(str, Enum):
    """上游健康状态。"""

    HEALTHY = "healthy"        # 正常承载流量
    DEGRADED = "degraded"      # 可用但承载能力受限（慢/部分错误）
    OPEN = "open"              # 熔断打开：非探测流量快速失败
    HALF_OPEN = "half_open"    # 半开：受控探测
    DISABLED = "disabled"      # 管理面手动停用


class ShardStatus(str, Enum):
    """单个分片的最终结论（确定性、可解释）。"""

    OK = "ok"                          # 从某个副本成功取得数据
    DEGRADED_TIMEOUT = "timeout"       # 所有候选均超时
    DEGRADED_ERROR = "upstream_error"  # 所有候选均报错/熔断/限流
    DEGRADED_DISABLED = "no_capacity"  # 没有任何可承载的副本


class OverallStatus(str, Enum):
    """网关整体响应语义，三态保持稳定。"""

    SUCCESS = "success"    # 全部分片成功
    DEGRADED = "degraded"  # 部分分片失败，返回降级数据并标明完整度
    FAILED = "failed"      # 全部分片失败，给出可分类原因


class FailureReason(str, Enum):
    """全部不可用时的可分类失败原因（确定性，与并发时序无关）。"""

    ALL_TIMEOUT = "all_timeout"
    ALL_UPSTREAM_ERROR = "all_upstream_error"
    NO_CAPACITY = "no_capacity"
    MIXED = "mixed"
    REQUEST_DEADLINE_EXCEEDED = "request_deadline_exceeded"


@dataclass(frozen=True)
class BreakerConfig:
    """熔断器配置。"""

    failure_threshold: int = 5       # 连续失败达到该值即熔断
    cooldown_seconds: float = 2.0    # 熔断冷却时间，之后进入半开探测
    half_open_max_probe: int = 1     # 半开期间允许的并发探测数
    success_to_close: int = 2        # 半开期间连续成功多少次后闭合


@dataclass(frozen=True)
class RateLimiterConfig:
    """单上游令牌桶限流配置（承载能力模型）。"""

    capacity: float = float("inf")   # 桶容量
    refill_per_second: float = float("inf")  # 每秒补充令牌数


@dataclass(frozen=True)
class RetryConfig:
    """带预算的重试配置。

    - 单请求最多尝试 ``max_attempts`` 次（含首次）。
    - 所有重试合计最多消费 ``total_budget_tokens`` 个重试令牌，
      令牌桶按 ``budget_refill_per_second`` 恢复，防止恢复抖动引发风暴。
    - 每次尝试受分片级截止时间约束；整个请求受总截止时间约束。
    """

    max_attempts: int = 3
    backoff_base_seconds: float = 0.01
    backoff_factor: float = 2.0
    total_budget_tokens: int = 64
    budget_refill_per_second: float = 16.0
    request_timeout_seconds: float = 1.0


@dataclass(frozen=True)
class UpstreamConfig:
    """单个上游（副本）的静态配置。"""

    name: str
    shard: str
    weight: int = 1
    breaker: BreakerConfig = field(default_factory=BreakerConfig)
    rate_limiter: RateLimiterConfig = field(default_factory=RateLimiterConfig)


@dataclass
class RuntimePolicy:
    """单个副本的运行期可变策略（区别于静态 ``UpstreamConfig``）。

    支持不重启在线调整；所有字段的读写都在该副本运行时的锁内完成。
    策略只影响**变更之后**的选择与准入判断：已经在途的请求按它
    进入时拿到的名额正常跑完，不被中途打断。
    """

    weight: int = 1                     # 选择权重（越大越优先）
    capacity: float = float("inf")      # 并发承载令牌桶容量
    refill_per_second: float = float("inf")  # 令牌补充速率
    traffic_scale: float = 1.0          # 灰度流量比例 0.0~1.0（灰度接管用）


@dataclass(frozen=True)
class GatewayConfig:
    """网关级编排配置。"""

    retry: RetryConfig = field(default_factory=RetryConfig)
    per_attempt_timeout: float = 0.2   # 单次上游调用的截止时间
    max_workers: int = 16              # 分片级并发度


@dataclass(frozen=True)
class GatewayRequest:
    """进入网关的请求。"""

    request_id: str
    shards: tuple[str, ...]


@dataclass(frozen=True)
class AttemptRecord:
    """单次尝试的判定依据，用于可解释日志与响应。"""

    upstream: str
    shard: str
    attempt: int
    outcome: str          # success / timeout / error / rejected_circuit_open / rate_limited
    detail: str
    elapsed_seconds: float


@dataclass(frozen=True)
class ShardResult:
    """单个分片的结论。"""

    shard: str
    status: ShardStatus
    data: object | None
    served_by: str | None
    attempts: tuple[AttemptRecord, ...] = ()


@dataclass(frozen=True)
class GatewayResponse:
    """对外稳定响应：不含任何底层异常文本。"""

    request_id: str
    status: OverallStatus
    data: dict[str, object]
    completeness: float                 # 0.0 ~ 1.0，数据完整程度
    succeeded_shards: tuple[str, ...]
    failed_shards: tuple[str, ...]
    failure_reason: FailureReason | None
    trace: tuple[ShardResult, ...] = ()
