"""网关配置项。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BreakerConfig:
    failure_threshold: int = 3          # 连续失败多少次后熔断
    cooldown_seconds: float = 5.0       # OPEN 冷却时长
    half_open_probe_max: int = 1        # HALF_OPEN 允许的探测并发
    half_open_success_threshold: int = 1  # 探测成功多少次后 CLOSED


@dataclass(frozen=True)
class RateLimitConfig:
    capacity: float = 100.0             # 令牌桶容量（每秒）
    refill_per_second: float = 100.0    # 每秒补充令牌


@dataclass(frozen=True)
class RetryConfig:
    max_attempts_per_shard: int = 3     # 单分片最大尝试次数（含首次）
    base_backoff_seconds: float = 0.1
    max_backoff_seconds: float = 1.0
    jitter_ratio: float = 0.0           # 0 表示无抖动（确定性测试）
    budget_capacity: float = 50.0       # 全局重试令牌桶容量
    budget_refill_per_second: float = 20.0


@dataclass(frozen=True)
class GatewayConfig:
    call_timeout_seconds: float = 1.0       # 单次上游调用超时
    request_timeout_seconds: float = 3.0    # 单请求总截止时间
    fanout_concurrency: int = 32            # 分片级并发上限
    breaker: BreakerConfig = BreakerConfig()
    rate_limit: RateLimitConfig = RateLimitConfig()
    retry: RetryConfig = RetryConfig()
