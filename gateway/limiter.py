"""限流组件。

- ``RateLimiter``：单上游令牌桶，对应该上游的承载能力。无令牌时
  调用被视为该副本"不具备承载能力"，选择器会跳过它而不是阻塞等待。
- ``RetryBudget``：全局重试令牌桶，约束所有并发请求的重试总量。
  恢复抖动时重试令牌被取空后，新重试被放弃（直接按失败降级），
  从而避免重试风暴或跨上游放大。令牌按时间匀速补充。
"""

from __future__ import annotations

import threading

from gateway.clock import Clock
from gateway.types import RateLimiterConfig, RetryConfig


class _TokenBucket:
    def __init__(self, capacity: float, refill_per_second: float, clock: Clock) -> None:
        self._capacity = capacity
        self._refill = refill_per_second
        self._clock = clock
        self._tokens = capacity
        self._last = clock.now()
        self._lock = threading.Lock()

    def _refill_locked(self) -> None:
        now = self._clock.now()
        elapsed = now - self._last
        if elapsed > 0:
            if self._refill == float("inf"):
                self._tokens = self._capacity
            else:
                self._tokens = min(
                    self._capacity, self._tokens + elapsed * self._refill
                )
            self._last = now

    @property
    def capacity(self) -> float:
        return self._capacity

    @property
    def refill_per_second(self) -> float:
        return self._refill

    def try_acquire(self, tokens: float = 1.0) -> bool:
        with self._lock:
            self._refill_locked()
            if self._tokens + 1e-9 >= tokens:
                self._tokens -= tokens
                return True
            return False

    def refund(self, tokens: float = 1.0) -> None:
        """归还令牌（调用结束后释放承载名额）。"""
        with self._lock:
            self._tokens = min(self._capacity, self._tokens + tokens)

    def available(self) -> float:
        with self._lock:
            self._refill_locked()
            return self._tokens

    def reconfigure(self, capacity: float, refill_per_second: float) -> None:
        """运行期调整桶参数（不重启改承载能力）。

        已在途的调用不受影响（它们结束时 ``refund`` 只会把令牌
        补到新容量上限）；新容量小于当前令牌数时立即截断，
        新请求立刻按新承载能力放行或拒绝。
        """
        with self._lock:
            self._refill_locked()
            self._capacity = capacity
            self._refill = refill_per_second
            self._tokens = min(self._tokens, capacity)


class RateLimiter(_TokenBucket):
    def __init__(self, name: str, config: RateLimiterConfig, clock: Clock) -> None:
        super().__init__(config.capacity, config.refill_per_second, clock)
        self.name = name


class RetryBudget:
    """全局重试预算：所有请求共享一个令牌桶。"""

    def __init__(self, config: RetryConfig, clock: Clock) -> None:
        self._bucket = _TokenBucket(
            config.total_budget_tokens, config.budget_refill_per_second, clock
        )

    def acquire(self, tokens: int = 1) -> bool:
        return self._bucket.try_acquire(float(tokens))

    def available(self) -> float:
        return self._bucket.available()
