"""重试策略。

- 指数退避：``base * 2**attempt``，封顶 ``max_backoff``；
- jitter_ratio>0 时采用“全抖动”风格（在 base 与退避上限之间均匀），
  随机源可注入种子，保证测试可复现；默认 jitter_ratio=0 完全确定；
- 全局预算是一个共享令牌桶，恢复抖动无法跨上游/跨请求放大为重试风暴。
"""

from __future__ import annotations

import logging
import random

from gateway.clock import Clock, RealClock
from gateway.config import RetryConfig
from gateway.ratelimit import TokenBucket

logger = logging.getLogger("gateway.retry")


class RetryPolicy:
    def __init__(
        self,
        config: RetryConfig | None = None,
        clock: Clock | None = None,
        seed: int | None = None,
    ) -> None:
        self.config = config or RetryConfig()
        self.clock: Clock = clock or RealClock()
        self._rng = random.Random(seed)
        self.budget = TokenBucket(
            "retry-budget",
            self.config.budget_capacity,
            self.config.budget_refill_per_second,
            clock=self.clock,
        )

    def backoff(self, attempt_index: int) -> float:
        """第 attempt_index 次失败后的退避秒数（0 基）。"""
        base = self.config.base_backoff_seconds
        cap = min(
            self.config.max_backoff_seconds,
            base * (2 ** attempt_index),
        )
        j = self.config.jitter_ratio
        if j <= 0:
            return cap
        # 全抖动：在 base 与 cap 之间均匀，抖动强度由 jitter_ratio 调节
        low = base
        high = base + (cap - base) * j
        if high <= low:
            return cap
        return self._rng.uniform(low, high)

    async def acquire_retry_permit(self) -> bool:
        """消耗一个全局重试令牌；预算不足返回 False（不等待、不排队）。"""
        ok = self.budget.try_acquire(1.0)
        if not ok:
            logger.debug("重试预算不足，放弃重试（防重试风暴）")
        return ok
