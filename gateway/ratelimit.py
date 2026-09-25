"""令牌桶限流器。

惰性补充：不依赖后台任务，按距上次补充经过的时间补充令牌。
熔断的“非探测快速失败”与限流的“令牌不足快速失败”在上层被区分分类。
同一原语也用于全局重试预算，避免恢复抖动放大为重试风暴。
"""

from __future__ import annotations

import threading

from gateway.clock import Clock, RealClock


class TokenBucket:
    def __init__(
        self,
        name: str,
        capacity: float,
        refill_per_second: float,
        clock: Clock | None = None,
    ) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self.name = name
        self.capacity = float(capacity)
        self.refill_per_second = float(refill_per_second)
        self.clock: Clock = clock or RealClock()
        self._lock = threading.Lock()
        self._tokens = float(capacity)
        self._last = self.clock.now()
        self._rejected = 0
        self._accepted = 0

    @property
    def rejected(self) -> int:
        return self._rejected

    def try_acquire(self, tokens: float = 1.0) -> bool:
        with self._lock:
            now = self.clock.now()
            elapsed = now - self._last
            if elapsed > 0:
                self._tokens = min(
                    self.capacity, self._tokens + elapsed * self.refill_per_second
                )
                self._last = now
            if self._tokens + 1e-9 >= tokens:
                self._tokens -= tokens
                self._accepted += 1
                return True
            self._rejected += 1
            return False

    def available(self) -> float:
        with self._lock:
            elapsed = self.clock.now() - self._last
            return min(self.capacity, self._tokens + elapsed * self.refill_per_second)
