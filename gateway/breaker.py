"""熔断器：CLOSED / OPEN / HALF_OPEN 三态，冷却后受控探测。

规则：
- CLOSED 下连续失败达到阈值 -> OPEN（成功会把连续失败计数清零）；
- OPEN 期间流量快速失败；冷却时间到 -> HALF_OPEN；
- HALF_OPEN 只放行固定数量的探测请求，其余仍快速失败；
- 探测成功达到阈值 -> CLOSED；探测失败 -> 重新 OPEN（冷却重置）。

两阶段接口：peek() 只查询不占名额；reserve() 才真正占用探测名额，
避免选择器遍历多个上游时把探测名额泄漏在最终未选中的副本上。
"""

from __future__ import annotations

import logging
import threading

from gateway.clock import Clock, RealClock
from gateway.config import BreakerConfig
from gateway.models import BreakerState

logger = logging.getLogger("gateway.breaker")


class CircuitBreaker:
    def __init__(
        self,
        name: str,
        config: BreakerConfig | None = None,
        clock: Clock | None = None,
    ) -> None:
        self.name = name
        self.config = config or BreakerConfig()
        self.clock: Clock = clock or RealClock()
        self._lock = threading.RLock()
        self._state = BreakerState.CLOSED
        self._consecutive_failures = 0
        self._opened_at = 0.0
        self._probes_in_flight = 0
        self._probe_successes = 0
        self._trip_count = 0

    @property
    def state(self) -> BreakerState:
        with self._lock:
            self._maybe_half_open()
            return self._state

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures

    @property
    def trip_count(self) -> int:
        return self._trip_count

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            self._maybe_half_open()
            return {
                "state": self._state.value,
                "consecutive_failures": self._consecutive_failures,
                "probes_in_flight": self._probes_in_flight,
                "probe_successes": self._probe_successes,
                "trips": self._trip_count,
            }

    def peek(self) -> tuple[bool, bool]:
        """只查询是否可放行及是否为探测，不占用探测名额。"""
        with self._lock:
            self._maybe_half_open()
            if self._state is BreakerState.CLOSED:
                return True, False
            if self._state is BreakerState.OPEN:
                return False, False
            if self._probes_in_flight < self.config.half_open_probe_max:
                return True, True
            return False, False

    def reserve(self) -> tuple[bool, bool]:
        """查询并占用名额（HALF_OPEN 时占用一个探测名额）。"""
        with self._lock:
            allowed, is_probe = self.peek()
            if not allowed:
                logger.debug("breaker[%s] reserve 判定=快速失败(%s)",
                             self.name, self._state.value)
                return False, False
            if is_probe:
                self._probes_in_flight += 1
                logger.debug("breaker[%s] reserve 判定=占用探测名额", self.name)
            return True, is_probe

    def allow(self) -> tuple[bool, bool]:
        """兼容接口：查询并占用名额。"""
        return self.reserve()

    def record_success(self, was_probe: bool) -> None:
        with self._lock:
            if was_probe:
                self._probes_in_flight = max(0, self._probes_in_flight - 1)
                self._probe_successes += 1
                if self._probe_successes >= self.config.half_open_success_threshold:
                    self._to_closed()
                    logger.info("breaker[%s] 探测成功 -> CLOSED 恢复", self.name)
            else:
                self._consecutive_failures = 0

    def record_failure(self, was_probe: bool) -> None:
        with self._lock:
            if was_probe:
                self._probes_in_flight = max(0, self._probes_in_flight - 1)
                self._to_open(reason="探测失败")
                return
            self._consecutive_failures += 1
            if self._consecutive_failures >= self.config.failure_threshold:
                self._to_open(reason=f"连续失败 {self._consecutive_failures} 次")

    def _maybe_half_open(self) -> None:
        """惰性状态迁移：OPEN 冷却到期即进入 HALF_OPEN（调用方持锁）。"""
        if self._state is not BreakerState.OPEN:
            return
        if self.clock.now() - self._opened_at >= self.config.cooldown_seconds:
            self._state = BreakerState.HALF_OPEN
            self._probes_in_flight = 0
            self._probe_successes = 0
            logger.info("breaker[%s] 冷却结束 -> HALF_OPEN 受控探测", self.name)

    def _to_open(self, reason: str) -> None:
        self._state = BreakerState.OPEN
        self._opened_at = self.clock.now()
        self._probes_in_flight = 0
        self._probe_successes = 0
        self._trip_count += 1
        logger.info("breaker[%s] -> OPEN（%s）", self.name, reason)

    def _to_closed(self) -> None:
        self._state = BreakerState.CLOSED
        self._consecutive_failures = 0
        self._probes_in_flight = 0
        self._probe_successes = 0
