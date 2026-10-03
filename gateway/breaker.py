"""熔断器：CLOSED -> OPEN -> HALF_OPEN -> CLOSED 状态机。

- CLOSED 下连续失败达到阈值转为 OPEN。
- OPEN 期间非探测流量快速失败；冷却时间过后，至多
  ``half_open_max_probe`` 个探测请求可以进入 HALF_OPEN。
- HALF_OPEN 下连续成功达到阈值则 CLOSED；任一失败立即回到 OPEN，
  并重新开始冷却计时（避免抖动恢复导致探测流量放大）。
所有状态迁移在单个互斥锁内完成，保证并发下结论一致、无重复探测。
"""

from __future__ import annotations

import threading

from gateway.clock import Clock
from gateway.types import BreakerConfig, UpstreamState

_CLOSED = UpstreamState.HEALTHY  # 健康即熔断闭合
_OPEN = UpstreamState.OPEN
_HALF_OPEN = UpstreamState.HALF_OPEN


class CircuitBreaker:
    def __init__(self, name: str, config: BreakerConfig, clock: Clock) -> None:
        self.name = name
        self.config = config
        self._clock = clock
        self._lock = threading.Lock()
        self._state: UpstreamState = _CLOSED
        self._consecutive_failures = 0
        self._consecutive_successes = 0
        self._opened_at = 0.0
        self._probes_in_flight = 0
        # 半开期间被放行过的总探测数（成功闭合后重置）
        self._probes_tried = 0

    def _maybe_enter_half_open_locked(self) -> None:
        """冷却已过则 OPEN -> HALF_OPEN（调用方持锁）。"""
        if self._state is _OPEN and self._clock.now() - self._opened_at >= self.config.cooldown_seconds:
            self._state = _HALF_OPEN
            self._consecutive_successes = 0
            self._probes_in_flight = 0
            self._probes_tried = 0

    def state(self) -> UpstreamState:
        """当前状态；冷却到期会惰性迁移到 HALF_OPEN 以便观测。"""
        with self._lock:
            self._maybe_enter_half_open_locked()
            return self._state

    def allow_request(self) -> bool:
        """是否放行本次调用（占用式，兼容旧接口）。

        CLOSED 总是放行；OPEN 冷却未过一律拒绝（快速失败）；
        HALF_OPEN 仅在探测名额内放行，名额外的流量继续快速失败。
        等价于 ``acquire_probe``：返回 True 即占用一个探测名额，
        调用方必须随后 ``record_success`` / ``record_failure`` /
        ``release_probe`` 之一归还结论，否则名额泄漏。
        """
        return self.acquire_probe()

    def probe_available(self) -> bool:
        """只读检查：当前是否允许放行（不占用探测名额）。

        供选择器做候选筛选使用——筛选阶段不能消耗名额，否则
        候选被列出但未被真正调用时名额会永久泄漏，副本将永远
        停在 HALF_OPEN 无法恢复。
        """
        with self._lock:
            self._maybe_enter_half_open_locked()
            if self._state is _CLOSED:
                return True
            if self._state is _OPEN:
                return False
            return (
                self._probes_in_flight < self.config.half_open_max_probe
                and self._probes_tried < self.config.success_to_close
            )

    def acquire_probe(self) -> bool:
        """占用式放行：返回 True 表示本次调用被允许且已记账。

        HALF_OPEN 下占用一个探测名额（受 ``half_open_max_probe``
        与 ``success_to_close`` 双重上界约束，探测流量不会因
        并发涌入而放大）。
        """
        with self._lock:
            self._maybe_enter_half_open_locked()
            if self._state is _CLOSED:
                return True
            if self._state is _OPEN:
                return False
            # HALF_OPEN：受控探测
            if self._probes_in_flight >= self.config.half_open_max_probe:
                return False
            if self._probes_tried >= self.config.success_to_close:
                # 所需探测都已在途/已用完，其余流量等结论
                return False
            self._probes_in_flight += 1
            self._probes_tried += 1
            return True

    def release_probe(self) -> None:
        """放弃一个已占用但未真正发起调用的探测名额。

        用于"选中后未调用"的路径（如占用名额后被限流、或请求
        截止时间已到）：名额归还、不计成败，保证每个副本最终
        都能被真正探测到，不会因名额泄漏而永远停在 HALF_OPEN。
        """
        with self._lock:
            if self._state is _HALF_OPEN:
                self._probes_in_flight = max(0, self._probes_in_flight - 1)
                self._probes_tried = max(0, self._probes_tried - 1)

    def record_success(self) -> None:
        with self._lock:
            if self._state is _HALF_OPEN:
                self._probes_in_flight = max(0, self._probes_in_flight - 1)
                self._consecutive_successes += 1
                if self._consecutive_successes >= self.config.success_to_close:
                    self._state = _CLOSED
                    self._consecutive_failures = 0
                    self._consecutive_successes = 0
                    self._probes_tried = 0
            else:
                self._consecutive_failures = 0

    def record_failure(self) -> None:
        with self._lock:
            if self._state is _HALF_OPEN:
                self._probes_in_flight = max(0, self._probes_in_flight - 1)
                # 探测失败：立即重新熔断并重新计时冷却
                self._state = _OPEN
                self._opened_at = self._clock.now()
                self._consecutive_successes = 0
                return
            self._consecutive_failures += 1
            if self._consecutive_failures >= self.config.failure_threshold:
                self._state = _OPEN
                self._opened_at = self._clock.now()

    def force_state(self, state: UpstreamState) -> None:
        """管理面/测试直接设定状态。"""
        with self._lock:
            self._state = state
            self._consecutive_failures = 0
            self._consecutive_successes = 0
            self._probes_in_flight = 0
            self._probes_tried = 0
            if state is _OPEN:
                self._opened_at = self._clock.now()
