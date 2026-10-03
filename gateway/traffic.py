"""灰度流量接管：把某个分片的流量从旧副本平滑切到新副本。

语义：
- 新副本（target）先只分一小部分流量（``start_share``），之后每过
  ``step_seconds`` 按 ``step_share`` 加码，直到 100% 完全替下旧副本
  （source 被排空，权重乘子归零）。
- 加码期间持续观察 target 的健康：失败数（含超时）相对基线增长达到
  ``rollback_failure_threshold``，或 target 熔断打开，立即撤回——
  target 乘子归零、source 保持全量，整体可用性不受影响。
- 所有推进都是**惰性、时钟驱动**的（与熔断器同一风格）：选择器每次
  计算候选时询问乘子，管理器在此时刻评估进度与健康，无需后台线程。
- ``status()`` 随时给出接管阶段、当前份额、两个副本的生效权重，
  可观测、可断言。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from enum import Enum

from gateway.clock import Clock, SystemClock
from gateway.types import UpstreamState
from gateway.upstream import UpstreamRegistry


class ShiftPhase(str, Enum):
    RAMPING = "ramping"        # 加码进行中
    COMPLETE = "complete"      # 已完全接管（source 排空）
    ROLLED_BACK = "rolled_back"  # 已撤回（target 排空，source 全量）


@dataclass(frozen=True)
class TrafficShiftPlan:
    """一次灰度接管计划。"""

    shard: str
    source: str                       # 被替下的旧副本
    target: str                       # 新上线副本
    start_share: float = 0.1          # 初始流量份额
    step_share: float = 0.15          # 每个阶梯增加的份额
    step_seconds: float = 0.05        # 阶梯间隔
    rollback_failure_threshold: int = 1  # 加码期间允许的目标失败次数上限


@dataclass
class _ShiftState:
    plan: TrafficShiftPlan
    started_at: float
    phase: ShiftPhase = ShiftPhase.RAMPING
    current_share: float = 0.0
    baseline_failures: int = 0        # 启动时 target 的累计失败数
    ramp_failures: int = 0            # 加码期间 target 新增失败数
    history: list[str] = field(default_factory=list)


class TrafficShiftManager:
    """按分片管理灰度接管计划，并向选择器提供权重乘子。"""

    def __init__(self, registry: UpstreamRegistry, clock: Clock | None = None) -> None:
        self._registry = registry
        self._clock = clock or SystemClock()
        self._lock = threading.Lock()
        self._by_shard: dict[str, _ShiftState] = {}

    # ------------------------------------------------------------------
    def start_shift(self, plan: TrafficShiftPlan) -> dict[str, object]:
        """启动接管计划；同一分片同时只允许一个进行中的计划。"""
        source_rt = self._registry.get(plan.source)
        target_rt = self._registry.get(plan.target)
        if source_rt.config.shard != plan.shard or target_rt.config.shard != plan.shard:
            raise ValueError(
                f"source/target must belong to shard {plan.shard!r}"
            )
        with target_rt.lock:
            baseline = target_rt.failure_count
        st = _ShiftState(
            plan=plan,
            started_at=self._clock.now(),
            current_share=plan.start_share,
            baseline_failures=baseline,
        )
        st.history.append(
            f"start: share={plan.start_share:.2f} baseline_failures={baseline}"
        )
        with self._lock:
            self._by_shard[plan.shard] = st
        return self.status(plan.shard)

    def weight_multiplier(self, name: str, shard: str) -> float:
        """选择器查询：该副本当前的生效权重乘子（0 表示排空）。"""
        with self._lock:
            st = self._by_shard.get(shard)
            if st is None:
                return 1.0
            self._evaluate_locked(st)
            if name == st.plan.target:
                return st.current_share if st.phase is not ShiftPhase.ROLLED_BACK else 0.0
            if name == st.plan.source:
                return 0.0 if st.phase is ShiftPhase.COMPLETE else 1.0
            return 1.0

    def status(self, shard: str | None = None) -> dict[str, object]:
        """接管进度快照：阶段、当前份额、生效权重、加码期失败数。"""
        with self._lock:
            shards = [shard] if shard else sorted(self._by_shard)
            out: dict[str, object] = {}
            for s in shards:
                st = self._by_shard.get(s)
                if st is None:
                    continue
                self._evaluate_locked(st)
                out[s] = {
                    "phase": st.phase.value,
                    "source": st.plan.source,
                    "target": st.plan.target,
                    "current_share": round(st.current_share, 4),
                    "effective_weights": {
                        st.plan.source: (
                            0.0 if st.phase is ShiftPhase.COMPLETE else 1.0
                        ),
                        st.plan.target: (
                            st.current_share
                            if st.phase is not ShiftPhase.ROLLED_BACK
                            else 0.0
                        ),
                    },
                    "ramp_failures": st.ramp_failures,
                    "elapsed_seconds": round(self._clock.now() - st.started_at, 4),
                    "history": list(st.history),
                }
            return out

    # ------------------------------------------------------------------
    def _evaluate_locked(self, st: _ShiftState) -> None:
        """惰性推进：先查健康，再按时间加码（调用方持锁）。"""
        if st.phase is not ShiftPhase.RAMPING:
            return
        rt = self._registry.get(st.plan.target)
        with rt.lock:
            failures = rt.failure_count
            breaker_open = rt.breaker.state() is UpstreamState.OPEN
        st.ramp_failures = failures - st.baseline_failures
        if st.ramp_failures >= st.plan.rollback_failure_threshold or breaker_open:
            st.phase = ShiftPhase.ROLLED_BACK
            st.current_share = 0.0
            st.history.append(
                f"rollback: ramp_failures={st.ramp_failures} breaker_open={breaker_open}"
            )
            return
        elapsed = self._clock.now() - st.started_at
        steps = int(elapsed // st.plan.step_seconds)
        share = min(1.0, st.plan.start_share + steps * st.plan.step_share)
        if share != st.current_share:
            st.history.append(f"ramp: share={share:.2f}")
        st.current_share = share
        if share >= 1.0:
            st.phase = ShiftPhase.COMPLETE
            st.history.append("complete: source drained")
