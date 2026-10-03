"""灰度接管：把流量平滑地从旧副本转移到新上线副本。

语义：
- ``begin_shift`` 发起一次接管：新副本先只分一小部分流量
  （``traffic_scale = 1/steps``），之后每过 ``step_interval_seconds``
  按比例加码一档，直到 ``steps`` 档走完全部接管（旧副本比例降到 0）。
- 加码采用**惰性求值**：不起后台线程，每次 ``refresh()``（网关每请求
  调用一次）按注入的时钟计算当前应处档位，结论可复现。
- 健康护栏：加码期间一旦新副本熔断器 OPEN（变慢/报错触发）或
  被管理面停用，立即**自动撤回**——新副本比例归零、旧副本比例
  恢复为 1，状态记为 ``rolled_back`` 并给出原因。分片内未参与
  接管的副本全程不受影响，整体可用性不被拖下水。
- 全程可观测：``status()`` 返回每次接管的当前档位、各副本实际
  流量比例与状态，不是一句"已切换"。
"""

from __future__ import annotations

import itertools
import threading
from dataclasses import dataclass

from gateway.clock import Clock, SystemClock
from gateway.types import UpstreamState
from gateway.upstream import UpstreamRegistry

RAMPING = "ramping"          # 加码进行中
COMPLETED = "completed"      # 已完全接管
ROLLED_BACK = "rolled_back"  # 已自动撤回


@dataclass
class _Shift:
    shift_id: str
    shard: str
    new_replica: str
    old_replicas: tuple[str, ...]
    steps: int
    step_interval_seconds: float
    started_at: float
    state: str = RAMPING
    current_step: int = 1
    rollback_reason: str | None = None


class TrafficShiftManager:
    """灰度接管控制器：按节奏加码、按健康自动撤回。"""

    def __init__(self, registry: UpstreamRegistry, clock: Clock | None = None) -> None:
        self._registry = registry
        self._clock = clock or SystemClock()
        self._lock = threading.Lock()
        self._shifts: dict[str, _Shift] = {}
        self._ids = itertools.count(1)

    # ------------------------------------------------------------------
    def begin_shift(
        self,
        shard: str,
        new_replica: str,
        old_replicas: tuple[str, ...] | list[str],
        *,
        steps: int = 5,
        step_interval_seconds: float = 0.2,
    ) -> str:
        """发起接管。新副本立即进入第 1 档（最小流量）。"""
        if steps < 1:
            raise ValueError("steps must be >= 1")
        names = {rt.config.name for rt in self._registry.replicas_for_shard(shard)}
        for n in (new_replica, *old_replicas):
            if n not in names:
                raise KeyError(f"{n} is not a replica of shard {shard}")
        shift_id = f"shift-{next(self._ids)}"
        shift = _Shift(
            shift_id=shift_id,
            shard=shard,
            new_replica=new_replica,
            old_replicas=tuple(old_replicas),
            steps=steps,
            step_interval_seconds=step_interval_seconds,
            started_at=self._clock.now(),
        )
        with self._lock:
            self._shifts[shift_id] = shift
        self._apply(shift, step=1)
        return shift_id

    def refresh(self) -> None:
        """推进所有进行中的接管（由网关每请求调用一次）。"""
        with self._lock:
            shifts = [s for s in self._shifts.values() if s.state == RAMPING]
        for shift in shifts:
            self._evaluate(shift)

    # ------------------------------------------------------------------
    def _evaluate(self, shift: _Shift) -> None:
        rt = self._registry.get(shift.new_replica)
        # 健康护栏：新副本熔断打开或被停用 -> 立即撤回
        if rt.breaker.state() is UpstreamState.OPEN:
            self._rollback(shift, "new replica circuit open during ramp")
            return
        with rt.lock:
            admin = rt.state
        if admin is UpstreamState.DISABLED:
            self._rollback(shift, "new replica disabled during ramp")
            return
        elapsed = self._clock.now() - shift.started_at
        step = 1 + int(elapsed / shift.step_interval_seconds)
        step = max(1, min(step, shift.steps))
        if step != shift.current_step:
            self._apply(shift, step)
        if step >= shift.steps:
            with self._lock:
                shift.state = COMPLETED

    def _apply(self, shift: _Shift, step: int) -> None:
        with self._lock:
            if shift.state != RAMPING:
                return
            shift.current_step = step
        scale_new = step / shift.steps
        self._registry.set_traffic_scale(shift.new_replica, scale_new)
        for old in shift.old_replicas:
            self._registry.set_traffic_scale(old, 1.0 - scale_new)

    def _rollback(self, shift: _Shift, reason: str) -> None:
        with self._lock:
            if shift.state != RAMPING:
                return
            shift.state = ROLLED_BACK
            shift.rollback_reason = reason
        self._registry.set_traffic_scale(shift.new_replica, 0.0)
        for old in shift.old_replicas:
            self._registry.set_traffic_scale(old, 1.0)

    # ------------------------------------------------------------------
    def status(self) -> dict[str, dict[str, object]]:
        """各接管的进度与实际流量比例（可直接观测）。"""
        self.refresh()
        out: dict[str, dict[str, object]] = {}
        with self._lock:
            shifts = list(self._shifts.values())
        for s in shifts:
            share = self._traffic_share(s.shard)
            out[s.shift_id] = {
                "shard": s.shard,
                "new_replica": s.new_replica,
                "old_replicas": list(s.old_replicas),
                "state": s.state,
                "current_step": s.current_step,
                "steps": s.steps,
                "rollback_reason": s.rollback_reason,
                "traffic_share": share,
            }
        return out

    def _traffic_share(self, shard: str) -> dict[str, float]:
        """分片内各副本当前有效权重占比（权重 × 灰度比例）。"""
        runtimes = self._registry.replicas_for_shard(shard)
        eff: dict[str, float] = {}
        for rt in runtimes:
            with rt.lock:
                eff[rt.config.name] = rt.policy.weight * rt.policy.traffic_scale
        total = sum(eff.values())
        if total <= 0:
            return {k: 0.0 for k in eff}
        return {k: round(v / total, 4) for k, v in eff.items()}
