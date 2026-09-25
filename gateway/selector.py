"""上游选择：先过熔断，再按承载能力（最少在途数）分流，平局确定性轮询。

- 用 breaker.peek() 遍历（不占用探测名额），排除 OPEN / 探测名额满的副本；
- 可放行副本中选“在途调用数最少”的；在途数相同时按每选择器独立的确定性
  轮询游标轮换（初始顺序按名字排序），保证分流且同一时序下结论一致；
- 定选时同时：(a) breaker.reserve() 占用探测名额；(b) upstream.reserve_slot()
  预订在途名额。于是即使调用任务尚未真正开始执行，承载也已对后续选择可见；
- 普通（CLOSED）副本优先于 HALF_OPEN 探测副本；
- 同一请求内已失败副本在后续重试中被优先排除（无其他副本时回退）。
"""

from __future__ import annotations

import itertools
import logging
from dataclasses import dataclass, field

from gateway.breaker import CircuitBreaker
from gateway.upstream import MockUpstream

logger = logging.getLogger("gateway.selector")


@dataclass
class SelectionContext:
    """单次请求的选择状态：记录本次请求已失败的副本，仅供请求内重试使用。"""

    failed_replicas: set[str] = field(default_factory=set)

    def record_failure(self, name: str) -> None:
        self.failed_replicas.add(name)


class UpstreamSelector:
    def __init__(
        self,
        replicas: list[MockUpstream],
        breakers: dict[str, CircuitBreaker],
    ) -> None:
        # 固定规范顺序，轮询游标据此轮换，保证可复现
        self.replicas = sorted(replicas, key=lambda up: up.name)
        self.breakers = breakers
        self._rr = itertools.count()

    def select(
        self, ctx: SelectionContext | None = None
    ) -> tuple[MockUpstream | None, bool]:
        excluded = ctx.failed_replicas if ctx is not None else set()
        normal: list[MockUpstream] = []
        probes: list[MockUpstream] = []
        for up in self.replicas:
            allowed, is_probe = self.breakers[up.name].peek()
            if not allowed:
                continue
            (probes if is_probe else normal).append(up)

        def fresh(pool):
            filtered = [up for up in pool if up.name not in excluded]
            return filtered or pool

        pool = fresh(normal) if normal else fresh(probes)
        if not pool:
            logger.debug(
                "选择结果=无可用上游（%d 个副本全部熔断/探测名额满）", len(self.replicas)
            )
            return None, False

        # 主排序键：在途数最少；平局时用轮询游标在“当前最空闲集合”内轮换
        min_load = min(up.in_flight for up in pool)
        candidates = [up for up in pool if up.in_flight == min_load]
        if len(candidates) == 1:
            up = candidates[0]
        else:
            # 候选在规范顺序中的起始位置由轮询游标决定
            ordered = sorted(candidates, key=lambda u: u.name)
            start = next(self._rr) % len(ordered)
            up = ordered[start]

        name = up.name
        is_probe = any(u.name == name for u in probes)
        # 定选后占用熔断探测名额 + 预订在途名额（并发下承载立即可见）
        allowed, _ = self.breakers[name].reserve()
        if not allowed:
            logger.debug("上游 %s 探测名额在定选时被抢占", name)
            return None, False
        up.reserve_slot()
        logger.debug("选择结果=上游 %s（in_flight=%d, probe=%s, 已失败副本=%s）",
                     name, up.in_flight, is_probe, sorted(excluded))
        return up, is_probe
