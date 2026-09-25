"""可注入时钟：生产用真实时钟，测试用虚拟事件时钟。

所有超时、熔断冷却、退避等待都经过 Clock，使故障注入测试完全确定、
不依赖真实 sleep。
"""

from __future__ import annotations

import asyncio
import heapq
import itertools
import time
from typing import Protocol


class Clock(Protocol):
    def now(self) -> float: ...
    async def sleep(self, seconds: float) -> None: ...


class RealClock:
    def now(self) -> float:
        return time.monotonic()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


class FakeClock:
    """事件驱动虚拟时钟。

    - sleep 只注册一个带到期时间的 Future，不消耗真实时间；
    - advance(seconds) 按到期顺序触发时间点，到期任务会获得执行机会，
      其新注册的定时器（如重试退避、下游延迟）会在同一推进循环中处理；
    - 被取消的定时器自动跳过（对应调用超时后取消等待中的上游任务）。
    """

    def __init__(self, start: float = 1000.0) -> None:
        self._now = start
        self._timers: list[tuple[float, int, asyncio.Future]] = []
        self._seq = itertools.count()

    # 每个定时器触发后排空就绪队列的轮数；需覆盖一次推进中最坏的协程恢复链深度
    DRAIN_STEPS = 64

    def now(self) -> float:
        return self._now

    async def sleep(self, seconds: float) -> None:
        # 零等待立即让出一次执行权即可，不进入定时器堆；
        # 否则它会与同刻到期的超时定时器产生入堆顺序竞争。
        if seconds <= 0:
            await asyncio.sleep(0)
            return
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        tie = next(self._seq)
        heapq.heappush(self._timers, (self._now + seconds, tie, fut))
        try:
            await fut
        except asyncio.CancelledError:
            if not fut.done():
                fut.cancel()
            raise

    async def advance(self, seconds: float) -> int:
        """把虚拟时间向前推进 seconds，返回触发的定时器数量。

        每触发一个定时器后，都通过有限轮 ``sleep(0)`` 把“因该定时器而恢复”的
        就绪协程链彻底排空，再触发下一个定时器。这样零延迟 / 链式重试等协程有
        机会在更晚的定时器（如请求截止）到期前完成，符合真实事件循环语义。
        """
        end = self._now + seconds
        fired = 0
        while self._timers and self._timers[0][0] <= end + 1e-12:
            at, _, fut = heapq.heappop(self._timers)
            if fut.cancelled():
                continue
            self._now = max(self._now, at)
            fut.set_result(None)
            fired += 1
            # 排空就绪队列；DRAIN_STEPS 只需覆盖最坏的协程恢复链深度
            for _ in range(self.DRAIN_STEPS):
                await asyncio.sleep(0)
        self._now = end
        return fired

    def pending(self) -> int:
        """尚未触发且未取消的定时器数量（测试可观测性）。"""
        return sum(1 for _, _, fut in self._timers if not fut.cancelled())
