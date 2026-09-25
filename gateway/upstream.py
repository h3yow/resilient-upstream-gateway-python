"""本地模拟上游：行为可脚本化（成功/报错/延迟），调用计数在并发下一致。

不依赖任何外部服务。行为可通过：
1. 固定 Behavior；
2. 可调用对象 ``fn(call_index, payload) -> Behavior``（可为 async），
   从而按调用序号、共享场景状态模拟“逐步恢复 / 状态抖动”。
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from gateway.clock import Clock, RealClock


class UpstreamError(Exception):
    """模拟上游主动抛出的错误（底层异常，禁止原样透出到网关响应）。"""


@dataclass
class Behavior:
    ok: bool = True                 # True 成功返回 data；False 抛 UpstreamError
    data: Any = None
    delay: float = 0.0              # 注入的处理延迟（秒，走注入时钟）
    error: str = "injected upstream failure"  # 失败时的内部原因（仅日志）


BehaviorFn = Callable[[int, Any], "Awaitable[Behavior] | Behavior"]


class MockUpstream:
    """单个上游副本。计数更新点之间不含 await，保证并发视图一致。"""

    def __init__(
        self,
        name: str,
        shard: str,
        behavior: Behavior | BehaviorFn | None = None,
        clock: Clock | None = None,
    ) -> None:
        self.name = name
        self.shard = shard
        self._behavior = behavior if behavior is not None else Behavior(ok=True)
        self.clock: Clock = clock or RealClock()
        self._call_count = 0
        self._in_flight = 0
        self._peak_in_flight = 0
        self._errors = 0
        self._history: list[str] = []

    @property
    def call_count(self) -> int:
        return self._call_count

    @property
    def error_count(self) -> int:
        return self._errors

    @property
    def in_flight(self) -> int:
        return self._in_flight

    @property
    def peak_in_flight(self) -> int:
        return self._peak_in_flight

    @property
    def history(self) -> tuple[str, ...]:
        return tuple(self._history)

    def snapshot(self) -> dict[str, int]:
        """一致性可观测快照。"""
        return {
            "calls": self._call_count,
            "in_flight": self._in_flight,
            "peak": self._peak_in_flight,
            "errors": self._errors,
        }

    def reset(self) -> None:
        self._call_count = 0
        self._in_flight = 0
        self._peak_in_flight = 0
        self._errors = 0
        self._history.clear()

    def reserve_slot(self) -> None:
        """在调用真正开始前预订一个在途名额（选择器用于承载分流）。"""
        self._in_flight += 1
        if self._in_flight > self._peak_in_flight:
            self._peak_in_flight = self._in_flight

    def release_slot(self) -> None:
        self._in_flight -= 1

    async def call(self, payload: Any = None, request_id: str = "",
                   slot_reserved: bool = False) -> Any:
        # 进入临界区：以下若干语句之间无 await
        self._call_count += 1
        index = self._call_count
        if not slot_reserved:
            self.reserve_slot()
        self._history.append(f"req={request_id}#{index}")
        behavior = await self._resolve(index, payload)
        try:
            if behavior.delay > 0:
                await self.clock.sleep(behavior.delay)
            if not behavior.ok:
                self._errors += 1
                raise UpstreamError(
                    f"[{self.name}] {behavior.error} (call#{index}, req={request_id})"
                )
            if behavior.data is not None:
                return behavior.data
            return {"upstream": self.name, "shard": self.shard, "echo": payload}
        finally:
            # 无论成功、失败还是被取消都回收在途名额
            self.release_slot()

    async def _resolve(self, index: int, payload: Any) -> Behavior:
        beh = self._behavior
        if callable(beh):
            produced = beh(index, payload)
            if inspect.isawaitable(produced):
                produced = await produced
            return produced
        return beh
