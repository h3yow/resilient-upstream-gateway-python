"""时钟抽象。

默认实现使用真实单调时钟；测试通过使用小的时间常量（毫秒级延迟、
短冷却）来保证用例快速且结论可复现。
"""

from __future__ import annotations

import time
from typing import Protocol


class Clock(Protocol):
    def now(self) -> float:
        """当前单调时间（秒）。"""
        ...

    def sleep(self, seconds: float) -> None:
        """睡眠指定秒数。"""
        ...


class SystemClock:
    """基于 ``time.monotonic`` 的真实时钟。"""

    def now(self) -> float:
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        if seconds > 0:
            time.sleep(seconds)
