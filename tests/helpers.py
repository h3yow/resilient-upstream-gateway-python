"""测试辅助：用虚拟时钟把一个网关请求跑到完成。

生产代码中的时钟通过依赖注入替换为 FakeClock，因此故障注入场景
（超时 / 报错 / 缓慢恢复 / 抖动）全部可在零真实耗时下确定复现。
"""

from __future__ import annotations

import asyncio

from gateway.clock import FakeClock
from gateway.orchestrator import GatewayOrchestrator


async def drive(
    gw: GatewayOrchestrator,
    clk: FakeClock,
    payload: object = None,
    rid: str = "test",
    *,
    tick: float = 0.01,
    max_virtual_seconds: float = 60.0,
):
    """逐步推进虚拟时钟直到请求完成；每一步先排空就绪队列。"""
    task = asyncio.create_task(gw.handle(payload, rid))
    steps = int(max_virtual_seconds / tick)
    for _ in range(steps):
        # 让所有可运行的协程（选择、注册定时器等）推进到挂起点
        for _ in range(32):
            await asyncio.sleep(0)
        if task.done():
            return task.result()
        await clk.advance(tick)
    for _ in range(32):
        await asyncio.sleep(0)
    if task.done():
        return task.result()
    raise AssertionError(
        f"请求在 {max_virtual_seconds} 虚拟秒内未完成；残留定时器={clk.pending()}"
    )
