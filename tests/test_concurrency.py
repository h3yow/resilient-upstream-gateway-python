"""并发与故障注入：计数一致性、慢上游隔离、重试预算防风暴、抖动恢复、结论确定性。"""

from __future__ import annotations

import asyncio
import logging

import pytest

from gateway.clock import FakeClock
from gateway.config import (
    BreakerConfig,
    GatewayConfig,
    RetryConfig,
)
from gateway.models import Completeness, FailureKind
from gateway.upstream import Behavior, MockUpstream
from tests.helpers import drive


@pytest.fixture(autouse=True)
def _log(caplog):
    caplog.set_level(logging.DEBUG, logger="gateway")


async def test_call_counts_consistent_under_concurrency(caplog):
    logging.getLogger("gateway.test").info(
        "场景：100 个并发请求打到 2 个健康副本 -> 总调用计数==请求数，在途全部归零"
    )
    clk = FakeClock()
    u1 = MockUpstream("a", "s", Behavior(delay=0.2, data={"v": 1}), clock=clk)
    u2 = MockUpstream("b", "s", Behavior(delay=0.2, data={"v": 2}), clock=clk)
    from gateway.orchestrator import GatewayOrchestrator
    gw = GatewayOrchestrator([u1, u2], GatewayConfig(
        retry=RetryConfig(max_attempts_per_shard=1)), clock=clk)

    tasks = [asyncio.create_task(gw.handle(request_id=f"r{i}")) for i in range(100)]
    for _ in range(400):
        for _ in range(10):
            await asyncio.sleep(0)
        if all(t.done() for t in tasks):
            break
        await clk.advance(0.02)
    results = [await t for t in tasks]

    # 判定依据：计数守恒
    assert u1.call_count + u2.call_count == 100
    assert u1.in_flight == 0 and u2.in_flight == 0
    # 两个副本都应承担流量（负载分流），且峰值并发被分散
    assert u1.call_count > 0 and u2.call_count > 0
    assert all(r.completeness is Completeness.FULL for r in results)
    logging.getLogger("gateway.test").info(
        "判定：a=%s b=%s", u1.snapshot(), u2.snapshot()
    )


async def test_slow_upstream_does_not_block_overall(caplog):
    logging.getLogger("gateway.test").info(
        "场景：两个分片，其中一个副本全部超时；健康分片仍须在其正常延迟内返回 DEGRADED"
    )
    clk = FakeClock()
    healthy = MockUpstream("fast-a", "fast", Behavior(delay=0.1, data={"ok": 1}),
                           clock=clk)
    slow = MockUpstream("slow-a", "slow", Behavior(delay=10.0, data={"x": 1}),
                        clock=clk)
    from gateway.orchestrator import GatewayOrchestrator
    gw = GatewayOrchestrator(
        [healthy, slow],
        GatewayConfig(call_timeout_seconds=0.3,
                      request_timeout_seconds=1.0,
                      retry=RetryConfig(max_attempts_per_shard=1)),
        clock=clk,
    )
    task = asyncio.create_task(gw.handle(request_id="slow-isolation"))
    # 只推进到 0.5 虚拟秒，远小于慢上游 10s 与请求总截止
    for _ in range(60):
        for _ in range(10):
            await asyncio.sleep(0)
        if task.done():
            break
        await clk.advance(0.01)
    assert task.done(), "慢上游拖垮了整体可用性"
    res = task.result()
    assert res.completeness is Completeness.DEGRADED
    assert res.data == {"fast": {"ok": 1}}
    assert res.elapsed <= 1.05  # 没有被慢上游拖到 10s


async def test_retry_budget_prevents_retry_storm(caplog):
    logging.getLogger("gateway.test").info(
        "场景：50 并发请求，全部失败；全局重试预算仅 10 -> 总尝试次数被预算封顶"
    )
    clk = FakeClock()
    bad = MockUpstream("bad-a", "s", Behavior(ok=False, error="boom"), clock=clk)
    from gateway.orchestrator import GatewayOrchestrator
    cfg = GatewayConfig(
        breaker=BreakerConfig(failure_threshold=10_000),  # 不熔断以隔离预算效果
        retry=RetryConfig(
            max_attempts_per_shard=4,
            budget_capacity=10.0,
            budget_refill_per_second=0.0,
            base_backoff_seconds=0.02,
        ),
    )
    gw = GatewayOrchestrator([bad], cfg, clock=clk)
    tasks = [asyncio.create_task(gw.handle(request_id=f"b{i}")) for i in range(50)]
    for _ in range(500):
        for _ in range(8):
            await asyncio.sleep(0)
        if all(t.done() for t in tasks):
            break
        await clk.advance(0.01)
    results = [await t for t in tasks]

    # 首次调用每个请求 1 次 = 50；额外重试次数 <= 预算 10
    assert bad.call_count <= 50 + 10, bad.call_count
    assert bad.call_count > 50  # 预算确实被部分使用
    kinds = {r.failure_reason for r in results}
    # 判定依据：预算耗尽被显式分类（部分请求拿不到重试令牌）
    assert FailureKind.RETRY_BUDGET_EXHAUSTED in kinds
    logging.getLogger("gateway.test").info(
        "判定：总调用=%d（<=60），原因集合=%s", bad.call_count,
        {k.value for k in kinds}
    )


async def test_gradual_recovery_via_half_open_probe(caplog):
    logging.getLogger("gateway.test").info(
        "场景：副本先故障跳闸 -> 冷却中只放行探测 -> 探测成功恢复后流量回归"
    )
    clk = FakeClock()
    # 第 1、2 次失败（触发熔断），冷却后的探测成功，之后全部成功
    def behavior(i, payload):
        if i <= 2:
            return Behavior(ok=False, error="transient")
        return Behavior(data={"recovered": True})

    up = MockUpstream("flap-a", "s", behavior, clock=clk)
    from gateway.orchestrator import GatewayOrchestrator
    cfg = GatewayConfig(
        breaker=BreakerConfig(failure_threshold=2, cooldown_seconds=2.0,
                              half_open_success_threshold=1),
        retry=RetryConfig(max_attempts_per_shard=1),
    )
    gw = GatewayOrchestrator([up], cfg, clock=clk)

    r1 = await drive(gw, clk, rid="fail-1", tick=0.01)
    assert r1.failure_reason is FailureKind.UPSTREAM_ERROR
    r2 = await drive(gw, clk, rid="fail-2", tick=0.01)
    assert gw.breakers["flap-a"].state.value == "open"

    # 冷却未到：快速失败且不调用上游
    r3 = await drive(gw, clk, rid="during-open", tick=0.01)
    assert r3.failure_reason is FailureKind.NO_HEALTHY_UPSTREAM
    calls_at_open = up.call_count

    # 推进冷却
    for _ in range(250):
        for _ in range(4):
            await asyncio.sleep(0)
        await clk.advance(0.01)
    assert gw.breakers["flap-a"].state.value == "half_open"

    # 探测请求成功 -> 恢复
    r4 = await drive(gw, clk, rid="probe", tick=0.01)
    assert r4.completeness is Completeness.FULL
    assert up.call_count == calls_at_open + 1
    assert gw.breakers["flap-a"].state.value == "closed"


async def test_same_fault_input_yields_consistent_conclusion(caplog):
    logging.getLogger("gateway.test").info(
        "场景：同一故障注入分别以 1 个请求与 32 个并发请求执行，最终结论必须一致"
    )
    clk = FakeClock()

    def build(clock):
        up = MockUpstream("x-a", "x",
                          Behavior(ok=False, error="deterministic-boom"), clock=clock)
        from gateway.orchestrator import GatewayOrchestrator
        return up, GatewayOrchestrator(
            [up],
            GatewayConfig(
                breaker=BreakerConfig(failure_threshold=10_000),
                retry=RetryConfig(max_attempts_per_shard=2, budget_capacity=100.0,
                                  base_backoff_seconds=0.01),
            ),
            clock=clock,
        )

    up1, gw1 = build(clk)
    single = await drive(gw1, clk, rid="single", tick=0.01)

    clk2 = FakeClock()
    up2, gw2 = build(clk2)
    tasks = [asyncio.create_task(gw2.handle(request_id=f"c{i}")) for i in range(32)]
    for _ in range(500):
        for _ in range(8):
            await asyncio.sleep(0)
        if all(t.done() for t in tasks):
            break
        await clk2.advance(0.01)
    concurrent = [await t for t in tasks]

    # 判定依据：无论并发时序如何，结论（完整度+原因分类）一致
    assert single.completeness is Completeness.UNAVAILABLE
    assert single.failure_reason is FailureKind.UPSTREAM_ERROR
    assert all(r.completeness is Completeness.UNAVAILABLE for r in concurrent)
    assert all(r.failure_reason is FailureKind.UPSTREAM_ERROR for r in concurrent)


async def test_state_flapping_reopens_and_conclusions_stable(caplog):
    logging.getLogger("gateway.test").info(
        "场景：上游状态抖动（失败-成功-失败交替）。序列：前2次失败跳闸；冷却后"
        "探测成功恢复；随后再次失败需重新累计。验证多次 OPEN<->CLOSED 转换且"
        "结论始终可分类、无异常透出。"
    )
    clk = FakeClock()
    # 调用序列：1,2 失败 -> 跳闸；3 探测成功 -> 恢复；4,5 失败 -> 再次跳闸
    seq = {1: False, 2: False, 3: True, 4: False, 5: False}

    def behavior(i, payload):
        ok = seq.get(i, True)
        return Behavior(ok=ok, error=f"flap-{i}") if not ok else Behavior(data={"n": i})

    up = MockUpstream("flap", "s", behavior, clock=clk)
    from gateway.orchestrator import GatewayOrchestrator
    cfg = GatewayConfig(
        breaker=BreakerConfig(failure_threshold=2, cooldown_seconds=1.0,
                              half_open_success_threshold=1),
        retry=RetryConfig(max_attempts_per_shard=1),
    )
    gw = GatewayOrchestrator([up], cfg, clock=clk)

    # 前两个请求失败并跳闸
    for rid in ("j1", "j2"):
        r = await drive(gw, clk, rid=rid, tick=0.01)
        assert r.failure_reason is FailureKind.UPSTREAM_ERROR
    assert gw.breakers["flap"].state.value == "open"

    # 冷却中快速失败（不触达上游，调用数不变）
    blocked = await drive(gw, clk, rid="j-blocked", tick=0.01)
    assert blocked.failure_reason is FailureKind.NO_HEALTHY_UPSTREAM
    assert up.call_count == 2

    # 推进冷却 -> 探测成功恢复
    for _ in range(120):
        for _ in range(4):
            await asyncio.sleep(0)
        await clk.advance(0.01)
    probe = await drive(gw, clk, rid="j-probe", tick=0.01)
    assert probe.completeness is Completeness.FULL
    assert up.call_count == 3
    assert gw.breakers["flap"].state.value == "closed"

    # 恢复后再次连续失败 -> 重新跳闸（第二次 OPEN，trip_count 累计）
    r4 = await drive(gw, clk, rid="j4", tick=0.01)
    r5 = await drive(gw, clk, rid="j5", tick=0.01)
    assert r4.failure_reason is FailureKind.UPSTREAM_ERROR
    assert r5.failure_reason is FailureKind.UPSTREAM_ERROR
    assert gw.breakers["flap"].state.value == "open"
    assert gw.breakers["flap"].trip_count == 2
    logging.getLogger("gateway.test").info(
        "判定：抖动序列下熔断跳闸次数=%d，总调用=%d，全部结论均为枚举分类",
        gw.breakers["flap"].trip_count, up.call_count,
    )
