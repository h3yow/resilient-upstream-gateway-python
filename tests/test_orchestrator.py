"""端到端编排：成功、报错、超时、熔断降级、全失败原因分类与异常归一化。"""

from __future__ import annotations

import logging

import pytest

from gateway.clock import FakeClock
from gateway.config import (
    BreakerConfig,
    GatewayConfig,
    RateLimitConfig,
    RetryConfig,
)
from gateway.models import Completeness, FailureKind
from gateway.upstream import Behavior, MockUpstream
from tests.helpers import drive


def make_gateway(upstreams, clk, **cfg_kw):
    from gateway.orchestrator import GatewayOrchestrator
    return GatewayOrchestrator(upstreams, GatewayConfig(**cfg_kw), clock=clk)


@pytest.fixture(autouse=True)
def _log(caplog):
    caplog.set_level(logging.DEBUG, logger="gateway")


async def test_full_success(caplog):
    logging.getLogger("gateway.test").info("场景：两个分片各自健康 -> FULL")
    clk = FakeClock()
    ups = [
        MockUpstream("o-a", "orders", Behavior(data={"o": 1}), clock=clk),
        MockUpstream("i-a", "inventory", Behavior(data={"i": 2}), clock=clk),
    ]
    gw = make_gateway(ups, clk)
    res = await drive(gw, clk, rid="full")
    assert res.completeness is Completeness.FULL
    assert res.data == {"orders": {"o": 1}, "inventory": {"i": 2}}
    assert res.failure_reason is None


async def test_upstream_error_is_classified_and_not_leaked(caplog):
    logging.getLogger("gateway.test").info(
        "场景：inventory 恒定报错；orders 健康 -> DEGRADED；底层异常不得出现在响应中"
    )
    clk = FakeClock()
    ups = [
        MockUpstream("o-a", "orders", Behavior(data={"o": 1}), clock=clk),
        MockUpstream("i-a", "inventory",
                     Behavior(ok=False, error="connection reset by peer"), clock=clk),
    ]
    gw = make_gateway(
        ups, clk,
        retry=RetryConfig(max_attempts_per_shard=2, jitter_ratio=0.0,
                          budget_capacity=5.0),
    )
    res = await drive(gw, clk, rid="degrade-err")
    assert res.completeness is Completeness.DEGRADED
    assert res.data == {"orders": {"o": 1}}
    bad = next(o for o in res.shard_outcomes if o.shard == "inventory")
    assert bad.ok is False
    assert bad.failure_kind is FailureKind.UPSTREAM_ERROR
    assert bad.attempts == 2
    # 判定依据：对外结构里只有分类枚举，不含原始异常文本
    serialized = repr(res)
    assert "connection reset" not in serialized


async def test_timeout_fast_fails_slow_upstream(caplog):
    logging.getLogger("gateway.test").info(
        "场景：一个上游延迟 5s，调用超时 0.5s -> 归类 timeout，慢调用被取消"
    )
    clk = FakeClock()
    slow = MockUpstream("s-a", "slow", Behavior(delay=5.0, data={"v": 1}), clock=clk)
    gw = make_gateway(
        [slow], clk,
        call_timeout_seconds=0.5,
        retry=RetryConfig(max_attempts_per_shard=1),
    )
    res = await drive(gw, clk, rid="timeout", tick=0.05)
    assert res.completeness is Completeness.UNAVAILABLE
    assert res.failure_reason is FailureKind.TIMEOUT
    assert slow.in_flight == 0  # 慢调用已被取消回收


async def test_retry_succeeds_on_other_replica(caplog):
    logging.getLogger("gateway.test").info(
        "场景：orders-a 持续报错，orders-b 健康 -> 重试在另一副本成功（不放大故障）"
    )
    clk = FakeClock()
    bad = MockUpstream("o-a", "orders", Behavior(ok=False, error="boom"), clock=clk)
    good = MockUpstream("o-b", "orders", Behavior(data={"o": 9}), clock=clk)
    gw = make_gateway(
        [bad, good], clk,
        retry=RetryConfig(max_attempts_per_shard=3, base_backoff_seconds=0.05,
                          jitter_ratio=0.0, budget_capacity=10.0),
    )
    res = await drive(gw, clk, rid="retry-other", tick=0.01)
    assert res.completeness is Completeness.FULL
    outcome = res.shard_outcomes[0]
    assert outcome.replica == "o-b"
    assert outcome.attempts == 2
    assert [r.replica for r in outcome.attempt_records] == ["o-a", "o-b"]
    # 坏副本只被打到 1 次（熔断尚未跳闸但选择器分流到健康副本）
    assert bad.call_count == 1


async def test_circuit_opens_and_skips_to_healthy_replica(caplog):
    logging.getLogger("gateway.test").info(
        "场景：连续多次请求中坏副本 o-a 累积失败触发熔断，之后请求直接走 o-b"
    )
    clk = FakeClock()
    bad = MockUpstream("o-a", "orders", Behavior(ok=False, error="boom"), clock=clk)
    good = MockUpstream("o-b", "orders", Behavior(data={"o": 1}), clock=clk)
    cfg = GatewayConfig(
        breaker=BreakerConfig(failure_threshold=2, cooldown_seconds=10.0),
        retry=RetryConfig(max_attempts_per_shard=1),
    )
    from gateway.orchestrator import GatewayOrchestrator
    gw = GatewayOrchestrator([bad, good], cfg, clock=clk)

    # 轮询分流：连续 4 个请求，两个副本各承担 2 次，o-a 因此累计 2 次失败而跳闸
    for i in range(4):
        await drive(gw, clk, rid=f"c{i}", tick=0.01)
    assert bad.call_count == 2
    assert gw.breakers["o-a"].state.value == "open"

    bad_calls_before = bad.call_count
    good_calls_before = good.call_count
    for i in range(3):
        res = await drive(gw, clk, rid=f"after-{i}", tick=0.01)
        # 判定依据：熔断后不再调用坏副本，且仍然 FULL（好副本承载）
        assert res.completeness is Completeness.FULL
        assert res.shard_outcomes[0].replica == "o-b"
    assert bad.call_count == bad_calls_before
    assert good.call_count == good_calls_before + 3


async def test_all_unavailable_reports_classified_reason(caplog):
    logging.getLogger("gateway.test").info("场景：唯一分片持续报错 -> UNAVAILABLE + upstream_error")
    clk = FakeClock()
    up = MockUpstream("x-a", "x", Behavior(ok=False, error="boom"), clock=clk)
    gw = make_gateway(
        [up], clk,
        breaker=BreakerConfig(failure_threshold=99),
        retry=RetryConfig(max_attempts_per_shard=2, budget_capacity=10.0),
    )
    res = await drive(gw, clk, rid="all-down", tick=0.01)
    assert res.completeness is Completeness.UNAVAILABLE
    assert res.failure_reason is FailureKind.UPSTREAM_ERROR


async def test_no_healthy_upstream_when_circuit_open(caplog):
    logging.getLogger("gateway.test").info("场景：唯一副本熔断 OPEN -> 快速失败 no_healthy_upstream")
    clk = FakeClock()
    up = MockUpstream("x-a", "x", Behavior(ok=False, error="boom"), clock=clk)
    from gateway.orchestrator import GatewayOrchestrator
    gw = GatewayOrchestrator(
        [up],
        GatewayConfig(
            breaker=BreakerConfig(failure_threshold=1, cooldown_seconds=100.0),
            retry=RetryConfig(max_attempts_per_shard=1),
        ),
        clock=clk,
    )
    r1 = await drive(gw, clk, rid="trip", tick=0.01)
    assert r1.failure_reason is FailureKind.UPSTREAM_ERROR
    r2 = await drive(gw, clk, rid="open-fastfail", tick=0.01)
    # 判定依据：熔断 OPEN 且无副本 -> 分类为 no_healthy_upstream，且未再触达上游
    assert r2.failure_reason is FailureKind.NO_HEALTHY_UPSTREAM
    assert up.call_count == 1


async def test_rate_limit_fast_fails_and_classifies(caplog):
    logging.getLogger("gateway.test").info(
        "场景：唯一副本限流桶容量极小（1）且不补充 -> 第 2 次尝试被限流，分类 rate_limited"
    )
    clk = FakeClock()
    up = MockUpstream("x-a", "x", Behavior(data={"v": 1}), clock=clk)
    from gateway.orchestrator import GatewayOrchestrator
    gw = GatewayOrchestrator(
        [up],
        GatewayConfig(
            rate_limit=RateLimitConfig(capacity=1.0, refill_per_second=0.0),
            retry=RetryConfig(max_attempts_per_shard=1),
        ),
        clock=clk,
    )
    r1 = await drive(gw, clk, rid="rl-1", tick=0.01)
    assert r1.completeness is Completeness.FULL
    r2 = await drive(gw, clk, rid="rl-2", tick=0.01)
    assert r2.completeness is Completeness.UNAVAILABLE
    assert r2.failure_reason is FailureKind.RATE_LIMITED
    # 判定依据：被限流未触达上游（调用数未增加），在途名额也已释放
    assert up.call_count == 1 and up.in_flight == 0
