"""熔断场景：连续失败跳闸、冷却后半开探测、探测结果决定恢复/重开。"""

from __future__ import annotations

import logging

import pytest

from gateway.breaker import CircuitBreaker
from gateway.config import BreakerConfig
from gateway.models import BreakerState


class ManualClock:
    def __init__(self, t=1000.0):
        self.t = t

    def now(self):
        return self.t


@pytest.fixture()
def scenario(caplog):
    caplog.set_level(logging.DEBUG, logger="gateway")
    clk = ManualClock()
    cb = CircuitBreaker(
        "payments-a",
        BreakerConfig(failure_threshold=3, cooldown_seconds=5.0,
                      half_open_probe_max=1, half_open_success_threshold=1),
        clock=clk,
    )
    logging.getLogger("gateway.test").info(
        "场景：阈值3次失败 -> OPEN；冷却5s -> HALF_OPEN 单探测；探测成功恢复/失败重开"
    )
    return clk, cb


def test_opens_after_consecutive_failures_and_fast_fails(scenario):
    clk, cb = scenario
    assert cb.allow() == (True, False)
    cb.record_failure(False)
    cb.record_failure(False)
    assert cb.state is BreakerState.CLOSED  # 未达阈值

    cb.record_failure(False)
    assert cb.state is BreakerState.OPEN
    # 判定依据：OPEN 期间非探测流量必须快速失败，不触达上游
    assert cb.allow() == (False, False)
    assert cb.trip_count == 1


def test_success_resets_consecutive_counter(scenario):
    _, cb = scenario
    cb.record_failure(False)
    cb.record_success(False)
    cb.record_failure(False)
    cb.record_failure(False)
    # 判定依据：中间一次成功清零了连续计数，2 次失败不跳闸
    assert cb.state is BreakerState.CLOSED


def test_cooldown_then_half_open_probe_then_recover(scenario):
    clk, cb = scenario
    for _ in range(3):
        cb.record_failure(False)
    assert cb.state is BreakerState.OPEN

    clk.t += 4.9
    assert cb.state is BreakerState.OPEN  # 冷却未到
    clk.t += 0.1
    assert cb.state is BreakerState.HALF_OPEN

    # 判定依据：只放行 1 个探测，其余流量仍快速失败
    assert cb.allow() == (True, True)
    assert cb.allow() == (False, False)

    cb.record_success(True)
    assert cb.state is BreakerState.CLOSED
    assert cb.consecutive_failures == 0


def test_failed_probe_reopens_with_fresh_cooldown(scenario):
    clk, cb = scenario
    for _ in range(3):
        cb.record_failure(False)
    clk.t += 5.0
    assert cb.allow() == (True, True)
    cb.record_failure(True)  # 探测失败
    assert cb.state is BreakerState.OPEN
    assert cb.trip_count == 2
    # 判定依据：新冷却重新计时
    assert cb.peek() == (False, False)
    clk.t += 4.9
    assert cb.state is BreakerState.OPEN
    clk.t += 0.1
    assert cb.state is BreakerState.HALF_OPEN
