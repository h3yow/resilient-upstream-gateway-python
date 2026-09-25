"""上游选择：按熔断状态过滤、按最少在途负载分流、平局确定性、探测名额不泄漏。"""

from __future__ import annotations

import logging

import pytest

from gateway.breaker import CircuitBreaker
from gateway.config import BreakerConfig
from gateway.selector import UpstreamSelector
from gateway.upstream import Behavior, MockUpstream


class ManualClock:
    def __init__(self, t=1000.0):
        self.t = t

    def now(self):
        return self.t


def _build(caplog):
    caplog.set_level(logging.DEBUG, logger="gateway")
    logging.getLogger("gateway.test").info(
        "场景：3 个同分片副本，调整在途数与熔断状态，验证分流与探测优先级"
    )
    clk = ManualClock()
    ups = [MockUpstream(f"u{i}", "shard-s", Behavior(), clock=clk) for i in (1, 2, 3)]
    breakers = {
        up.name: CircuitBreaker(up.name, BreakerConfig(), clock=clk) for up in ups
    }
    return clk, ups, breakers, UpstreamSelector(ups, breakers)


def test_tie_picks_lexicographically_first(caplog):
    _, ups, _, sel = _build(caplog)
    # 判定依据：所有副本在途数都为 0 时，结果必须稳定（名字最小者）
    assert sel.select() == (ups[0], False)


def test_picks_least_in_flight(caplog):
    _, ups, _, sel = _build(caplog)
    ups[0]._in_flight = 2
    ups[1]._in_flight = 3
    # u3 在途 0 -> 被选中
    assert sel.select() == (ups[2], False)


def test_open_replicas_are_skipped(caplog):
    _, ups, breakers, sel = _build(caplog)
    ups[0]._in_flight = 2
    ups[1]._in_flight = 3
    for _ in range(3):
        breakers["u1"].record_failure(False)
    # u1 OPEN，u2 在途 3，u3 在途 0
    up, probe = sel.select()
    assert up is ups[2] and probe is False


def test_normal_replicas_preferred_over_probe_and_no_slot_leak(caplog):
    clk, ups, breakers, sel = _build(caplog)
    for _ in range(3):
        breakers["u1"].record_failure(False)
    clk.t += 5.0  # u1 进入 HALF_OPEN
    # 判定依据：普通副本 u2/u3 可用时不消耗 u1 的探测名额
    up, probe = sel.select()
    assert up is ups[1] and probe is False
    assert breakers["u1"].snapshot()["probes_in_flight"] == 0


def test_only_probe_when_all_others_open(caplog):
    clk, ups, breakers, sel = _build(caplog)
    for _ in range(3):
        breakers["u1"].record_failure(False)
    clk.t += 5.0  # u1 HALF_OPEN
    for _ in range(3):
        breakers["u2"].record_failure(False)
    for _ in range(3):
        breakers["u3"].record_failure(False)

    up, probe = sel.select()
    assert up is ups[0] and probe is True
    # 判定依据：探测名额被占满后，后续流量快速失败
    assert sel.select() == (None, False)


def test_no_healthy_upstream(caplog):
    _, ups, breakers, sel = _build(caplog)
    for name in ("u1", "u2", "u3"):
        for _ in range(3):
            breakers[name].record_failure(False)
    assert sel.select() == (None, False)
