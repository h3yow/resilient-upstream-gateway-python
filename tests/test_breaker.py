"""熔断器状态机测试：阈值、冷却探测、探测失败重熔断、并发一致性。"""

from __future__ import annotations

import threading

from gateway.breaker import CircuitBreaker
from gateway.clock import SystemClock
from gateway.types import BreakerConfig, UpstreamState


def _breaker(**kw) -> CircuitBreaker:
    cfg = BreakerConfig(
        failure_threshold=kw.get("failure_threshold", 3),
        cooldown_seconds=kw.get("cooldown_seconds", 0.15),
        half_open_max_probe=1,
        success_to_close=2,
    )
    return CircuitBreaker("u", cfg, SystemClock())


def test_closed_then_opens_after_threshold():
    b = _breaker()
    for _ in range(2):
        b.record_failure()
    assert b.state() is UpstreamState.HEALTHY
    assert b.allow_request() is True
    b.record_failure()
    assert b.state() is UpstreamState.OPEN
    print("[场景] 连续失败 3 次 -> 熔断打开，判定依据: consecutive_failures>=threshold")


def test_open_rejects_traffic_fast():
    b = _breaker()
    for _ in range(3):
        b.record_failure()
    assert b.allow_request() is False  # 非探测流量快速失败
    print("[场景] OPEN 期间请求被快速拒绝，判定依据: state=OPEN 且冷却未过")


def test_cooldown_allows_limited_probe_and_closes_on_success():
    b = _breaker()
    for _ in range(3):
        b.record_failure()
    assert b.state() is UpstreamState.OPEN
    # 冷却期内探测也不放行
    assert b.allow_request() is False
    SystemClock().sleep(0.18)
    # half_open_max_probe=1：只有一个探测名额
    assert b.allow_request() is True
    assert b.allow_request() is False
    assert b.state() is UpstreamState.HALF_OPEN
    b.record_success()
    # 还差一次成功，且上一探测已结束，可再放一个探测
    assert b.allow_request() is True
    b.record_success()
    assert b.state() is UpstreamState.HEALTHY
    print("[场景] 冷却后受控探测成功 2 次 -> 闭合，判定依据: successes>=success_to_close")


def test_probe_failure_reopens_and_restarts_cooldown():
    b = _breaker()
    for _ in range(3):
        b.record_failure()
    SystemClock().sleep(0.18)
    assert b.allow_request() is True
    b.record_failure()
    assert b.state() is UpstreamState.OPEN
    assert b.allow_request() is False  # 冷却被重置，立即探测不允许
    print("[场景] 探测失败立即重新熔断并重置冷却，防止抖动恢复放大流量")


def test_concurrent_failures_open_exactly_once():
    """并发下状态迁移结论一致：最终 OPEN，且没有线程看到非法状态。"""
    b = _breaker(failure_threshold=10)
    seen = set()
    barrier = threading.Barrier(10)

    def worker():
        barrier.wait()
        for _ in range(2):
            b.record_failure()
            seen.add(b.state())

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert b.state() is UpstreamState.OPEN
    assert seen <= {UpstreamState.HEALTHY, UpstreamState.OPEN}
    print(f"[场景] 20 个并发失败调用 -> 最终状态唯一 OPEN，中途观测集合={seen}")
