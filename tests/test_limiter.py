"""限流器与全局重试预算测试。"""

from __future__ import annotations

import threading

from gateway.clock import SystemClock
from gateway.limiter import RateLimiter, RetryBudget
from gateway.types import RateLimiterConfig, RetryConfig


def test_bucket_rejects_after_capacity_and_refills():
    rl = RateLimiter(
        "u",
        RateLimiterConfig(capacity=3, refill_per_second=100),
        SystemClock(),
    )
    assert all(rl.try_acquire() for _ in range(3))
    assert rl.try_acquire() is False  # 超出承载能力，不阻塞
    SystemClock().sleep(0.03)        # 100/s => 约补充 3 个
    assert rl.try_acquire() is True
    print("[场景] 容量 3 取空后拒绝，等待补充后恢复，判定依据: 令牌桶水位")


def test_unlimited_bucket_never_rejects():
    rl = RateLimiter("u", RateLimiterConfig(), SystemClock())
    assert all(rl.try_acquire() for _ in range(10_000))


def test_retry_budget_global_cap_under_concurrency():
    """20 个线程同时各取 10 个重试令牌，预算桶只有 50：总成功数恰好 50。"""
    budget = RetryBudget(
        RetryConfig(total_budget_tokens=50, budget_refill_per_second=0.0),
        SystemClock(),
    )
    acquired = {"n": 0}
    lock = threading.Lock()
    barrier = threading.Barrier(20)

    def worker():
        barrier.wait()
        for _ in range(10):
            if budget.acquire():
                with lock:
                    acquired["n"] += 1

    threads = [threading.Thread(target=worker) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert acquired["n"] == 50
    print("[场景] 并发重试令牌争抢，判定依据: 全局预算=50，实际发放严格=50，无重试风暴")


def test_retry_budget_refills_over_time():
    budget = RetryBudget(
        RetryConfig(total_budget_tokens=10, budget_refill_per_second=100),
        SystemClock(),
    )
    assert all(budget.acquire() for _ in range(10))
    assert budget.acquire() is False
    SystemClock().sleep(0.02)
    assert budget.acquire() is True
