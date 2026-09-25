"""限流与重试预算：令牌补充、封顶、退避确定性、预算耗尽即停止重试。"""

from __future__ import annotations

import asyncio
import logging

import pytest

from gateway.config import RetryConfig
from gateway.ratelimit import TokenBucket
from gateway.retry import RetryPolicy


class ManualClock:
    def __init__(self, t=1000.0):
        self.t = t

    def now(self):
        return self.t


@pytest.fixture(autouse=True)
def _log(caplog):
    caplog.set_level(logging.DEBUG, logger="gateway")
    logging.getLogger("gateway.test").info(
        "场景：令牌桶容量/补充/封顶；重试退避指数增长与可复现抖动；预算耗尽快速停止"
    )


def test_bucket_capacity_and_refill():
    clk = ManualClock()
    bucket = TokenBucket("upstream-x", capacity=3.0, refill_per_second=2.0, clock=clk)
    assert all(bucket.try_acquire() for _ in range(3))
    assert not bucket.try_acquire()  # 桶空 -> 快速失败
    clk.t += 0.5
    assert bucket.try_acquire()     # 补 1
    assert not bucket.try_acquire()
    clk.t += 10.0
    # 判定依据：长时间补充后令牌仍被容量封顶
    assert sum(1 for _ in range(3) if bucket.try_acquire()) == 3
    assert not bucket.try_acquire()


def test_backoff_is_exponential_and_capped():
    clk = ManualClock()
    rp = RetryPolicy(
        RetryConfig(base_backoff_seconds=0.1, max_backoff_seconds=1.0, jitter_ratio=0.0),
        clock=clk,
    )
    assert [rp.backoff(i) for i in range(6)] == [0.1, 0.2, 0.4, 0.8, 1.0, 1.0]


def test_jitter_is_reproducible_with_seed():
    a = RetryPolicy(RetryConfig(jitter_ratio=1.0), clock=ManualClock(), seed=123)
    b = RetryPolicy(RetryConfig(jitter_ratio=1.0), clock=ManualClock(), seed=123)
    seq_a = [a.backoff(i) for i in range(8)]
    seq_b = [b.backoff(i) for i in range(8)]
    assert seq_a == seq_b  # 同种子 -> 抖动序列一致，故障场景可复现


def test_retry_budget_exhaustion_stops_retrying():
    clk = ManualClock()
    # 容量只有 2 个重试令牌，无补充
    rp = RetryPolicy(
        RetryConfig(budget_capacity=2.0, budget_refill_per_second=0.0),
        clock=clk,
    )

    async def body():
        results = [await rp.acquire_retry_permit() for _ in range(3)]
        return results

    assert asyncio.run(body()) == [True, True, False]
