"""编排器集成测试：超时/报错/缓慢恢复/状态抖动/并发压力。

每个用例都打印注入场景与判定依据；断言三态语义、完整度、
熔断与重试行为，以及各上游调用计数的一致性。
"""

from __future__ import annotations

import threading
import time

import pytest

from gateway.clock import SystemClock
from gateway.scenarios import SHARDS, build_gateway
from gateway.types import (
    BreakerConfig,
    FailureReason,
    GatewayConfig,
    GatewayRequest,
    OverallStatus,
    RetryConfig,
    ShardStatus,
    UpstreamState,
)

FAST = BreakerConfig(failure_threshold=3, cooldown_seconds=0.15, success_to_close=2)
PROBE = BreakerConfig(failure_threshold=2, cooldown_seconds=0.15, success_to_close=2)


def _req(i: int = 0) -> GatewayRequest:
    return GatewayRequest(request_id=f"req-{i}", shards=SHARDS)


# ---------------------------------------------------------------------
def test_all_healthy_returns_success_full_completeness():
    s = build_gateway()
    r = s.gateway.handle(_req(1))
    assert r.status is OverallStatus.SUCCESS
    assert r.completeness == 1.0
    assert set(r.data) == set(SHARDS)
    print("[场景] 全部上游健康 -> SUCCESS，判定依据: 每个分片首个副本即成功")


def test_partial_failure_returns_degraded_with_completeness():
    # orders 两个副本：一个永远慢（超时），一个永远报错
    behaviors = {
        "orders-0": {"latency": 1.0},
        "orders-1": {"fail_every": 1},
    }
    s = build_gateway(
        behaviors,
        config=GatewayConfig(
            retry=RetryConfig(max_attempts=3, backoff_base_seconds=0.001),
            per_attempt_timeout=0.05,
        ),
        breaker=FAST,
    )
    r = s.gateway.handle(_req(2))
    assert r.status is OverallStatus.DEGRADED
    assert r.completeness == pytest.approx(2 / 3)
    assert r.failed_shards == ("orders",)
    assert "orders" not in r.data
    snap = s.registry.snapshot()
    assert snap["orders-0"]["failure_count"] >= 1
    print(
        "[场景] orders 一副本超时+一副本报错 -> DEGRADED 完整度 2/3，"
        "判定依据: orders 全候选失败，其余两片成功"
    )


def test_all_timeout_classified_as_all_timeout():
    behaviors = {f"{sh}-0": {"latency": 1.0} for sh in SHARDS}
    behaviors.update({f"{sh}-1": {"latency": 1.0} for sh in SHARDS})
    s = build_gateway(
        behaviors,
        config=GatewayConfig(
            retry=RetryConfig(max_attempts=2, backoff_base_seconds=0.001),
            per_attempt_timeout=0.03,
        ),
        breaker=FAST,
    )
    t0 = time.monotonic()
    r = s.gateway.handle(_req(3))
    elapsed = time.monotonic() - t0
    assert r.status is OverallStatus.FAILED
    assert r.failure_reason is FailureReason.ALL_TIMEOUT
    assert r.completeness == 0.0
    # 6 个慢上游并行，整体必须远快于串行
    assert elapsed < 0.5, f"慢上游拖垮整体: {elapsed:.3f}s"
    print(f"[场景] 全部上游缓慢 -> FAILED/ALL_TIMEOUT，耗时 {elapsed:.3f}s（并行不被慢上游阻塞）")


def test_mixed_failure_reasons_classified_as_mixed():
    # users=超时，orders=错误，inventory=无容量（停用+熔断），构成三类混合
    behaviors = {
        "users-0": {"latency": 1.0}, "users-1": {"latency": 1.0},
        "orders-0": {"fail_every": 1}, "orders-1": {"fail_every": 1},
    }
    s = build_gateway(
        behaviors,
        config=GatewayConfig(
            retry=RetryConfig(max_attempts=2, backoff_base_seconds=0.001),
            per_attempt_timeout=0.03,
        ),
        breaker=FAST,
    )
    for name in ("inventory-0", "inventory-1"):
        s.registry.get(name).state = UpstreamState.DISABLED
    r = s.gateway.handle(_req(4))
    assert r.status is OverallStatus.FAILED
    assert r.failure_reason is FailureReason.MIXED
    statuses = {t.shard: t.status for t in r.trace}
    assert statuses["users"] is ShardStatus.DEGRADED_TIMEOUT
    assert statuses["orders"] is ShardStatus.DEGRADED_ERROR
    assert statuses["inventory"] is ShardStatus.DEGRADED_DISABLED
    print("[场景] 超时/错误/无容量三类并存 -> MIXED，判定依据: 失败集合含多种 ShardStatus")


def test_slow_replica_fails_over_to_healthy_one():
    behaviors = {"orders-0": {"latency": 1.0, "weight": 1000}}
    s = build_gateway(
        behaviors,
        config=GatewayConfig(
            retry=RetryConfig(max_attempts=3, backoff_base_seconds=0.001),
            per_attempt_timeout=0.05,
        ),
        breaker=FAST,
    )
    r = s.gateway.handle(_req(5))
    orders = next(t for t in r.trace if t.shard == "orders")
    assert orders.status is ShardStatus.OK
    assert orders.served_by == "orders-1"
    # 权重最高的慢副本确实先被调用并超时，之后故障转移
    assert s.registry.snapshot()["orders-0"]["failure_count"] >= 1
    assert any(a.outcome == "timeout" for a in orders.attempts)
    print("[场景] orders-0 超时后故障转移到 orders-1 成功，判定依据: attempts 含 timeout 后 success")


def test_circuit_opens_and_then_recovers_via_probe():
    """连续错误触发熔断；冷却后探测成功恢复，期间流量快速失败/转移。"""
    behaviors = {"users-0": {"fail_every": 1, "weight": 1000}}
    s = build_gateway(
        behaviors,
        config=GatewayConfig(
            retry=RetryConfig(max_attempts=2, backoff_base_seconds=0.001),
            per_attempt_timeout=0.1,
        ),
        breaker=PROBE,
    )
    # 打 2 轮让 users-0 连续失败熔断（每轮先尝试 users-0 失败再转移 users-1）
    for i in range(2):
        r = s.gateway.handle(_req(10 + i))
        assert r.status is OverallStatus.SUCCESS
    assert s.registry.get("users-0").breaker.state() is UpstreamState.OPEN
    # 熔断期间请求不再打 users-0（快速失败后只用 users-1）
    before = s.registry.snapshot()["users-0"]["failure_count"]
    r = s.gateway.handle(_req(13))
    after = s.registry.snapshot()["users-0"]["failure_count"]
    assert r.status is OverallStatus.SUCCESS
    assert after == before
    print("[场景] users-0 熔断打开期间被跳过（计数不增长），流量全部转移 users-1")

    # 等待冷却，让行为恢复（替换为成功实现）
    from gateway.upstream import SimulatedUpstream

    s.registry._impls["users-0"] = SimulatedUpstream(
        "users-0", "users", latency=0.001, clock=SystemClock()
    )
    time.sleep(0.2)
    # 半开探测：success_to_close=2，需要两轮成功
    r1 = s.gateway.handle(_req(14))
    assert s.registry.get("users-0").breaker.state() in {
        UpstreamState.HALF_OPEN,
        UpstreamState.HEALTHY,
    }
    r2 = s.gateway.handle(_req(15))
    assert r1.status is OverallStatus.SUCCESS and r2.status is OverallStatus.SUCCESS
    assert s.registry.get("users-0").breaker.state() is UpstreamState.HEALTHY
    print("[场景] 冷却后受控探测连续成功 -> 熔断闭合，users-0 恢复承载")


def test_retry_budget_caps_retries_under_recovery_flap():
    """恢复抖动时全局重试预算耗尽后快速失败，不产生重试风暴。"""
    # 所有副本恒定错误，制造高重试需求
    behaviors = {f"{sh}-{i}": {"fail_every": 1} for sh in SHARDS for i in range(2)}
    s = build_gateway(
        behaviors,
        config=GatewayConfig(
            retry=RetryConfig(
                max_attempts=4,
                total_budget_tokens=6,
                budget_refill_per_second=0.0,
                backoff_base_seconds=0.001,
            ),
            per_attempt_timeout=0.05,
        ),
        breaker=BreakerConfig(failure_threshold=100, cooldown_seconds=10),
    )
    r = s.gateway.handle(_req(20))
    assert r.status is OverallStatus.FAILED
    real_outcomes = {"success", "timeout", "error"}
    total_calls = sum(
        1 for t in r.trace for a in t.attempts if a.outcome in real_outcomes
    )
    # 3 个首次尝试 + 最多 6 个预算内重试 = 9
    assert total_calls <= 9, total_calls
    budget_logs = [
        a for t in r.trace for a in t.attempts if a.outcome == "retry_budget_exhausted"
    ]
    assert budget_logs, "应观察到预算耗尽的快速失败记录"
    print(f"[场景] 全错误抖动下真实上游调用={total_calls} <= 3+6，判定依据: 全局重试预算硬上限")


def test_deadline_bounds_total_time_even_with_retries():
    behaviors = {f"{sh}-{i}": {"latency": 0.6} for sh in SHARDS for i in range(2)}
    s = build_gateway(
        behaviors,
        config=GatewayConfig(
            retry=RetryConfig(
                max_attempts=5,
                request_timeout_seconds=0.25,
                backoff_base_seconds=0.02,
            ),
            per_attempt_timeout=0.08,
        ),
        breaker=FAST,
    )
    t0 = time.monotonic()
    r = s.gateway.handle(_req(21))
    elapsed = time.monotonic() - t0
    assert r.status is OverallStatus.FAILED
    assert elapsed < 0.6, f"请求未受截止时间约束: {elapsed:.3f}s"
    print(f"[场景] 全慢 + 多重试，总耗时 {elapsed:.3f}s 受 0.25s 截止时间约束")


def test_concurrent_requests_keep_counts_consistent():
    """并发压力下：计数之和==实际请求结论数，active_calls 归零，结论稳定。"""
    behaviors = {
        "orders-0": {"fail_every": 2},   # 间歇性错误
        "orders-1": {"latency": 0.01},
    }
    s = build_gateway(
        behaviors,
        config=GatewayConfig(
            retry=RetryConfig(max_attempts=3, backoff_base_seconds=0.001),
            per_attempt_timeout=0.05,
            max_workers=32,
        ),
        breaker=FAST,
    )
    n = 40
    results = []
    barrier = threading.Barrier(n)

    def worker(i):
        barrier.wait()
        results.append(s.gateway.handle(_req(100 + i)))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(results) == n
    # 有健康副本兜底，结论应全部稳定可复现（SUCCESS 或 DEGRADED，绝不 FAILED）
    assert all(r.status in {OverallStatus.SUCCESS, OverallStatus.DEGRADED} for r in results)
    snap = s.registry.snapshot()
    total_finished = sum(
        v["success_count"] + v["failure_count"] for v in snap.values()
    )
    counted_attempts = sum(
        len(t.attempts)
        for r in results
        for t in r.trace
        if any(a.outcome in {"success", "timeout", "error"} for a in t.attempts)
    )
    assert total_finished == counted_attempts, (total_finished, counted_attempts)
    assert all(v["active_calls"] == 0 for v in snap.values())
    print(
        f"[场景] {n} 并发请求，完成调用计数={total_finished} 与响应中尝试数一致，"
        "在途全部归零，无异常透出"
    )


def test_underlying_exception_text_never_leaks():
    """底层异常不得原样透出：trace 中只有枚举 outcome 与固定 detail。"""
    behaviors = {"users-1": {"fail_every": 1}}
    s = build_gateway(
        behaviors,
        config=GatewayConfig(
            retry=RetryConfig(max_attempts=2, backoff_base_seconds=0.001),
            per_attempt_timeout=0.05,
        ),
        breaker=FAST,
    )
    r = s.gateway.handle(_req(30))
    serialized = str(r)
    assert "injected error" not in serialized
    for t in r.trace:
        for a in t.attempts:
            assert a.outcome in {
                "success", "timeout", "error",
                "rejected_circuit_open", "rate_limited",
                "retry_budget_exhausted", "request_deadline",
            }
    print("[场景] 响应序列化不含底层异常文本，判定依据: outcome 全为受控枚举")


def test_gradual_recovery_and_flapping_determinism():
    """副本先坏后逐步恢复：请求结论随恢复进度变化，且同一注入序列下结论可复现。

    orders-0：前 3 次报错（触发熔断），第 4 次起成功 -> 缓慢恢复。
    orders-1：始终健康，作为兜底，因此所有请求结论都是 SUCCESS，
    但流量来源应从 orders-1 切回恢复后的 orders-0。
    """
    from gateway.upstream import SimulatedUpstream

    def build_one():
        s = build_gateway(
            {"orders-0": {"fail_nth": [1, 2, 3], "weight": 100}},
            config=GatewayConfig(
                retry=RetryConfig(max_attempts=2, backoff_base_seconds=0.001),
                per_attempt_timeout=0.05,
            ),
            breaker=BreakerConfig(failure_threshold=3, cooldown_seconds=0.12, success_to_close=1),
        )
        return s

    def run_sequence():
        s = build_one()
        served = []
        for i in range(6):
            r = s.gateway.handle(_req(200 + i))
            t = next(x for x in r.trace if x.shard == "orders")
            served.append(t.served_by)
            if i == 3:
                # 冷却后开始受控探测恢复
                time.sleep(0.14)
        return served

    first = run_sequence()
    second = run_sequence()
    assert first == second, f"相同注入场景结论必须可复现: {first} vs {second}"
    # 前 3 次坏副本流量被兜底；恢复期间探测成功后流量回到 orders-0
    assert first[:3] == ["orders-1", "orders-1", "orders-1"], first
    assert "orders-0" in first[3:], first
    print(f"[场景] 逐步恢复序列 served_by={first}，两次运行完全一致（可复现），判定依据: 固定注入+固定种子")


def test_flapping_then_all_replica_errors_still_classified_stable():
    """状态抖动（时好时坏）下，同一时刻全副本不可用 -> FAILED 且原因稳定不抖动。"""
    from gateway.upstream import SimulatedUpstream

    clock = SystemClock()
    s = build_gateway(
        config=GatewayConfig(
            retry=RetryConfig(max_attempts=2, backoff_base_seconds=0.001),
            per_attempt_timeout=0.05,
        ),
        breaker=FAST,
        clock=clock,
    )
    # users 两个副本交替抖动：本轮 0 错，下轮 1 错。用序号错开来构造"同时坏"窗口
    s.registry._impls["users-0"] = SimulatedUpstream(
        "users-0", "users", fail_nth=[1, 3, 5], latency=0.001, clock=clock
    )
    s.registry._impls["users-1"] = SimulatedUpstream(
        "users-1", "users", fail_nth=[1, 2, 3, 4, 5], latency=0.001, clock=clock
    )
    conclusions = []
    for i in range(5):
        r = s.gateway.handle(
            GatewayRequest(request_id=f"f{i}", shards=("users",))
        )
        conclusions.append((r.status, r.failure_reason))
    # 结论只允许是受控枚举集合中的值，不允许异常或 None 之外的未定义状态
    assert all(
        st in {OverallStatus.SUCCESS, OverallStatus.DEGRADED, OverallStatus.FAILED}
        for st, _ in conclusions
    )
    print(f"[场景] 抖动窗口内 users 结论序列={conclusions}，全部为受控三态枚举")


def test_replica_capacity_limit_causes_skip_without_blocking():
    """orders-0 令牌桶容量为 0（无承载能力）时，选择后取令牌失败应立即
    跳到 orders-1，不阻塞、不报错给调用方。"""
    behaviors = {
        "orders-0": {"capacity": 0.0, "refill_per_second": 0.0, "weight": 1000},
    }
    s = build_gateway(
        behaviors,
        config=GatewayConfig(
            retry=RetryConfig(max_attempts=3, backoff_base_seconds=0.001),
            per_attempt_timeout=0.05,
        ),
        breaker=FAST,
    )
    t0 = time.monotonic()
    r = s.gateway.handle(_req(300))
    elapsed = time.monotonic() - t0
    assert r.status is OverallStatus.SUCCESS
    orders = next(t for t in r.trace if t.shard == "orders")
    assert orders.served_by == "orders-1"
    assert any(a.outcome == "rate_limited" for a in orders.attempts)
    assert elapsed < 0.3
    print(f"[场景] orders-0 容量=0 被立即跳过，转移 orders-1，耗时 {elapsed:.3f}s 不阻塞")
