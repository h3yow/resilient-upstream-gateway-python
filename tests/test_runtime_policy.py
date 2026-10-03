"""运行期策略调整测试：不重启改权重/并发承载/停用启用。

覆盖：
- 改策略时还有请求在跑 -> 在途请求按进入时的判断跑完，计数不乱；
- 变更后的新请求立即按新策略走；
- 成功/失败/拒绝/在途计数与响应中的判定信息一致。
"""

from __future__ import annotations

import threading
import time

from gateway.scenarios import SHARDS, build_gateway
from gateway.types import (
    BreakerConfig,
    GatewayConfig,
    GatewayRequest,
    OverallStatus,
    RetryConfig,
)

FAST = BreakerConfig(failure_threshold=3, cooldown_seconds=0.15, success_to_close=2)
CONF = GatewayConfig(
    retry=RetryConfig(max_attempts=2, backoff_base_seconds=0.001),
    per_attempt_timeout=0.5,
)


def _req(i: int = 0) -> GatewayRequest:
    return GatewayRequest(request_id=f"pol-{i}", shards=SHARDS)


def _served_by(resp, shard: str) -> str | None:
    return next(t for t in resp.trace if t.shard == shard).served_by


def test_weight_change_takes_effect_for_new_requests():
    """运行期把 orders-0 权重调为 0：新请求立即全部流向 orders-1。"""
    s = build_gateway(
        {"orders-0": {"weight": 1000}}, config=CONF, breaker=FAST
    )
    r1 = s.gateway.handle(_req(1))
    assert _served_by(r1, "orders") == "orders-0"  # 高权重先被选中

    s.registry.update_policy("orders-0", weight=0)
    assert s.registry.snapshot()["orders-0"]["policy"]["weight"] == 0

    for i in range(2, 6):
        r = s.gateway.handle(_req(i))
        assert r.status is OverallStatus.SUCCESS
        assert _served_by(r, "orders") == "orders-1"
    snap = s.registry.snapshot()
    # 调权后 orders-0 不再收到新调用；orders-1 承担了后续 4 次
    assert snap["orders-0"]["success_count"] == 1
    assert snap["orders-1"]["success_count"] == 4
    print("[场景] 运行期权重 1000->0，新请求全部转移 orders-1，"
          "判定依据: served_by 与 success_count 一一对应")


def test_capacity_shrink_and_restore_without_restart():
    """并发承载调到 0 -> 副本被跳过（rate_limited）；调回后立即恢复承载。"""
    s = build_gateway(
        {"orders-0": {"weight": 1000}}, config=CONF, breaker=FAST
    )
    assert _served_by(s.gateway.handle(_req(1)), "orders") == "orders-0"

    s.registry.update_policy("orders-0", capacity=0.0, refill_per_second=0.0)
    r = s.gateway.handle(_req(2))
    assert r.status is OverallStatus.SUCCESS
    assert _served_by(r, "orders") == "orders-1"
    orders = next(t for t in r.trace if t.shard == "orders")
    assert any(a.outcome == "rate_limited" and a.upstream == "orders-0"
               for a in orders.attempts)

    s.registry.update_policy("orders-0", capacity=10.0, refill_per_second=10.0)
    r = s.gateway.handle(_req(3))
    assert _served_by(r, "orders") == "orders-0"
    snap = s.registry.snapshot()
    assert snap["orders-0"]["policy"]["capacity"] == 10.0
    # orders-0 成功 2 次（调容量前后各一），orders-1 成功 1 次（收紧期间）
    assert snap["orders-0"]["success_count"] == 2
    assert snap["orders-1"]["success_count"] == 1
    print("[场景] 承载 无限->0->10，期间 rate_limited 转移、恢复后回流，"
          "判定依据: attempts 含 rate_limited 且计数与轨迹一致")


def test_disable_mid_flight_inflight_completes_normally():
    """停用发生在请求执行途中：在途请求正常跑完并计数，新请求立即绕开。"""
    s = build_gateway(
        {"orders-0": {"weight": 1000, "latency": 0.15}},
        config=CONF, breaker=FAST,
    )
    inflight: list = []

    def fire():
        inflight.append(s.gateway.handle(_req(10)))

    t = threading.Thread(target=fire)
    t.start()
    time.sleep(0.05)  # 确认请求已进入 orders-0 的在途调用
    assert s.registry.snapshot()["orders-0"]["active_calls"] == 1

    s.registry.disable("orders-0")  # 改到一半：请求还在跑
    t.join()

    r = inflight[0]
    assert r.status is OverallStatus.SUCCESS
    assert _served_by(r, "orders") == "orders-0"  # 在途请求按进入时的判断跑完
    snap = s.registry.snapshot()
    assert snap["orders-0"]["success_count"] == 1   # 不多算也不少算
    assert snap["orders-0"]["active_calls"] == 0    # 在途归零
    assert snap["orders-0"]["admin_state"].value == "disabled"

    # 变更后的新请求立即按新策略走：绕开 orders-0
    r2 = s.gateway.handle(_req(11))
    assert r2.status is OverallStatus.SUCCESS
    assert _served_by(r2, "orders") == "orders-1"
    snap2 = s.registry.snapshot()
    assert snap2["orders-0"]["success_count"] == 1  # 停用期间没有新调用打进去
    assert snap2["orders-0"]["rejection_count"] >= 1  # 被拒绝次数有记录

    # 重新启用后立即回到选择池
    s.registry.enable("orders-0")
    r3 = s.gateway.handle(_req(12))
    assert _served_by(r3, "orders") == "orders-0"
    assert s.registry.snapshot()["orders-0"]["success_count"] == 2
    print("[场景] 在途请求进行中停用 orders-0：在途正常完成计 1 次成功，"
          "新请求转移 orders-1 且 rejection_count 有记录，重新启用后回流")


def test_policy_change_under_concurrent_load_counts_stay_consistent():
    """并发施压中途改策略：所有响应三态稳定，计数与轨迹严格对账。"""
    s = build_gateway(
        {"orders-0": {"weight": 1000, "latency": 0.01}},
        config=GatewayConfig(
            retry=RetryConfig(max_attempts=2, backoff_base_seconds=0.001),
            per_attempt_timeout=0.5,
            max_workers=32,
        ),
        breaker=FAST,
    )
    n = 30
    results: list = []
    barrier = threading.Barrier(n)

    def worker(i: int):
        barrier.wait()
        results.append(s.gateway.handle(_req(100 + i)))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    # 施压中途把 orders-0 停用再调权重，模拟运维并发操作
    time.sleep(0.005)
    s.registry.disable("orders-0")
    s.registry.update_policy("orders-1", weight=500)
    for t in threads:
        t.join()

    assert len(results) == n
    assert all(r.status in {OverallStatus.SUCCESS, OverallStatus.DEGRADED}
               for r in results)
    snap = s.registry.snapshot()
    # 对账：成功+失败计数之和 == 响应轨迹中真实调用次数
    real = {"success", "timeout", "error"}
    counted = sum(
        1 for r in results for t in r.trace for a in t.attempts if a.outcome in real
    )
    total = sum(v["success_count"] + v["failure_count"] for v in snap.values())
    assert total == counted, (total, counted)
    assert all(v["active_calls"] == 0 for v in snap.values())
    print(f"[场景] {n} 并发中途停用+调权，真实调用计数={total} 与轨迹完全对账，"
          "在途全部归零，判定依据: 锁内计数 + 变更只影响新请求")
