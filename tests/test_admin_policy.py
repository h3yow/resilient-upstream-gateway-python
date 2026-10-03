"""运行期策略调整测试：不重启改权重/承载/停用启用。

覆盖：
- 停用/启用与在途请求并发：在途请求按进入时的判断跑完并照常计数，
  新请求立即按新策略走；
- 承载能力调小：在途调用不受影响，新请求立即受新容量约束；
- 并发施压 + 策略反复翻转：成功/失败/拒绝/在途计数最终一致，
  响应 trace 里的判定能和计数对上。
"""

from __future__ import annotations

import threading
import time

from gateway.scenarios import build_gateway
from gateway.types import (
    BreakerConfig,
    GatewayConfig,
    GatewayRequest,
    OverallStatus,
    RetryConfig,
)

FAST = BreakerConfig(failure_threshold=3, cooldown_seconds=0.15, success_to_close=2)
CONFIG = GatewayConfig(
    retry=RetryConfig(max_attempts=2, backoff_base_seconds=0.001),
    per_attempt_timeout=0.5,
)


def _req(i: int, shards=("users",)) -> GatewayRequest:
    return GatewayRequest(request_id=f"req-{i}", shards=shards)


def test_disable_mid_flight_inflight_completes_new_requests_avoid():
    """停用发生在请求执行中途：在途请求正常跑完，新请求立刻绕开。"""
    s = build_gateway(
        {"users-0": {"latency": 0.2}, "users-1": {"latency": 0.001}},
        config=CONFIG,
        breaker=FAST,
    )
    s.registry.update_policy("users-0", weight=10_000)  # 确保首个请求选中 users-0
    s.registry.update_policy("users-1", weight=1)

    inflight = {}
    t = threading.Thread(target=lambda: inflight.update(r=s.gateway.handle(_req(1))))
    t.start()
    time.sleep(0.05)  # 请求已在 users-0 上执行
    assert s.registry.snapshot()["users-0"]["active_calls"] == 1

    s.registry.disable("users-0")  # 改策略：立即停用
    r2 = s.gateway.handle(_req(2))  # 新请求必须绕开 users-0
    t.join()

    r1 = inflight["r"]
    assert r1.status is OverallStatus.SUCCESS
    assert r1.trace[0].served_by == "users-0"  # 在途请求按进入时的判断跑完
    assert r2.status is OverallStatus.SUCCESS
    assert r2.trace[0].served_by == "users-1"  # 新请求立即按新策略走

    snap = s.registry.snapshot()
    assert snap["users-0"]["success_count"] == 1  # 在途那次照常计数，不重复不丢失
    assert snap["users-0"]["active_calls"] == 0
    assert snap["users-1"]["success_count"] == 1
    print(
        "[场景] users-0 在途调用中被停用 -> 在途跑完计 1 次成功，"
        "新请求全部走 users-1，判定依据: served_by 与 success_count 对齐"
    )

    s.registry.enable("users-0")
    r3 = s.gateway.handle(_req(3))
    assert r3.trace[0].served_by == "users-0"  # 重新启用后立即参与服务
    print("[场景] users-0 重新启用 -> 立即恢复承载，判定依据: served_by=users-0")


def test_capacity_shrink_mid_flight_applies_to_new_requests_only():
    """承载调小：在途调用跑完，新请求立即受新容量约束并故障转移。"""
    s = build_gateway(
        {
            "users-0": {"latency": 0.2, "capacity": 4, "refill_per_second": 0},
            "users-1": {"latency": 0.2},
        },
        config=CONFIG,
        breaker=FAST,
    )
    s.registry.update_policy("users-0", weight=10_000)

    # 两个在途请求分别占住 users-0 / users-1（选择器按在途数分流）
    inflight: dict[int, object] = {}
    threads = [
        threading.Thread(target=lambda i=i: inflight.update({i: s.gateway.handle(_req(i))}))
        for i in (1, 2)
    ]
    for t in threads:
        t.start()
        time.sleep(0.03)
    time.sleep(0.03)
    assert s.registry.snapshot()["users-0"]["active_calls"] == 1

    # 运行期把 users-0 容量从 4 直接调到 0（排空）：在途那 1 个不受影响，
    # 但令牌余量被截断到 0，新请求已拿不到令牌。
    s.registry.update_policy("users-0", capacity=0, refill_per_second=0)
    r3 = s.gateway.handle(_req(3))
    for t in threads:
        t.join()

    assert inflight[1].trace[0].served_by == "users-0"  # 在途按进入时的判断跑完
    assert r3.status is OverallStatus.SUCCESS
    assert r3.trace[0].served_by == "users-1"  # 立即故障转移
    outcomes = [a.outcome for a in r3.trace[0].attempts]
    assert "rate_limited" in outcomes  # 判定依据可在 trace 中直接看到

    snap = s.registry.snapshot()
    assert snap["users-0"]["success_count"] == 1
    assert snap["users-0"]["active_calls"] == 0
    print(
        "[场景] users-0 容量 4->0 且有 1 个在途 -> 在途跑完，"
        "新请求 rate_limited 后转移 users-1，判定依据: trace.outcome=rate_limited"
    )


def test_weight_update_takes_effect_immediately_and_snapshot_matches():
    s = build_gateway(config=CONFIG, breaker=FAST)
    s.registry.update_policy("users-0", weight=1)
    s.registry.update_policy("users-1", weight=10_000)
    r = s.gateway.handle(_req(1))
    assert r.trace[0].served_by == "users-1"  # 权重大者立即优先
    snap = s.registry.snapshot()
    assert snap["users-1"]["weight"] == 10_000
    assert snap["users-1"]["success_count"] == 1
    print("[场景] users-1 权重调大 -> 新请求立即选中，判定依据: snapshot.weight 与 served_by")


def test_concurrent_load_with_policy_flapping_counters_stay_consistent():
    """并发施压 + 反复停用/启用/改权重：最终计数与 trace 判定一一对应。"""
    s = build_gateway(
        {"users-0": {"latency": 0.002}, "users-1": {"latency": 0.002}},
        config=CONFIG,
        breaker=FAST,
    )
    stop = threading.Event()
    traces: list = []
    lock = threading.Lock()

    def client(i: int):
        while not stop.is_set():
            r = s.gateway.handle(_req(i, shards=("users",)))
            with lock:
                traces.append(r)

    def flapper():
        for _ in range(30):
            s.registry.disable("users-0")
            s.registry.update_policy("users-0", weight=1)
            s.registry.enable("users-0")
            s.registry.update_policy("users-0", weight=5)

    clients = [threading.Thread(target=client, args=(i,)) for i in range(4)]
    for t in clients:
        t.start()
    flapper()
    time.sleep(0.2)
    stop.set()
    for t in clients:
        t.join()

    # 汇总所有响应 trace 里每个副本的真实调用结论
    counted = {"users-0": 0, "users-1": 0}
    for r in traces:
        assert r.status is OverallStatus.SUCCESS  # 双副本至少一个可用，整体不受策略翻转影响
        for a in r.trace[0].attempts:
            if a.outcome in ("success", "timeout", "error") and a.upstream in counted:
                counted[a.upstream] += 1

    snap = s.registry.snapshot()
    for name in counted:
        assert snap[name]["success_count"] + snap[name]["failure_count"] == counted[name], (
            f"{name} 计数与 trace 判定不一致: "
            f"snapshot={snap[name]['success_count']}+{snap[name]['failure_count']} trace={counted[name]}"
        )
        assert snap[name]["active_calls"] == 0
    print(
        f"[场景] 4 线程并发 + 30 次停用/启用/改权重 -> 全部 SUCCESS，"
        f"trace 判定数={counted} 与 snapshot 计数一致，在途归零"
    )
