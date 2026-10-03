"""并发恢复无死角测试：多副本同时故障、同时到恢复期、并发施压。

验证三件事：
1. 同一分片多个副本几乎同时熔断、又几乎同时到冷却点时，每个副本
   最终都真的被探测到并回到正常服务，没有副本被饿死；
2. 探测严格有上界（half_open_max_probe=1）：大量请求同时涌入
   不会把故障副本放开来探，每个副本每次恢复 episode 只准入 1 个探测；
3. 彻底恢复后表现与从没出过故障一致。
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
    UpstreamConfig,
    UpstreamState,
)
from gateway.upstream import SimulatedUpstream

FAST = BreakerConfig(
    failure_threshold=2, cooldown_seconds=0.15,
    half_open_max_probe=1, success_to_close=1,
)
CONF = GatewayConfig(
    retry=RetryConfig(max_attempts=3, backoff_base_seconds=0.001),
    per_attempt_timeout=0.05,
    max_workers=32,
)
REPLICAS = ("orders-0", "orders-1", "orders-2")


def _build_three_failing_replicas():
    """orders 分片 3 个副本全部恒定报错。"""
    s = build_gateway(
        {"orders-0": {"fail_every": 1}, "orders-1": {"fail_every": 1}},
        config=CONF, breaker=FAST,
    )
    s.registry.register(
        UpstreamConfig(name="orders-2", shard="orders", weight=1, breaker=FAST),
        SimulatedUpstream("orders-2", "orders", fail_every=1, latency=0.001),
    )
    return s


def _req(i: int) -> GatewayRequest:
    return GatewayRequest(request_id=f"rec-{i}", shards=("orders",))


def _heal_all(s):
    for name in REPLICAS:
        s.registry._impls[name] = SimulatedUpstream(name, "orders", latency=0.001)


def _flood(s, n: int, start: int = 0) -> list:
    results: list = []
    barrier = threading.Barrier(n)

    def worker(i: int):
        barrier.wait()
        results.append(s.gateway.handle(_req(start + i)))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return results


def test_simultaneous_recovery_no_starvation_and_bounded_probes():
    s = _build_three_failing_replicas()
    # 两轮请求让 3 个副本几乎同时熔断（每轮 3 次尝试正好各打一遍）
    for i in range(2):
        s.gateway.handle(_req(i))
    for name in REPLICAS:
        rt = s.registry.get(name)
        assert rt.breaker.state() is UpstreamState.OPEN
        assert rt.failure_count == 2
    print("[场景] orders 三副本同时熔断 OPEN，冷却 0.15s 后同时到恢复观察点")

    _heal_all(s)
    time.sleep(0.18)  # 三个副本同时冷却到期

    _flood(s, 24, start=100)  # 并发施压：24 个请求同时涌入

    # 1) 没有副本被饿死：三个副本都被真正探测并回到正常服务
    for name in REPLICAS:
        rt = s.registry.get(name)
        assert rt.breaker.state() is UpstreamState.HEALTHY, name
        # 2) 探测严格有上界：24 并发下每个副本只准入过 1 个探测
        assert rt.breaker.probes_admitted_total == 1, (name, rt.breaker.probes_admitted_total)
        assert rt.success_count >= 1
    snap = s.registry.snapshot()
    assert all(snap[n]["active_calls"] == 0 for n in REPLICAS)
    print(f"[判定] 24 并发后三副本全部 HEALTHY，各准入探测数="
          f"{[s.registry.get(n).breaker.probes_admitted_total for n in REPLICAS]}"
          "（上界=1），无饿死、无放大")

    # 3) 彻底恢复后与从未故障一致：继续施压全部成功
    before = {n: s.registry.get(n).success_count for n in REPLICAS}
    results2 = _flood(s, 12, start=200)
    assert all(r.status is OverallStatus.SUCCESS for r in results2)
    after = {n: s.registry.get(n).success_count for n in REPLICAS}
    assert sum(after[n] - before[n] for n in REPLICAS) == 12  # 每请求恰好 1 次成功调用
    print("[场景] 恢复后 12 并发全部 SUCCESS，每请求 1 次调用即成功，"
          "表现与从未故障一致")


def test_no_probe_admitted_before_cooldown_expires():
    """冷却未过时并发涌入：0 个探测被准入，故障副本不被打扰。"""
    s = _build_three_failing_replicas()
    for i in range(2):
        s.gateway.handle(_req(i))
    frozen = {n: s.registry.get(n).failure_count for n in REPLICAS}

    results = _flood(s, 16, start=300)  # 冷却期内施压
    for name in REPLICAS:
        rt = s.registry.get(name)
        assert rt.breaker.probes_admitted_total == 0
        assert rt.failure_count == frozen[name]  # 没有新调用打到故障副本
    assert all(r.status is OverallStatus.FAILED for r in results)
    print("[场景] 冷却期内 16 并发：0 探测准入、故障副本计数冻结、"
          "请求快速失败，判定依据: probes_admitted_total==0")


def test_flapping_replica_probed_again_each_episode_until_stable():
    """探测失败会重新熔断，但下一个冷却期仍会被再次探测，最终稳定恢复。"""
    s = build_gateway(
        {"orders-0": {"fail_every": 1, "weight": 1000}},
        config=GatewayConfig(
            retry=RetryConfig(max_attempts=2, backoff_base_seconds=0.001),
            per_attempt_timeout=0.05,
        ),
        breaker=FAST,
    )
    for i in range(2):
        s.gateway.handle(_req(i))
    rt = s.registry.get("orders-0")
    assert rt.breaker.state() is UpstreamState.OPEN

    # 第一个恢复 episode：副本仍然报错 -> 探测失败，立即重新熔断
    time.sleep(0.18)
    s.gateway.handle(_req(10))
    assert rt.breaker.state() is UpstreamState.OPEN
    assert rt.breaker.probes_admitted_total == 1
    print("[判定] episode-1 探测失败立即重熔断，探测数=1")

    # 第二个恢复 episode：副本已修复 -> 探测成功，彻底恢复
    s.registry._impls["orders-0"] = SimulatedUpstream("orders-0", "orders", latency=0.001)
    time.sleep(0.18)
    r = s.gateway.handle(_req(11))
    assert r.status is OverallStatus.SUCCESS
    assert rt.breaker.state() is UpstreamState.HEALTHY
    assert rt.breaker.probes_admitted_total == 2  # 每个 episode 恰好 1 个探测
    # 恢复后流量重新回到 orders-0（高权重），与从未故障一致
    r2 = s.gateway.handle(_req(12))
    served = next(t for t in r2.trace if t.shard == "orders").served_by
    assert served == "orders-0"
    print("[场景] 抖动副本两个恢复 episode 各准入 1 个探测，最终 HEALTHY "
          "并重新承载流量，判定依据: probes_admitted_total==2 且 state=HEALTHY")
