"""多副本同时故障、同时进入恢复观察的并发测试。

覆盖：
- 同一分片两个副本几乎同时熔断、冷却几乎同时到期：并发施压下
  每个副本都真的被探测到并回到正常服务，没有副本被落在
  HALF_OPEN/OPEN 里永远得不到探测；
- 半开探测并发度始终有上界（不因请求洪峰放开探测）；
- 探测名额不因"选中但未调用"（如限流拦截）而泄漏——
  副本恢复承载能力后仍能走完探测流程闭合熔断；
- 彻底恢复后，行为与从未故障时一致。
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
    UpstreamState,
)

# 两次失败即熔断，冷却短，半开只需 2 次成功、探测名额 1
PROBE = BreakerConfig(
    failure_threshold=2,
    cooldown_seconds=0.1,
    half_open_max_probe=1,
    success_to_close=2,
)
CONFIG = GatewayConfig(
    retry=RetryConfig(max_attempts=2, backoff_base_seconds=0.005),
    per_attempt_timeout=0.05,
)


def _req(i: int, shards=("users",)) -> GatewayRequest:
    return GatewayRequest(request_id=f"req-{i}", shards=shards)


def test_simultaneous_failure_and_recovery_under_concurrent_load():
    """两副本同时熔断 -> 并发施压 -> 都真的被探测并恢复，探测并发有界。"""
    s = build_gateway(
        {
            "users-0": {"fail_nth": (1, 2), "latency": 0.001},
            "users-1": {"fail_nth": (1, 2), "latency": 0.001},
        },
        config=CONFIG,
        breaker=PROBE,
    )
    # 让两个副本几乎同时熔断
    for i in range(2):
        s.gateway.handle(_req(i))
    snap = s.registry.snapshot()
    assert snap["users-0"]["state"] is UpstreamState.OPEN
    assert snap["users-1"]["state"] is UpstreamState.OPEN
    print("[场景] users-0/users-1 同时熔断 OPEN，冷却 0.1s 后并发施压")

    time.sleep(0.12)  # 冷却同时到期
    stop = threading.Event()
    results: list = []
    lock = threading.Lock()
    max_probes_seen = {"users-0": 0, "users-1": 0}

    def client(i: int):
        while not stop.is_set():
            r = s.gateway.handle(_req(1000 + i))
            with lock:
                results.append(r)

    def monitor():
        # 持续采样两个熔断器的在途探测数，验证上界
        while not stop.is_set():
            for name in max_probes_seen:
                b = s.registry.get(name).breaker
                with b._lock:  # 测试直接观测内部水位
                    max_probes_seen[name] = max(max_probes_seen[name], b._probes_in_flight)

    threads = [threading.Thread(target=client, args=(i,)) for i in range(6)]
    mon = threading.Thread(target=monitor)
    for t in threads:
        t.start()
    mon.start()
    time.sleep(0.6)  # 持续施压，覆盖整个恢复窗口
    stop.set()
    for t in threads:
        t.join()
    mon.join()

    snap = s.registry.snapshot()
    for name in ("users-0", "users-1"):
        assert snap[name]["state"] is UpstreamState.HEALTHY, (
            f"{name} 未恢复：{snap[name]['state']}"
        )
        # 每个副本都真的被探测到：故障后又有新的成功调用
        assert snap[name]["success_count"] >= 2, f"{name} 未被实际探测: {snap[name]}"
        assert max_probes_seen[name] <= 1, (
            f"{name} 探测并发越界: {max_probes_seen[name]}"
        )
    print(
        f"[场景] 并发恢复完成：两副本均 HEALTHY，"
        f"success={[snap[n]['success_count'] for n in ('users-0','users-1')]}，"
        f"探测并发峰值={max_probes_seen}（上界 1）"
    )

    # 彻底恢复后表现与从未故障一致：连续请求全部首次尝试即成功
    for i in range(20):
        r = s.gateway.handle(_req(2000 + i))
        assert r.status is OverallStatus.SUCCESS
        attempts = r.trace[0].attempts
        assert len(attempts) == 1 and attempts[0].outcome == "success"
    print("[场景] 恢复后 20 个请求全部首次尝试即成功，与从未故障一致")


def test_probe_slot_not_leaked_when_rate_limited_after_selection():
    """半开副本被限流拦截时探测名额不泄漏，恢复承载后仍能闭合。"""
    s = build_gateway(
        {
            "users-0": {"fail_nth": (1, 2), "latency": 0.001},
            "users-1": {"latency": 0.001},
        },
        config=CONFIG,
        breaker=PROBE,
    )
    s.registry.update_policy("users-0", weight=10_000)  # 确保前两次请求先打 users-0
    for i in range(2):
        s.gateway.handle(_req(i))
    assert s.registry.snapshot()["users-0"]["state"] is UpstreamState.OPEN

    # 冷却到期前把 users-0 的承载调到 0：半开后候选能列出它，
    # 但调用前被限流拦截——探测名额必须归还而不是泄漏。
    s.registry.update_policy("users-0", capacity=0, refill_per_second=0)
    time.sleep(0.12)
    for i in range(10):
        r = s.gateway.handle(_req(100 + i))
        assert r.status is OverallStatus.SUCCESS  # users-1 顶上
    rt = s.registry.get("users-0")
    with rt.breaker._lock:
        assert rt.breaker._probes_in_flight == 0, "探测名额被限流路径泄漏"
        assert rt.breaker._probes_tried == 0, "未真正调用却消耗了探测配额"
    print("[场景] 半开副本被限流拦截 10 次 -> 探测名额 0 泄漏，配额未消耗")

    # 恢复承载能力：副本必须能走完探测流程真正恢复
    s.registry.update_policy("users-0", capacity=100, refill_per_second=100)
    recovered = False
    for i in range(30):
        r = s.gateway.handle(_req(200 + i))
        assert r.status is OverallStatus.SUCCESS
        if s.registry.snapshot()["users-0"]["state"] is UpstreamState.HEALTHY:
            recovered = True
            break
        time.sleep(0.02)
    assert recovered, "users-0 永远停在不可用状态（探测名额泄漏）"
    snap = s.registry.snapshot()
    assert snap["users-0"]["success_count"] >= 2  # 探测真的打到了它
    print(
        f"[场景] users-0 恢复承载后被实际探测并闭合，"
        f"success_count={snap['users-0']['success_count']}"
    )


def test_all_replicas_open_still_recovers_each_one():
    """全部副本同时 OPEN 时，恢复观察不会因互相挤压而遗漏任何副本。"""
    s = build_gateway(
        {
            "users-0": {"fail_nth": (1, 2), "latency": 0.001},
            "users-1": {"fail_nth": (1, 2), "latency": 0.001},
        },
        config=CONFIG,
        breaker=PROBE,
    )
    for i in range(2):
        s.gateway.handle(_req(i))
    time.sleep(0.12)

    # 单线程顺序请求：每个副本都应在有限请求内完成 2 次探测并闭合
    recovered: set[str] = set()
    for i in range(40):
        s.gateway.handle(_req(300 + i))
        snap = s.registry.snapshot()
        for name in ("users-0", "users-1"):
            if snap[name]["state"] is UpstreamState.HEALTHY:
                recovered.add(name)
        if len(recovered) == 2:
            break
        time.sleep(0.02)
    assert recovered == {"users-0", "users-1"}, f"有副本未恢复: {recovered}"
    print(f"[场景] 全部副本同时 OPEN -> 有限请求内全部恢复: {sorted(recovered)}")
