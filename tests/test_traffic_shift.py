"""灰度流量接管测试：平滑加码、进度可观测、异常自动撤回。

场景：
- 新副本按阶梯加码直到完全替下旧副本，每一步份额与生效权重可查询；
- 加码途中新副本开始报错/变慢 -> 自动撤回，流量退回旧副本，
  全程整体可用性不下降（所有请求仍 SUCCESS）。
"""

from __future__ import annotations

import time

from gateway.scenarios import build_gateway
from gateway.traffic import ShiftPhase, TrafficShiftPlan
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
    per_attempt_timeout=0.05,
)


def _req(i: int, shards=("users",)) -> GatewayRequest:
    return GatewayRequest(request_id=f"req-{i}", shards=shards)


def test_ramp_progresses_observably_until_source_fully_replaced():
    s = build_gateway(
        {"users-0": {"latency": 0.001}, "users-1": {"latency": 0.001}},
        config=CONFIG,
        breaker=FAST,
    )
    plan = TrafficShiftPlan(
        shard="users",
        source="users-0",
        target="users-1",
        start_share=0.2,
        step_share=0.2,
        step_seconds=0.03,
    )
    st = s.traffic.start_shift(plan)["users"]
    assert st["phase"] == "ramping"
    assert st["current_share"] == 0.2
    print(f"[场景] 启动灰度 users-0 -> users-1，初始份额={st['current_share']}")

    time.sleep(0.04)  # 越过 1 个阶梯
    mid = s.traffic.status("users")["users"]
    assert mid["phase"] == "ramping"
    assert 0.2 < mid["current_share"] < 1.0
    assert mid["effective_weights"]["users-1"] == mid["current_share"]
    print(f"[场景] 加码中：份额={mid['current_share']} 生效权重={mid['effective_weights']}")

    # 加码期间两个副本都在承载（target 已分到真实流量）
    served = set()
    for i in range(60):
        r = s.gateway.handle(_req(i))
        assert r.status is OverallStatus.SUCCESS
        served.add(r.trace[0].served_by)
    assert served == {"users-0", "users-1"}

    time.sleep(0.25)  # 越过全部阶梯
    done = s.traffic.status("users")["users"]
    assert done["phase"] == "complete"
    assert done["current_share"] == 1.0
    assert done["effective_weights"] == {"users-0": 0.0, "users-1": 1.0}

    for i in range(60, 70):
        r = s.gateway.handle(_req(i))
        assert r.trace[0].served_by == "users-1"  # 旧副本已排空
    print(
        f"[场景] 接管完成：份额=1.0，users-0 排空，全部流量到 users-1，"
        f"history={done['history']}"
    )


def test_ramp_rolls_back_when_target_errors_and_availability_holds():
    """加码途中 target 恒定报错 -> 自动撤回，流量退回 source，全程 SUCCESS。"""
    s = build_gateway(
        {"users-0": {"latency": 0.001}, "users-1": {"fail_every": 1}},
        config=CONFIG,
        breaker=FAST,
    )
    s.traffic.start_shift(
        TrafficShiftPlan(
            shard="users",
            source="users-0",
            target="users-1",
            start_share=0.5,
            step_share=0.1,
            step_seconds=0.03,
            rollback_failure_threshold=1,
        )
    )
    phases_seen = set()
    for i in range(60):
        r = s.gateway.handle(_req(i))
        assert r.status is OverallStatus.SUCCESS  # 整体可用性不被拖下水
        phases_seen.add(s.traffic.status("users")["users"]["phase"])
        if "rolled_back" in phases_seen:
            break
    final = s.traffic.status("users")["users"]
    assert final["phase"] == ShiftPhase.ROLLED_BACK.value
    assert final["effective_weights"] == {"users-0": 1.0, "users-1": 0.0}
    assert final["ramp_failures"] >= 1

    # 撤回后 target 不再承载，source 顶回全部流量
    for i in range(60, 70):
        r = s.gateway.handle(_req(i))
        assert r.status is OverallStatus.SUCCESS
        assert r.trace[0].served_by == "users-0"
    print(
        f"[场景] 加码途中 users-1 恒定报错 -> 自动撤回，"
        f"ramp_failures={final['ramp_failures']}，流量退回 users-0，"
        f"history={final['history']}"
    )


def test_ramp_rolls_back_when_target_becomes_slow():
    """加码途中 target 变慢（超时）-> 超时计入失败，触发撤回。"""
    s = build_gateway(
        {"users-0": {"latency": 0.001}, "users-1": {"latency": 0.5}},  # 远超单次超时
        config=CONFIG,
        breaker=FAST,
    )
    s.traffic.start_shift(
        TrafficShiftPlan(
            shard="users",
            source="users-0",
            target="users-1",
            start_share=0.5,
            step_share=0.1,
            step_seconds=0.03,
            rollback_failure_threshold=1,
        )
    )
    rolled = False
    for i in range(60):
        r = s.gateway.handle(_req(i))
        assert r.status is OverallStatus.SUCCESS
        if s.traffic.status("users")["users"]["phase"] == "rolled_back":
            rolled = True
            break
    assert rolled, "target 持续超时未触发撤回"
    final = s.traffic.status("users")["users"]
    assert final["effective_weights"]["users-1"] == 0.0
    print(
        f"[场景] 加码途中 users-1 变慢超时 -> 撤回，"
        f"ramp_failures={final['ramp_failures']}，history={final['history']}"
    )


def test_shift_rejects_mismatched_shard():
    s = build_gateway(config=CONFIG, breaker=FAST)
    try:
        s.traffic.start_shift(
            TrafficShiftPlan(shard="orders", source="users-0", target="users-1")
        )
    except ValueError:
        print("[场景] source/target 不属于目标分片 -> 拒绝启动，判定依据: ValueError")
    else:
        raise AssertionError("应拒绝跨分片的接管计划")
