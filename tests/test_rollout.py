"""灰度接管测试：平滑加码、加码途中自动撤回、进度可观测。

场景都围绕 orders 分片：orders-0 是在役旧副本，orders-1 是新上线副本。
判定依据打印在日志里：档位、流量比例、撤回原因、每请求 served_by。
"""

from __future__ import annotations

import time

from gateway.rollout import COMPLETED, ROLLED_BACK, TrafficShiftManager
from gateway.scenarios import build_gateway
from gateway.types import (
    BreakerConfig,
    GatewayConfig,
    GatewayRequest,
    OverallStatus,
    RetryConfig,
)

CONF = GatewayConfig(
    retry=RetryConfig(max_attempts=3, backoff_base_seconds=0.001),
    per_attempt_timeout=0.1,
)
FAST = BreakerConfig(failure_threshold=2, cooldown_seconds=0.5, success_to_close=1)


def _req(i: int) -> GatewayRequest:
    return GatewayRequest(request_id=f"shift-{i}", shards=("orders",))


def _served(resp) -> str | None:
    return next(t for t in resp.trace if t.shard == "orders").served_by


def test_gradual_takeover_with_observable_progress():
    """新副本按节奏加码直到完全替下旧副本；每步流量比例可直接观测。"""
    s = build_gateway(config=CONF, breaker=FAST)
    mgr = TrafficShiftManager(s.registry)
    s.gateway._shift_manager = mgr

    shift_id = mgr.begin_shift(
        "orders", "orders-1", ["orders-0"], steps=4, step_interval_seconds=0.05
    )
    st = mgr.status()[shift_id]
    assert st["state"] == "ramping" and st["current_step"] == 1
    assert st["traffic_share"]["orders-1"] == 0.25  # 起步只分 1/4
    print(f"[场景] 接管开始 step=1/4 流量比例={st['traffic_share']}")

    seen_steps = [1]
    for i in range(1, 7):
        time.sleep(0.055)  # 每轮跨过一档
        r = s.gateway.handle(_req(i))
        assert r.status is OverallStatus.SUCCESS  # 加码全程可用性不掉
        st = mgr.status()[shift_id]
        seen_steps.append(st["current_step"])
        print(f"[判定] 第{i}轮 step={st['current_step']}/4 "
              f"share={st['traffic_share']} served_by={_served(resp=r)}")

    st = mgr.status()[shift_id]
    assert st["state"] == COMPLETED and st["current_step"] == 4
    assert st["traffic_share"]["orders-1"] == 1.0
    assert st["traffic_share"]["orders-0"] == 0.0
    assert seen_steps == sorted(seen_steps) and seen_steps[-1] == 4

    # 完全接管后：旧副本比例 0 不再参与选择，新副本承载全部流量
    for i in range(7, 10):
        r = s.gateway.handle(_req(i))
        assert _served(r) == "orders-1"
    snap = s.registry.snapshot()
    assert snap["orders-0"]["policy"]["traffic_scale"] == 0.0
    assert snap["orders-1"]["policy"]["traffic_scale"] == 1.0
    print("[场景] 接管完成：orders-0 流量比例 0 退出选择，orders-1 承载全部，"
          "判定依据: traffic_share 与 served_by 一致")


def test_ramp_rolls_back_automatically_when_new_replica_errors():
    """加码途中新副本开始报错 -> 自动撤回，流量退回旧副本，可用性不掉。"""
    # 新副本更高权重（模拟更强力的新机器），但恒定报错
    s = build_gateway(
        {"orders-1": {"fail_every": 1, "weight": 1000}},
        config=CONF, breaker=FAST,
    )
    mgr = TrafficShiftManager(s.registry)
    s.gateway._shift_manager = mgr

    shift_id = mgr.begin_shift(
        "orders", "orders-1", ["orders-0"], steps=10, step_interval_seconds=0.05
    )
    rolled_back_at = None
    statuses = []
    for i in range(12):
        r = s.gateway.handle(_req(100 + i))
        statuses.append(r.status)
        st = mgr.status()[shift_id]
        if st["state"] == ROLLED_BACK and rolled_back_at is None:
            rolled_back_at = i
            print(f"[判定] 第{i}轮检测到撤回：原因={st['rollback_reason']} "
                  f"share={st['traffic_share']}")
        time.sleep(0.02)

    # 全程可用性不被拖下水：旧副本始终兜底，没有 FAILED
    assert all(st_ is OverallStatus.SUCCESS for st_ in statuses)
    assert rolled_back_at is not None, "新副本报错应触发自动撤回"

    st = mgr.status()[shift_id]
    assert st["state"] == ROLLED_BACK
    assert "circuit open" in st["rollback_reason"]
    assert st["traffic_share"]["orders-1"] == 0.0
    assert st["traffic_share"]["orders-0"] == 1.0

    # 撤回后流量完全回到旧副本
    for i in range(200, 203):
        r = s.gateway.handle(_req(i))
        assert r.status is OverallStatus.SUCCESS
        assert _served(r) == "orders-0"
    snap = s.registry.snapshot()
    assert snap["orders-1"]["policy"]["traffic_scale"] == 0.0
    assert snap["orders-0"]["policy"]["traffic_scale"] == 1.0
    # 新副本的失败被如实计数，与响应轨迹对得上
    new_failures = snap["orders-1"]["failure_count"]
    assert new_failures >= FAST.failure_threshold
    print(f"[场景] 加码途中 orders-1 报错 -> 自动撤回，orders-1 失败计数="
          f"{new_failures}，撤回后 12 轮请求全部 SUCCESS 且由 orders-0 承载，"
          "判定依据: rollback_reason + traffic_share + served_by")


def test_shift_rejects_unknown_replica():
    """非法目标直接报错，不产生半截状态。"""
    s = build_gateway(config=CONF, breaker=FAST)
    mgr = TrafficShiftManager(s.registry)
    try:
        mgr.begin_shift("orders", "orders-9", ["orders-0"])
        raised = False
    except KeyError:
        raised = True
    assert raised
    assert mgr.status() == {}
    print("[场景] 对不存在的副本发起接管 -> KeyError，无半截接管记录")
