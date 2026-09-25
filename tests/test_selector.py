"""选择器测试：熔断/停用/限流副本被跳过，负载按健康与在途数分发。"""

from __future__ import annotations

from gateway.clock import SystemClock
from gateway.selector import UpstreamSelector
from gateway.types import (
    BreakerConfig,
    RateLimiterConfig,
    UpstreamConfig,
    UpstreamState,
)
from gateway.upstream import UpstreamRegistry


def _registry_with_two_replicas(capacity: float = float("inf")):
    reg = UpstreamRegistry(SystemClock())
    for i in range(2):
        reg.register(
            UpstreamConfig(
                name=f"s-{i}",
                shard="s",
                weight=1,
                rate_limiter=RateLimiterConfig(capacity=capacity, refill_per_second=0.0),
            )
        )
    return reg


def test_both_healthy_become_candidates_in_deterministic_order():
    reg = _registry_with_two_replicas()
    sel_a = UpstreamSelector(reg, seed=7)
    sel_b = UpstreamSelector(reg, seed=7)
    first = [sel_a.candidates("s")[0] for _ in range(5)]
    second = [sel_b.candidates("s")[0] for _ in range(5)]
    assert first == second  # 相同种子 -> 同样序列，可复现
    print(f"[场景] 两副本均健康，固定种子序列={first}，判定依据: 相同随机种子")


def test_open_or_disabled_replica_is_skipped():
    reg = _registry_with_two_replicas()
    reg.get("s-0").breaker.force_state(UpstreamState.OPEN)
    sel = UpstreamSelector(reg, seed=1)
    assert sel.candidates("s") == ["s-1"]
    reg.get("s-0").breaker.force_state(UpstreamState.HEALTHY)
    reg.get("s-0").state = UpstreamState.DISABLED
    assert sel.candidates("s") == ["s-1"]
    print("[场景] OPEN/DISABLED 副本被跳过，判定依据: breaker.allow_request/admin_state")


def test_capacity_enforced_by_orchestrator_not_selector():
    """令牌承载能力在编排器真正调用时占用；选择器只负责健康/在途排序。"""
    reg = _registry_with_two_replicas(capacity=1.0)
    sel = UpstreamSelector(reg, seed=1)
    # 令牌桶为空与否不影响候选筛选（检查不消耗名额）
    assert set(sel.candidates("s")) == {"s-0", "s-1"}
    assert reg.get("s-0").limiter.try_acquire() is True
    assert reg.get("s-0").limiter.try_acquire() is False
    reg.get("s-0").limiter.refund(1.0)
    assert reg.get("s-0").limiter.try_acquire() is True
    print("[场景] 承载名额由编排器 acquire/refund，选择器筛选不消耗令牌")


def test_load_prefers_less_busy_replica():
    reg = _registry_with_two_replicas()
    sel = UpstreamSelector(reg, seed=1)
    sel.mark_started("s-0")
    sel.mark_started("s-0")
    names = [sel.candidates("s")[0] for _ in range(3)]
    assert all(n == "s-1" for n in names)  # 更空闲的副本总是优先
    print(f"[场景] s-0 在途=2，流量全部给 s-1，序列={names}，判定依据: active_calls 排序")
