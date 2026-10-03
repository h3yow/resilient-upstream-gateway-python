"""可复现的故障注入场景工厂。

默认拓扑：3 个分片（users/orders/inventory），每个分片 2 个副本，
命名为 ``<shard>-0`` / ``<shard>-1``。通过 ``behaviors`` 表脚本化
每个副本的延迟、抖动、按序号错误/超时，所有行为只发生在本进程内。

behaviors 示例::

    {
        "orders-0": {"latency": 0.5},            # 稳定慢 -> 触发单次超时
        "orders-1": {"fail_every": 1},           # 恒定报错
        "inventory-0": {"timeout_nth": [1, 2]},  # 前两次超时，之后恢复
    }
"""

from __future__ import annotations

from dataclasses import dataclass

from gateway.clock import Clock
from gateway.gateway import Gateway
from gateway.types import (
    BreakerConfig,
    GatewayConfig,
    RateLimiterConfig,
    UpstreamConfig,
)
from gateway.upstream import SimulatedUpstream, UpstreamRegistry

SHARDS: tuple[str, ...] = ("users", "orders", "inventory")
REPLICAS_PER_SHARD = 2


def build_registry(
    behaviors: dict[str, dict] | None = None,
    *,
    clock: Clock | None = None,
    breaker: BreakerConfig | None = None,
) -> UpstreamRegistry:
    """按行为表构造注册表，键为上游名（如 ``orders-0``）。"""
    behaviors = behaviors or {}
    registry = UpstreamRegistry(clock)
    for shard in SHARDS:
        for i in range(REPLICAS_PER_SHARD):
            name = f"{shard}-{i}"
            b = behaviors.get(name, {})
            cfg = UpstreamConfig(
                name=name,
                shard=shard,
                weight=int(b.get("weight", 1)),
                breaker=breaker or BreakerConfig(),
                rate_limiter=RateLimiterConfig(
                    capacity=float(b.get("capacity", float("inf"))),
                    refill_per_second=float(b.get("refill_per_second", float("inf"))),
                ),
            )
            impl = SimulatedUpstream(
                name,
                shard,
                latency=float(b.get("latency", 0.001)),
                jitter=float(b.get("jitter", 0.0)),
                fail_nth=tuple(b.get("fail_nth", ())),
                timeout_nth=tuple(b.get("timeout_nth", ())),
                fail_every=int(b.get("fail_every", 0)),
                seed=int(b.get("seed", 1000 + i)),
                clock=clock,
            )
            registry.register(cfg, impl)
    return registry


@dataclass
class BuiltScenario:
    gateway: Gateway
    registry: UpstreamRegistry
    config: GatewayConfig

    @property
    def traffic(self):
        return self.gateway.traffic


def build_gateway(
    behaviors: dict[str, dict] | None = None,
    *,
    clock: Clock | None = None,
    config: GatewayConfig | None = None,
    breaker: BreakerConfig | None = None,
) -> BuiltScenario:
    config = config or GatewayConfig()
    registry = build_registry(behaviors, clock=clock, breaker=breaker)
    gateway = Gateway(registry, config, clock=clock)
    return BuiltScenario(gateway=gateway, registry=registry, config=config)
