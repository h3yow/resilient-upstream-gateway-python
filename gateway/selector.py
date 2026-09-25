"""上游选择与负载分发。

对某个分片，按以下规则给出候选副本名称序列：

1. 管理面未停用（admin state != DISABLED）；
2. 熔断器允许通过（CLOSED 或拿到半开探测名额）；
   —— OPEN 冷却未过的副本直接跳过，实现"非探测流量快速失败"。
3. 在合格副本中按 (健康优先级, 在途调用数, 加权随机) 排序：
   流量优先流向更健康、更空闲、权重更高的副本。
   随机项使用固定种子，相同输入下顺序可复现。

承载能力（令牌桶）不在筛选时消费令牌，而由编排器在真正发起
调用前 ``try_acquire``、调用结束后 ``refund``，避免"检查即消耗"
在重试路径上浪费其他副本的名额。``active_calls`` 实时反映在途压力。
"""

from __future__ import annotations

import random

from gateway.types import UpstreamState
from gateway.upstream import UpstreamRegistry

_HEALTH_PRIORITY = {
    UpstreamState.HALF_OPEN: 0,  # 受控探测优先路由（名额由熔断器严格限制）
    UpstreamState.HEALTHY: 1,
    UpstreamState.DEGRADED: 2,
    UpstreamState.OPEN: 3,
    UpstreamState.DISABLED: 4,
}


class UpstreamSelector:
    def __init__(self, registry: UpstreamRegistry, seed: int | None = 7) -> None:
        self._registry = registry
        self._rng = random.Random(seed)

    def candidates(self, shard: str) -> list[str]:
        runtimes = self._registry.replicas_for_shard(shard)
        eligible: list[tuple[int, int, float, str]] = []
        for rt in runtimes:
            with rt.lock:
                if rt.state is UpstreamState.DISABLED:
                    rt.rejection_count += 1
                    continue
                if not rt.breaker.allow_request():
                    rt.rejection_count += 1
                    continue
                state = rt.breaker.state()
                priority = _HEALTH_PRIORITY.get(state, 9)
                active = rt.active_calls
                weight = max(1, rt.config.weight)
            tie = self._rng.random() / weight
            eligible.append((priority, active, tie, rt.config.name))
        eligible.sort(key=lambda x: (x[0], x[1], x[2]))
        return [name for _, _, _, name in eligible]

    def mark_started(self, name: str) -> None:
        rt = self._registry.get(name)
        with rt.lock:
            rt.active_calls += 1

    def mark_finished(self, name: str) -> None:
        rt = self._registry.get(name)
        with rt.lock:
            rt.active_calls = max(0, rt.active_calls - 1)
