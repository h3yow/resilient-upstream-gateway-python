"""本地可控的模拟上游与上游注册表。

模拟上游支持脚本化故障注入：固定延迟、按调用序号触发超时/错误、
带种子的随机抖动。所有行为均发生在本进程内，不依赖任何外部服务。

注册表（``UpstreamRegistry``）统一维护每个上游的运行时状态与计数：
成功/失败/拒绝次数、在途调用数，全部在该上游自己的锁内更新，
保证并发压力下计数准确、可断言。
"""

from __future__ import annotations

import itertools
import threading
from dataclasses import dataclass, field
from typing import Callable, Protocol

from gateway.breaker import CircuitBreaker
from gateway.clock import Clock, SystemClock
from gateway.errors import UpstreamCallError, UpstreamTimeout
from gateway.limiter import RateLimiter
from gateway.types import UpstreamConfig, UpstreamState


class Upstream(Protocol):
    """上游协议：网关只依赖该接口。"""

    name: str
    shard: str

    def call(self, request_id: str, timeout: float) -> object:
        """在 ``timeout`` 秒内返回数据，否则抛 ``UpstreamTimeout``。"""
        ...


@dataclass
class UpstreamRuntime:
    """一个上游的全部可变运行时状态。"""

    config: UpstreamConfig
    breaker: CircuitBreaker
    limiter: RateLimiter
    state: UpstreamState = UpstreamState.HEALTHY
    active_calls: int = 0
    success_count: int = 0
    failure_count: int = 0
    rejection_count: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)


class SimulatedUpstream:
    """脚本化的本地模拟上游。

    behavior 参数：
    - ``latency``：基础延迟秒数；
    - ``jitter``：在基础延迟上叠加 [0, jitter) 的均匀抖动；
    - ``fail_nth`` / ``timeout_nth``：按 1 起始的调用序号触发；
    - ``fail_every``：每 N 次调用失败一次（模拟持续错误率）；
    - ``data_factory``：自定义返回数据。
    """

    def __init__(
        self,
        name: str,
        shard: str,
        *,
        latency: float = 0.0,
        jitter: float = 0.0,
        fail_nth: tuple[int, ...] = (),
        timeout_nth: tuple[int, ...] = (),
        fail_every: int = 0,
        data_factory: Callable[[int], object] | None = None,
        seed: int | None = None,
        clock: Clock | None = None,
    ) -> None:
        self.name = name
        self.shard = shard
        self._latency = latency
        self._jitter = jitter
        self._fail_nth = set(fail_nth)
        self._timeout_nth = set(timeout_nth)
        self._fail_every = fail_every
        self._data_factory = data_factory or (lambda n: {"shard": shard, "source": name, "seq": n})
        self._clock = clock or SystemClock()
        self._rng = __import__("random").Random(seed if seed is not None else hash(name) & 0xFFFF)
        self._seq_iter = itertools.count(1)
        self._seq_lock = threading.Lock()

    def call(self, request_id: str, timeout: float) -> object:
        with self._seq_lock:
            seq = next(self._seq_iter)
        delay = self._latency + (self._rng.random() * self._jitter if self._jitter else 0.0)
        if seq in self._timeout_nth or delay > timeout:
            # 仍然消耗时间，但不超过调用方给的观察窗口
            self._clock.sleep(min(delay, timeout) if timeout > 0 else delay)
            raise UpstreamTimeout(f"{self.name}#{seq}")
        self._clock.sleep(delay)
        if seq in self._fail_nth or (self._fail_every and seq % self._fail_every == 0):
            raise UpstreamCallError(f"{self.name}#{seq} injected error")
        return self._data_factory(seq)


class UpstreamRegistry:
    """所有上游的运行时状态中心。"""

    def __init__(self, clock: Clock | None = None) -> None:
        self._clock = clock or SystemClock()
        self._runtimes: dict[str, UpstreamRuntime] = {}
        self._impls: dict[str, Upstream] = {}
        self._by_shard: dict[str, list[str]] = {}
        self._lock = threading.Lock()

    def register(
        self,
        config: UpstreamConfig,
        impl: Upstream | None = None,
    ) -> SimulatedUpstream:
        impl = impl or SimulatedUpstream(
            config.name, config.shard, clock=self._clock, latency=0.001
        )
        rt = UpstreamRuntime(
            config=config,
            breaker=CircuitBreaker(config.name, config.breaker, self._clock),
            limiter=RateLimiter(config.name, config.rate_limiter, self._clock),
        )
        with self._lock:
            self._runtimes[config.name] = rt
            self._impls[config.name] = impl
            self._by_shard.setdefault(config.shard, []).append(config.name)
        return impl  # type: ignore[return-value]

    def get(self, name: str) -> UpstreamRuntime:
        return self._runtimes[name]

    def impl(self, name: str) -> Upstream:
        return self._impls[name]

    def replicas_for_shard(self, shard: str) -> list[UpstreamRuntime]:
        with self._lock:
            names = list(self._by_shard.get(shard, ()))
        return [self._runtimes[n] for n in sorted(names)]

    def shards(self) -> list[str]:
        with self._lock:
            return sorted(self._by_shard)

    def snapshot(self) -> dict[str, dict[str, object]]:
        """导出所有上游的计数与状态快照（供测试断言一致性）。"""
        out: dict[str, dict[str, object]] = {}
        with self._lock:
            names = sorted(self._runtimes)
        for name in names:
            rt = self._runtimes[name]
            with rt.lock:
                out[name] = {
                    "shard": rt.config.shard,
                    "state": rt.breaker.state(),
                    "admin_state": rt.state,
                    "active_calls": rt.active_calls,
                    "success_count": rt.success_count,
                    "failure_count": rt.failure_count,
                    "rejection_count": rt.rejection_count,
                }
        return out
