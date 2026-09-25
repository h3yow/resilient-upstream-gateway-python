"""编排核心：上游选择 + 熔断/限流/带预算重试 + 降级聚合。

执行模型
--------
上游按 shard 分组，每个 shard 有若干副本。一次请求对所有 shard 做有界并发
扇出；shard 内部在副本间做“选择 -> 调用 -> 熔断反馈 -> 带预算重试”。

结论语义（对同一故障输入、不同并发时序保持一致）
------------------------------------------------
- 全部分片成功 -> FULL；部分成功 -> DEGRADED（成功数据照常返回）；
- 全部分片失败 -> UNAVAILABLE，failure_reason 取确定性优先级：
  NO_HEALTHY_UPSTREAM > DEADLINE_EXCEEDED > RATE_LIMITED >
  RETRY_BUDGET_EXHAUSTED > TIMEOUT > UPSTREAM_ERROR > CIRCUIT_OPEN。
  （即“越偏系统性、越偏无恢复手段”的原因越优先暴露。）
- 底层异常（UpstreamError / asyncio.TimeoutError 等）绝不原样透出，
  一律归一化为 FailureKind。
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections import defaultdict
from collections.abc import Iterable

from gateway.breaker import CircuitBreaker
from gateway.clock import Clock, RealClock
from gateway.config import GatewayConfig
from gateway.models import (
    AttemptRecord,
    Completeness,
    FailureKind,
    GatewayResult,
    ShardOutcome,
)
from gateway.ratelimit import TokenBucket
from gateway.retry import RetryPolicy
from gateway.selector import SelectionContext, UpstreamSelector
from gateway.upstream import MockUpstream

logger = logging.getLogger("gateway.orchestrator")


async def _silently(task: asyncio.Future) -> None:
    """等待一个我们已主动取消/收尾的任务，吞掉取消与完成异常。"""
    try:
        await task
    except BaseException:  # noqa: BLE001 - 仅用于回收被取消的任务
        pass

# 全失败时的确定性原因优先级（序号越小越优先）
_FAILURE_PRIORITY: dict[FailureKind, int] = {
    FailureKind.NO_HEALTHY_UPSTREAM: 0,
    FailureKind.DEADLINE_EXCEEDED: 1,
    FailureKind.RATE_LIMITED: 2,
    FailureKind.RETRY_BUDGET_EXHAUSTED: 3,
    FailureKind.TIMEOUT: 4,
    FailureKind.UPSTREAM_ERROR: 5,
    FailureKind.CIRCUIT_OPEN: 6,
}


class GatewayOrchestrator:
    def __init__(
        self,
        upstreams: Iterable[MockUpstream],
        config: GatewayConfig | None = None,
        clock: Clock | None = None,
    ) -> None:
        self.config = config or GatewayConfig()
        self.clock: Clock = clock or RealClock()
        self.upstreams = list(upstreams)
        if not self.upstreams:
            raise ValueError("at least one upstream is required")

        self.breakers: dict[str, CircuitBreaker] = {
            up.name: CircuitBreaker(up.name, self.config.breaker, clock=self.clock)
            for up in self.upstreams
        }
        # 每个上游副本一个令牌桶（承载能力保护）
        self.rate_limiters: dict[str, TokenBucket] = {
            up.name: TokenBucket(
                up.name,
                self.config.rate_limit.capacity,
                self.config.rate_limit.refill_per_second,
                clock=self.clock,
            )
            for up in self.upstreams
        }
        self.retry_policy = RetryPolicy(self.config.retry, clock=self.clock, seed=7)

        self._shards: dict[str, list[MockUpstream]] = defaultdict(list)
        for up in self.upstreams:
            self._shards[up.shard].append(up)
        # 固定分片顺序，保证响应结构与聚合结论确定
        self._shard_names = sorted(self._shards)
        self._selectors = {
            shard: UpstreamSelector(replicas, self.breakers)
            for shard, replicas in self._shards.items()
        }

    # ---- 公共入口 --------------------------------------------------------

    async def handle(
        self, payload: object = None, request_id: str = ""
    ) -> GatewayResult:
        request_id = request_id or uuid.uuid4().hex[:12]
        started = self.clock.now()
        deadline = started + self.config.request_timeout_seconds
        logger.info("请求 %s 开始：分片=%s，截止=%.3f",
                    request_id, self._shard_names, deadline)

        semaphore = asyncio.Semaphore(self.config.fanout_concurrency)

        async def run_shard(shard: str) -> ShardOutcome:
            async with semaphore:
                return await self._handle_shard(shard, payload, request_id, deadline)

        outcomes = await asyncio.gather(
            *(run_shard(s) for s in self._shard_names)
        )
        return self._aggregate(request_id, outcomes, started)

    def stats(self) -> dict[str, object]:
        """一致性可观测快照：各上游计数与熔断状态。"""
        return {
            up.name: {
                "shard": up.shard,
                **up.snapshot(),
                "breaker": self.breakers[up.name].snapshot(),
                "rate_limited": self.rate_limiters[up.name].rejected,
            }
            for up in self.upstreams
        }

    def reset(self) -> None:
        """复位全部运行态：上游计数、熔断器、限流器、重试预算重建（保留拓扑）。"""
        for up in self.upstreams:
            up.reset()
        self.breakers = {
            up.name: CircuitBreaker(up.name, self.config.breaker, clock=self.clock)
            for up in self.upstreams
        }
        self.rate_limiters = {
            up.name: TokenBucket(
                up.name,
                self.config.rate_limit.capacity,
                self.config.rate_limit.refill_per_second,
                clock=self.clock,
            )
            for up in self.upstreams
        }
        self.retry_policy = RetryPolicy(self.config.retry, clock=self.clock, seed=7)
        self._selectors = {
            shard: UpstreamSelector(replicas, self.breakers)
            for shard, replicas in self._shards.items()
        }
        logger.info("网关运行态已复位（拓扑不变）")

    # ---- 单分片：选择 + 调用 + 重试 -------------------------------------

    async def _handle_shard(
        self,
        shard: str,
        payload: object,
        request_id: str,
        deadline: float,
    ) -> ShardOutcome:
        selector = self._selectors[shard]
        ctx = SelectionContext()
        outcome = ShardOutcome(shard=shard, ok=False)
        last_kind = FailureKind.NO_HEALTHY_UPSTREAM

        for attempt in range(self.config.retry.max_attempts_per_shard):
            # 1) 截止时间硬约束
            remaining = deadline - self.clock.now()
            if remaining <= 0:
                last_kind = FailureKind.DEADLINE_EXCEEDED
                logger.info("请求 %s 分片 %s 判定=%s（截止时间已过）",
                            request_id, shard, last_kind.value)
                break

            # 2) 选择上游（含熔断 peek/reserve、最少在途分流）
            up, is_probe = selector.select(ctx)
            if up is None:
                last_kind = FailureKind.NO_HEALTHY_UPSTREAM
                logger.info("请求 %s 分片 %s 判定=%s（尝试#%d）",
                            request_id, shard, last_kind.value, attempt + 1)
                break

            # 3) 上游级限流（承载能力）；被限流时未发起调用，需释放预订名额
            if not self.rate_limiters[up.name].try_acquire():
                up.release_slot()
                last_kind = FailureKind.RATE_LIMITED
                logger.info("请求 %s 分片 %s 上游 %s 判定=%s",
                            request_id, shard, up.name, last_kind.value)
                break

            # 4) 调用（单次超时取“调用超时”与“剩余截止”的较小值；
            #    在途名额已由选择器预订，调用结束时由上游自行释放）
            timeout = min(self.config.call_timeout_seconds, remaining)
            kind, data, latency = await self._invoke(
                up, payload, request_id, timeout, is_probe
            )
            outcome.attempt_records.append(
                AttemptRecord(
                    replica=up.name, shard=shard, ok=kind is None,
                    failure_kind=kind, latency=latency, probe=is_probe,
                )
            )

            if kind is None:
                self.breakers[up.name].record_success(is_probe)
                outcome.ok = True
                outcome.replica = up.name
                outcome.data = data
                outcome.attempts = attempt + 1
                logger.info("请求 %s 分片 %s 成功（上游=%s, 探测=%s, 尝试#%d, 耗时=%.3f）",
                            request_id, shard, up.name, is_probe, attempt + 1, latency)
                return outcome

            last_kind = kind
            self.breakers[up.name].record_failure(is_probe)
            ctx.record_failure(up.name)
            outcome.attempts = attempt + 1

            # 5) 是否还能重试：预算 + 截止时间
            if attempt + 1 >= self.config.retry.max_attempts_per_shard:
                logger.info("请求 %s 分片 %s 停止重试（已达单分片最大尝试次数）",
                            request_id, shard)
                break
            if not await self.retry_policy.acquire_retry_permit():
                last_kind = FailureKind.RETRY_BUDGET_EXHAUSTED
                break
            backoff = self.retry_policy.backoff(attempt)
            if self.clock.now() + backoff >= deadline:
                last_kind = FailureKind.DEADLINE_EXCEEDED
                logger.info("请求 %s 分片 %s 判定=%s（退避后将超截止）",
                            request_id, shard, last_kind.value)
                break
            logger.info("请求 %s 分片 %s 第#%d 次失败(%s)，退避 %.3f 后在其它/同副本重试",
                        request_id, shard, attempt + 1, kind.value, backoff)
            await self.clock.sleep(backoff)

        outcome.failure_kind = last_kind
        logger.info("请求 %s 分片 %s 最终失败：%s，尝试次数=%d",
                    request_id, shard, last_kind.value, outcome.attempts)
        return outcome

    async def _invoke(
        self,
        up: MockUpstream,
        payload: object,
        request_id: str,
        timeout: float,
        is_probe: bool,
    ) -> tuple[FailureKind | None, object, float]:
        """单次调用，归一化所有异常为 FailureKind；返回 (kind, data, latency)。

        超时等待者直接使用注入时钟的 sleep：调用先完成则取消超时等待；
        超时等待先完成则取消仍在途的调用（慢上游及时止损）。两者同一刻到期时，
        调用定时器更早入堆会先触发，保证“恰好在超时前完成”判定为成功。
        """
        started = self.clock.now()
        # 先创建超时任务（先注册定时器），再启动调用任务：
        # 保证 asyncio.wait 观察到的任务集合在它真正挂起前就已完整，
        # 且“恰好在超时点完成”时调用定时器因更晚入堆而不会错误抢跑。
        timeout_task = asyncio.ensure_future(self.clock.sleep(timeout))
        call_task = asyncio.ensure_future(
            up.call(payload, request_id, slot_reserved=True)
        )

        async def guarded() -> tuple[FailureKind | None, object, float]:
            try:
                data = await call_task
                return None, data, self.clock.now() - started
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # 任何底层异常都归一化，绝不原样透出
                kind = FailureKind.UPSTREAM_ERROR
                logger.debug(
                    "请求 %s 上游 %s 调用异常归一化为 %s：%r（不原样透出）",
                    request_id, up.name, kind.value, exc,
                )
                return kind, None, self.clock.now() - started

        guarded_task = asyncio.ensure_future(guarded())
        try:
            await asyncio.wait(
                {guarded_task, timeout_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
        except asyncio.CancelledError:
            call_task.cancel()
            timeout_task.cancel()
            await _silently(guarded_task)
            raise

        if guarded_task.done():
            if not timeout_task.done():
                timeout_task.cancel()
                await _silently(timeout_task)
            return guarded_task.result()

        # 超时等待先完成：取消在途调用
        call_task.cancel()
        timeout_task.cancel()
        await _silently(guarded_task)
        latency = self.clock.now() - started
        logger.info("请求 %s 上游 %s 判定=timeout（%.3fs，探测=%s）",
                    request_id, up.name, timeout, is_probe)
        return FailureKind.TIMEOUT, None, latency

    # ---- 聚合 ------------------------------------------------------------

    def _aggregate(
        self,
        request_id: str,
        outcomes: list[ShardOutcome],
        started: float,
    ) -> GatewayResult:
        data = {o.shard: o.data for o in outcomes if o.ok}
        oks = [o for o in outcomes if o.ok]
        if len(oks) == len(outcomes):
            completeness = Completeness.FULL
            reason = None
        elif oks:
            completeness = Completeness.DEGRADED
            reason = None
            logger.info(
                "请求 %s 降级：成功分片=%s，失败分片=%s（完整度=%.0f%%）",
                request_id,
                [o.shard for o in oks],
                [f"{o.shard}:{o.failure_kind.value if o.failure_kind else '?'}"
                 for o in outcomes if not o.ok],
                100.0 * len(oks) / len(outcomes),
            )
        else:
            completeness = Completeness.UNAVAILABLE
            reason = min(
                (o.failure_kind for o in outcomes if o.failure_kind is not None),
                key=lambda k: _FAILURE_PRIORITY[k],
            )
            logger.info("请求 %s 全部不可用：汇总原因=%s（各分片=%s）",
                        request_id, reason.value,
                        {o.shard: o.failure_kind.value for o in outcomes})

        return GatewayResult(
            request_id=request_id,
            completeness=completeness,
            data=data,
            shard_outcomes=list(outcomes),
            failure_reason=reason,
            elapsed=self.clock.now() - started,
        )
