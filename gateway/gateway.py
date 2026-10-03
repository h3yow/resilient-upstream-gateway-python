"""请求编排器：本工程的核心入口。

语义契约（保持稳定、可解释）：
- 各分片并发处理；单分片慢/挂不阻塞其他分片。单次上游调用有独立超时，
  整个请求有总截止时间。
- 分片内按候选副本顺序尝试，失败按预算与退避重试；重试只在副本间
  做受控转移，不在同一请求内对同一副本放大。
- 响应三态：SUCCESS（全部分片成功）/ DEGRADED（部分成功，附完整度）/
  FAILED（全部失败，附确定性分类原因）。
- 所有底层异常在本层翻译成枚举结论，不原样透出。
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait as futures_wait

from gateway.clock import Clock, SystemClock
from gateway.errors import (
    UpstreamCallError,
    UpstreamTimeout,
)
from gateway.limiter import RetryBudget
from gateway.selector import UpstreamSelector
from gateway.types import (
    AttemptRecord,
    FailureReason,
    GatewayConfig,
    GatewayRequest,
    GatewayResponse,
    OverallStatus,
    ShardResult,
    ShardStatus,
)
from gateway.upstream import UpstreamRegistry

logger = logging.getLogger("gateway")

_OUTCOME_TIMEOUT = "timeout"
_OUTCOME_ERROR = "error"
_OUTCOME_CIRCUIT_OPEN = "rejected_circuit_open"
_OUTCOME_RATE_LIMITED = "rate_limited"
_OUTCOME_SUCCESS = "success"
_OUTCOME_DEADLINE = "request_deadline"


class Gateway:
    def __init__(
        self,
        registry: UpstreamRegistry,
        config: GatewayConfig | None = None,
        *,
        clock: Clock | None = None,
        selector: UpstreamSelector | None = None,
        retry_budget: RetryBudget | None = None,
        executor: ThreadPoolExecutor | None = None,
        shift_manager: object | None = None,
    ) -> None:
        self._registry = registry
        self.config = config or GatewayConfig()
        self._clock = clock or SystemClock()
        self._selector = selector or UpstreamSelector(registry)
        self._retry_budget = retry_budget or RetryBudget(self.config.retry, self._clock)
        self._shift_manager = shift_manager
        self._executor = executor or ThreadPoolExecutor(
            max_workers=self.config.max_workers, thread_name_prefix="gw"
        )
        self._owns_executor = executor is None

    # ------------------------------------------------------------------
    def handle(self, request: GatewayRequest) -> GatewayResponse:
        start = self._clock.now()
        deadline = start + self.config.retry.request_timeout_seconds
        shards = list(request.shards)

        # 惰性推进灰度接管：按当前时钟评估加码档位/健康护栏
        if self._shift_manager is not None:
            self._shift_manager.refresh()

        future_map = {
            self._executor.submit(self._handle_shard, request.request_id, shard, deadline): shard
            for shard in shards
        }
        results_by_shard: dict[str, ShardResult] = {}
        timed_out_shards: set[str] = set()
        done, not_done = futures_wait(future_map, timeout=self.config.retry.request_timeout_seconds)
        for fut in not_done:
            shard = future_map[fut]
            timed_out_shards.add(shard)
            fut.cancel()
        for fut in done:
            shard = future_map[fut]
            results_by_shard[shard] = fut.result()

        results: list[ShardResult] = []
        for shard in shards:
            if shard in results_by_shard:
                results.append(results_by_shard[shard])
            else:
                results.append(
                    ShardResult(
                        shard=shard,
                        status=ShardStatus.DEGRADED_TIMEOUT,
                        data=None,
                        served_by=None,
                        attempts=(
                            AttemptRecord(
                                upstream="-",
                                shard=shard,
                                attempt=0,
                                outcome=_OUTCOME_DEADLINE,
                                detail="request deadline exceeded before shard completed",
                                elapsed_seconds=self._clock.now() - start,
                            ),
                        ),
                    )
                )

        succeeded = [r.shard for r in results if r.status is ShardStatus.OK]
        failed = [r.shard for r in results if r.status is not ShardStatus.OK]
        reason, completeness = self._classify(results)

        if not failed:
            status = OverallStatus.SUCCESS
            reason = None
        elif succeeded:
            status = OverallStatus.DEGRADED
        else:
            status = OverallStatus.FAILED

        data = {r.shard: r.data for r in results if r.status is ShardStatus.OK}
        response = GatewayResponse(
            request_id=request.request_id,
            status=status,
            data=data,
            completeness=completeness,
            succeeded_shards=tuple(succeeded),
            failed_shards=tuple(failed),
            failure_reason=reason,
            trace=tuple(results),
        )
        logger.info(
            "[判定] request=%s status=%s completeness=%.2f ok=%s failed=%s reason=%s",
            request.request_id,
            status.value,
            completeness,
            succeeded,
            failed,
            reason.value if reason else "-",
        )
        return response

    # ------------------------------------------------------------------
    def _handle_shard(self, request_id: str, shard: str, deadline: float) -> ShardResult:
        attempts: list[AttemptRecord] = []
        tried: set[str] = set()  # 本请求内已经真实尝试过的副本
        attempt_no = 0

        while attempt_no < self.config.retry.max_attempts:
            if self._clock.now() >= deadline:
                break
            # 每次尝试重新选择：熔断器状态/在途数可能已变化。
            # 优先选择本请求尚未尝试过的副本，实现"跨副本故障转移而非
            # 对同一坏副本放大"；所有副本都试过才回退到全候选
            # （此时 OPEN 的副本会被熔断器再次快速拒绝）。
            candidates = self._selector.candidates(shard)
            fresh = [c for c in candidates if c not in tried]
            name = (fresh or candidates)[0] if (fresh or candidates) else None
            if name is None:
                attempts.append(
                    AttemptRecord(
                        upstream="-",
                        shard=shard,
                        attempt=attempt_no + 1,
                        outcome=_OUTCOME_CIRCUIT_OPEN,
                        detail="no eligible replica: disabled or circuit open",
                        elapsed_seconds=0.0,
                    )
                )
                attempt_no += 1
                if not self._sleep_backoff(attempt_no, deadline):
                    break
                continue

            rt = self._registry.get(name)
            if not rt.limiter.try_acquire():
                attempts.append(
                    AttemptRecord(
                        upstream=name,
                        shard=shard,
                        attempt=attempt_no + 1,
                        outcome=_OUTCOME_RATE_LIMITED,
                        detail="replica at capacity; no token",
                        elapsed_seconds=0.0,
                    )
                )
                attempt_no += 1
                tried.add(name)
                # 限流不计熔断失败；尝试下一副本，仅在还有预算时退避
                if not self._sleep_backoff(attempt_no, deadline):
                    break
                continue

            # 调用前准入：HALF_OPEN 下占用探测名额。筛选（would_allow）
            # 与准入之间状态可能被并发请求改变，准入失败则快速转移。
            # 准入成功后，下面的 record_success/record_failure 必然释放名额。
            if not rt.breaker.allow_request():
                rt.limiter.refund()
                attempts.append(
                    AttemptRecord(
                        upstream=name,
                        shard=shard,
                        attempt=attempt_no + 1,
                        outcome=_OUTCOME_CIRCUIT_OPEN,
                        detail="circuit closed to new probes between selection and call",
                        elapsed_seconds=0.0,
                    )
                )
                attempt_no += 1
                tried.add(name)
                if not self._sleep_backoff(attempt_no, deadline):
                    break
                continue

            self._selector.mark_started(name)
            t0 = self._clock.now()
            remaining = deadline - t0
            timeout = max(0.0, min(self.config.per_attempt_timeout, remaining))
            outcome, detail, data = self._invoke(name, request_id, timeout)
            elapsed = self._clock.now() - t0
            self._selector.mark_finished(name)
            rt.limiter.refund()
            attempt_no += 1
            tried.add(name)

            with rt.lock:
                if outcome == _OUTCOME_SUCCESS:
                    rt.success_count += 1
                else:
                    rt.failure_count += 1

            attempts.append(
                AttemptRecord(
                    upstream=name,
                    shard=shard,
                    attempt=attempt_no,
                    outcome=outcome,
                    detail=detail,
                    elapsed_seconds=elapsed,
                )
            )

            if outcome == _OUTCOME_SUCCESS:
                rt.breaker.record_success()
                logger.info(
                    "[调用] request=%s shard=%s upstream=%s attempt=%d -> success %.4fs",
                    request_id, shard, name, attempt_no, elapsed,
                )
                return ShardResult(
                    shard=shard,
                    status=ShardStatus.OK,
                    data=data,
                    served_by=name,
                    attempts=tuple(attempts),
                )

            rt.breaker.record_failure()
            logger.info(
                "[调用] request=%s shard=%s upstream=%s attempt=%d -> %s (%s) %.4fs",
                request_id, shard, name, attempt_no, outcome, detail, elapsed,
            )
            if attempt_no >= self.config.retry.max_attempts:
                break
            # 重试需要全局预算令牌（首次尝试不消费）
            if not self._retry_budget.acquire():
                attempts.append(
                    AttemptRecord(
                        upstream=name,
                        shard=shard,
                        attempt=attempt_no,
                        outcome="retry_budget_exhausted",
                        detail="global retry budget empty; fail fast to avoid storm",
                        elapsed_seconds=0.0,
                    )
                )
                break
            if not self._sleep_backoff(attempt_no, deadline):
                break

        status, reason_detail = self._shard_status_from_attempts(attempts)
        logger.info(
            "[分片结论] request=%s shard=%s status=%s attempts=%d 依据=%s",
            request_id, shard, status.value, len(attempts), reason_detail,
        )
        return ShardResult(
            shard=shard,
            status=status,
            data=None,
            served_by=None,
            attempts=tuple(attempts),
        )

    def _invoke(self, name: str, request_id: str, timeout: float) -> tuple[str, str, object]:
        """执行单次调用并把任何异常翻译成 (outcome, detail, data)。"""
        impl = self._registry.impl(name)
        if timeout <= 0:
            return _OUTCOME_TIMEOUT, "no time budget left for attempt", None
        try:
            data = impl.call(request_id, timeout)
            return _OUTCOME_SUCCESS, "ok", data
        except UpstreamTimeout:
            return _OUTCOME_TIMEOUT, "upstream did not respond in time", None
        except UpstreamCallError:
            return _OUTCOME_ERROR, "upstream returned error", None
        except Exception as exc:  # 兜底：底层异常绝不原样透出
            return _OUTCOME_ERROR, f"unexpected upstream condition: {type(exc).__name__}", None

    def _sleep_backoff(self, attempt_no: int, deadline: float) -> bool:
        """指数退避；返回 False 表示退避后已超过总截止时间，应停止。"""
        rc = self.config.retry
        delay = rc.backoff_base_seconds * (rc.backoff_factor ** max(0, attempt_no - 1))
        remaining = deadline - self._clock.now()
        if remaining <= 0:
            return False
        self._clock.sleep(min(delay, remaining))
        return self._clock.now() < deadline

    @staticmethod
    def _shard_status_from_attempts(attempts: list[AttemptRecord]) -> tuple[ShardStatus, str]:
        """把尝试序列归约为分片结论。超时优先于错误，错误优先于无容量。"""
        outcomes = {a.outcome for a in attempts}
        if not outcomes:
            return ShardStatus.DEGRADED_TIMEOUT, "no attempt made before deadline"
        if outcomes & {_OUTCOME_TIMEOUT, _OUTCOME_DEADLINE}:
            return ShardStatus.DEGRADED_TIMEOUT, "at least one attempt timed out"
        if _OUTCOME_ERROR in outcomes:
            return ShardStatus.DEGRADED_ERROR, "all attempts errored"
        if _OUTCOME_RATE_LIMITED in outcomes:
            return ShardStatus.DEGRADED_DISABLED, "all replicas at capacity"
        return ShardStatus.DEGRADED_DISABLED, "no eligible replica (disabled/open)"

    @staticmethod
    def _classify(results: list[ShardResult]) -> tuple[FailureReason | None, float]:
        total = len(results)
        ok = sum(1 for r in results if r.status is ShardStatus.OK)
        completeness = ok / total if total else 0.0
        failed = [r for r in results if r.status is not ShardStatus.OK]
        if not failed:
            return None, 1.0
        kinds = {r.status for r in failed}
        if len(kinds) > 1:
            reason = FailureReason.MIXED
        elif all(r.status is ShardStatus.DEGRADED_TIMEOUT for r in failed):
            reason = FailureReason.ALL_TIMEOUT
        elif all(r.status is ShardStatus.DEGRADED_ERROR for r in failed):
            reason = FailureReason.ALL_UPSTREAM_ERROR
        else:
            reason = FailureReason.NO_CAPACITY
        return reason, completeness

    def shutdown(self) -> None:
        if self._owns_executor:
            self._executor.shutdown(wait=False)
