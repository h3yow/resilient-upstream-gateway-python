"""FastAPI 装配：把编排器暴露为 HTTP 服务，并提供观测/管理端点。

- ``POST /orchestrate``：发起编排请求，返回稳定三态响应；
- ``GET /upstreams``：查看各上游状态、运行期策略与调用计数；
- ``GET /scenario``：查看当前注入场景；
- ``POST /admin/upstreams/{name}/state``：手动启用/停用某副本；
- ``POST /admin/upstreams/{name}/policy``：运行期调整权重/并发承载；
- ``POST /admin/shifts`` + ``GET /admin/shifts``：发起/观测灰度接管。

仅用于本地验证；生产语义以编排器返回的三态响应为准。
"""

from __future__ import annotations

import math

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from gateway.rollout import TrafficShiftManager
from gateway.scenarios import SHARDS, build_gateway
from gateway.types import (
    BreakerConfig,
    GatewayConfig,
    GatewayRequest,
    RetryConfig,
    UpstreamState,
)

# 默认演示场景：orders-0 恒定慢（每次先超时再故障转移到 orders-1），
# inventory-0 前两次超时随后恢复（可连续调用观察缓慢恢复）。
DEFAULT_BEHAVIORS = {
    "orders-0": {"latency": 0.5, "weight": 1000},
    "inventory-0": {"timeout_nth": [1, 2], "latency": 0.001, "weight": 1000},
}

DEFAULT_CONFIG = GatewayConfig(
    retry=RetryConfig(
        max_attempts=3,
        backoff_base_seconds=0.005,
        request_timeout_seconds=1.0,
    ),
    per_attempt_timeout=0.08,
)
DEFAULT_BREAKER = BreakerConfig(
    failure_threshold=3, cooldown_seconds=0.2, success_to_close=2
)


class OrchestrateBody(BaseModel):
    request_id: str = "demo"
    shards: list[str] = Field(default_factory=lambda: list(SHARDS))


class StateBody(BaseModel):
    state: UpstreamState


class PolicyBody(BaseModel):
    """运行期策略调整：只传要改的字段。"""

    weight: int | None = None
    capacity: float | None = None
    refill_per_second: float | None = None


class ShiftBody(BaseModel):
    shard: str
    new_replica: str
    old_replicas: list[str]
    steps: int = 5
    step_interval_seconds: float = 0.2


def _response_to_dict(resp) -> dict:
    return {
        "request_id": resp.request_id,
        "status": resp.status.value,
        "data": resp.data,
        "completeness": round(resp.completeness, 4),
        "succeeded_shards": list(resp.succeeded_shards),
        "failed_shards": list(resp.failed_shards),
        "failure_reason": resp.failure_reason.value if resp.failure_reason else None,
        "trace": [
            {
                "shard": sr.shard,
                "status": sr.status.value,
                "served_by": sr.served_by,
                "attempts": [
                    {
                        "upstream": a.upstream,
                        "attempt": a.attempt,
                        "outcome": a.outcome,
                        "detail": a.detail,
                        "elapsed_seconds": round(a.elapsed_seconds, 4),
                    }
                    for a in sr.attempts
                ],
            }
            for sr in resp.trace
        ],
    }


def create_app(
    behaviors: dict | None = None,
    config: GatewayConfig | None = None,
    breaker: BreakerConfig | None = None,
) -> FastAPI:
    built = build_gateway(
        behaviors if behaviors is not None else DEFAULT_BEHAVIORS,
        config=config or DEFAULT_CONFIG,
        breaker=breaker or DEFAULT_BREAKER,
    )
    gateway = built.gateway
    registry = built.registry
    shift_manager = TrafficShiftManager(registry)
    gateway.shift_manager = shift_manager

    app = FastAPI(title="Resilient Upstream Gateway", version="0.1.0")

    @app.get("/healthz")
    def healthz():
        return {"ok": True}

    @app.post("/orchestrate")
    def orchestrate(body: OrchestrateBody):
        unknown = [s for s in body.shards if s not in registry.shards()]
        if unknown:
            raise HTTPException(status_code=400, detail=f"unknown shards: {unknown}")
        resp = gateway.handle(
            GatewayRequest(request_id=body.request_id, shards=tuple(body.shards))
        )
        return _response_to_dict(resp)

    @app.get("/upstreams")
    def upstreams():
        snap = registry.snapshot()
        for v in snap.values():
            v["state"] = v["state"].value
            v["admin_state"] = v["admin_state"].value
            policy = v.get("policy")
            if policy:
                for k, val in policy.items():
                    if isinstance(val, float) and not math.isfinite(val):
                        policy[k] = "inf"
        return {"upstreams": snap}

    @app.get("/scenario")
    def scenario():
        return {"shards": list(SHARDS), "behaviors": behaviors or DEFAULT_BEHAVIORS}

    @app.post("/admin/upstreams/{name}/state")
    def set_state(name: str, body: StateBody):
        try:
            registry.get(name)
        except KeyError:
            raise HTTPException(status_code=404, detail=f"unknown upstream: {name}")
        if body.state is UpstreamState.DISABLED:
            registry.disable(name)
        elif body.state is UpstreamState.HEALTHY:
            registry.enable(name)
        else:
            rt = registry.get(name)
            with rt.lock:
                rt.state = body.state
        return {"name": name, "admin_state": body.state.value}

    @app.post("/admin/upstreams/{name}/policy")
    def set_policy(name: str, body: PolicyBody):
        """运行期调整权重/并发承载，不重启生效；只影响之后的新请求。"""
        try:
            policy = registry.update_policy(
                name,
                weight=body.weight,
                capacity=body.capacity,
                refill_per_second=body.refill_per_second,
            )
        except KeyError:
            raise HTTPException(status_code=404, detail=f"unknown upstream: {name}")
        return {
            "name": name,
            "policy": {
                "weight": policy.weight,
                "capacity": policy.capacity if math.isfinite(policy.capacity) else "inf",
                "refill_per_second": (
                    policy.refill_per_second
                    if math.isfinite(policy.refill_per_second)
                    else "inf"
                ),
                "traffic_scale": policy.traffic_scale,
            },
        }

    @app.post("/admin/shifts")
    def begin_shift(body: ShiftBody):
        """发起灰度接管：新副本从小流量开始按节奏加码。"""
        try:
            shift_id = shift_manager.begin_shift(
                body.shard,
                body.new_replica,
                body.old_replicas,
                steps=body.steps,
                step_interval_seconds=body.step_interval_seconds,
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return {"shift_id": shift_id}

    @app.get("/admin/shifts")
    def shifts():
        """各接管的进度、状态与当前实际流量比例。"""
        return {"shifts": shift_manager.status()}

    return app


app = create_app()
