"""FastAPI 装配：把编排器暴露为 HTTP 服务，并提供观测/管理端点。

- ``POST /orchestrate``：发起编排请求，返回稳定三态响应；
- ``GET /upstreams``：查看各上游状态与调用计数；
- ``GET /scenario``：查看当前注入场景；
- ``POST /admin/upstreams/{name}/state``：手动启用/停用某副本。

仅用于本地验证；生产语义以编排器返回的三态响应为准。
"""

from __future__ import annotations

import math

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

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
            rt = registry.get(name)
        except KeyError:
            raise HTTPException(status_code=404, detail=f"unknown upstream: {name}")
        rt.state = body.state
        return {"name": name, "admin_state": body.state.value}

    return app


app = create_app()
