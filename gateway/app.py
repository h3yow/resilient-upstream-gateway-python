"""FastAPI 装配：HTTP 入口 + 本地验证用的可观测/故障注入端点。

演示网关内置 3 个分片（orders / inventory / payments），每个分片 2 个本地
模拟副本，全部行为可通过 /admin/scenario 注入，不依赖任何外部服务。
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, Response
from pydantic import BaseModel, Field

from gateway.config import GatewayConfig
from gateway.orchestrator import GatewayOrchestrator
from gateway.upstream import Behavior, MockUpstream

logger = logging.getLogger("gateway.app")


# 演示拓扑：分片 -> 副本数
DEMO_TOPOLOGY: dict[str, list[str]] = {
    "orders": ["orders-a", "orders-b"],
    "inventory": ["inventory-a", "inventory-b"],
    "payments": ["payments-a", "payments-b"],
}


class QueryRequest(BaseModel):
    payload: dict[str, Any] = Field(default_factory=dict)
    request_id: str | None = None


class ScenarioRequest(BaseModel):
    # 目标："all" 或具体上游名（如 orders-a）或分片名（如 orders）
    target: str = "all"
    ok: bool = True
    delay: float = 0.0
    error: str = "injected failure"


def _build_demo_gateway(config: GatewayConfig | None = None):
    """构造本地演示网关；返回 (orchestrator, upstreams_by_name, clock)。

    默认所有副本健康（零延迟成功）。
    """
    from gateway.clock import RealClock

    clock = RealClock()
    upstreams: list[MockUpstream] = []
    for shard, names in DEMO_TOPOLOGY.items():
        for name in names:
            upstreams.append(
                MockUpstream(name, shard, Behavior(data=None), clock=clock)
            )
    gw = GatewayOrchestrator(upstreams, config or GatewayConfig(), clock=clock)
    by_name = {up.name: up for up in upstreams}
    return gw, by_name, clock


def create_app(config: GatewayConfig | None = None) -> FastAPI:
    app = FastAPI(
        title="resilient-upstream-gateway",
        description="多上游选择 / 熔断 / 限流 / 带预算重试 / 降级编排网关",
        version="0.1.0",
    )
    gw, by_name, _clock = _build_demo_gateway(config)
    app.state.gateway = gw
    app.state.upstreams = by_name

    def _serialize(result) -> dict[str, Any]:
        return {
            "request_id": result.request_id,
            "completeness": result.completeness.value,
            "data": result.data,
            "failure_reason": result.failure_reason.value
            if result.failure_reason
            else None,
            "elapsed": round(result.elapsed, 4),
            "shards": [
                {
                    "shard": o.shard,
                    "ok": o.ok,
                    "replica": o.replica,
                    "attempts": o.attempts,
                    "failure_kind": o.failure_kind.value
                    if o.failure_kind
                    else None,
                    "trace": [
                        {
                            "replica": r.replica,
                            "ok": r.ok,
                            "probe": r.probe,
                            "failure_kind": r.failure_kind.value
                            if r.failure_kind
                            else None,
                            "latency": round(r.latency, 4),
                        }
                        for r in o.attempt_records
                    ],
                }
                for o in result.shard_outcomes
            ],
        }

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    from gateway.models import Completeness

    @app.post("/query")
    async def query(req: QueryRequest, response: Response) -> dict[str, Any]:
        result = await gw.handle(req.payload, request_id=req.request_id or "")
        body = _serialize(result)
        # HTTP 层面也可区分：部分降级保持 200；全部不可用返回 503，原因在 body
        if result.completeness is Completeness.UNAVAILABLE:
            response.status_code = 503
        return body

    @app.get("/admin/stats")
    async def stats() -> dict[str, Any]:
        return {"upstreams": gw.stats()}

    @app.post("/admin/scenario")
    async def scenario(req: ScenarioRequest) -> dict[str, Any]:
        """注入上游行为（成功/报错/延迟）。target 支持 all / 分片名 / 副本名。"""
        targets: list[MockUpstream] = []
        if req.target == "all":
            targets = list(by_name.values())
        elif req.target in by_name:
            targets = [by_name[req.target]]
        elif req.target in DEMO_TOPOLOGY:
            targets = [
                up for name, up in by_name.items()
                if name.startswith(f"{req.target}-")
            ]
        else:
            return {"applied": [], "warning": f"unknown target: {req.target}"}

        behavior = Behavior(ok=req.ok, delay=req.delay, error=req.error)
        for up in targets:
            up._behavior = behavior
        logger.info("故障注入：target=%s ok=%s delay=%s error=%s -> %s",
                    req.target, req.ok, req.delay, req.error,
                    [u.name for u in targets])
        return {"applied": [u.name for u in targets]}

    @app.post("/admin/reset")
    async def reset() -> dict[str, Any]:
        """复位演示环境：所有副本恢复健康行为，计数、熔断器与预算全部归零。"""
        for up in by_name.values():
            up._behavior = Behavior()
        gw.reset()
        logger.info("演示环境已复位")
        return {"reset": True}

    return app


# 供 uvicorn 加载
app = create_app()
