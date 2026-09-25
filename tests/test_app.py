"""HTTP 层：/query 的可解释响应、200 降级、503 全失败，以及 /admin 故障注入。"""

from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from gateway.app import create_app


@pytest.fixture()
def client(caplog):
    caplog.set_level(logging.DEBUG, logger="gateway")
    return TestClient(create_app())


def test_query_full_success(client):
    logging.getLogger("gateway.test").info("HTTP 场景：默认全健康 -> FULL/200")
    resp = client.post("/query", json={"payload": {"q": 1}, "request_id": "http-1"})  # body matches QueryRequest
    assert resp.status_code == 200
    body = resp.json()
    assert body["completeness"] == "full"
    assert set(body["data"]) == {"orders", "inventory", "payments"}
    assert body["failure_reason"] is None
    assert len(body["shards"]) == 3


def test_query_degraded_is_200_explainable(client):
    logging.getLogger("gateway.test").info("HTTP 场景：注入 payments 全分片故障 -> DEGRADED/200")
    inj = client.post("/admin/scenario",
                      json={"target": "payments", "ok": False, "error": "gateway down"})
    assert inj.status_code == 200
    resp = client.post("/query", json={"request_id": "http-2"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["completeness"] == "degraded"
    assert set(body["data"]) == {"orders", "inventory"}
    bad = next(s for s in body["shards"] if s["shard"] == "payments")
    assert bad["ok"] is False
    assert bad["failure_kind"] == "upstream_error"
    # 底层错误文本不向外暴露
    assert "gateway down" not in resp.text


def test_query_all_down_is_503_classified(client):
    logging.getLogger("gateway.test").info("HTTP 场景：全部上游故障 -> UNAVAILABLE/503 + 分类原因")
    client.post("/admin/scenario", json={"target": "all", "ok": False})
    resp = client.post("/query", json={"request_id": "http-3"})
    assert resp.status_code == 503
    body = resp.json()
    assert body["completeness"] == "unavailable"
    assert body["failure_reason"] in {"upstream_error", "no_healthy_upstream"}


def test_stats_endpoint_reports_consistency(client):
    client.post("/query", json={"request_id": "http-stats"})
    resp = client.get("/admin/stats")
    assert resp.status_code == 200
    ups = resp.json()["upstreams"]
    assert len(ups) == 6
    # 每个分片恰好有一个副本被调用，且无在途残留
    for shard in ("orders", "inventory", "payments"):
        called = [name for name, s in ups.items()
                  if s["shard"] == shard and s["calls"] == 1]
        assert len(called) == 1, called
    assert all(s["in_flight"] == 0 for s in ups.values())


def test_admin_reset_restores_health(client):
    logging.getLogger("gateway.test").info("HTTP 场景：全故障 -> reset -> 恢复 FULL")
    client.post("/admin/scenario", json={"target": "all", "ok": False})
    assert client.post("/query", json={"request_id": "r-before"}).status_code == 503
    reset = client.post("/admin/reset")
    assert reset.status_code == 200 and reset.json()["reset"] is True
    resp = client.post("/query", json={"request_id": "r-after"})
    assert resp.status_code == 200
    assert resp.json()["completeness"] == "full"
    stats = client.get("/admin/stats").json()["upstreams"]
    # 复位后熔断器全部回到 closed，且无在途残留
    assert all(s["breaker"]["state"] == "closed" and s["in_flight"] == 0
               for s in stats.values())
