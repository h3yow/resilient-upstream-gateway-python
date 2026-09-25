"""HTTP 层测试：三态响应序列化、状态查看与管理面停用、参数校验。"""

from __future__ import annotations

from fastapi.testclient import TestClient

from gateway.app import create_app
from gateway.scenarios import SHARDS


def _client(behaviors=None):
    return TestClient(create_app(behaviors))


def test_healthz_and_scenario():
    c = _client({})
    assert c.get("/healthz").json() == {"ok": True}
    info = c.get("/scenario").json()
    assert set(info["shards"]) == set(SHARDS)
    print("[场景] HTTP 健康检查与场景查询 -> 200")


def test_success_path_serialized():
    c = _client({})  # 全健康
    r = c.post("/orchestrate", json={"request_id": "h1", "shards": list(SHARDS)})
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "success"
    assert body["completeness"] == 1.0
    assert body["failure_reason"] is None
    assert len(body["trace"]) == len(SHARDS)
    print(f"[场景] HTTP 全成功 -> status=success，trace 分片数={len(body['trace'])}")


def test_degraded_path_over_http():
    behaviors = {
        "orders-0": {"latency": 1.0, "weight": 1000},
        "orders-1": {"fail_every": 1},
    }
    c = _client(behaviors)
    r = c.post(
        "/orchestrate",
        json={
            "request_id": "h2",
            "shards": list(SHARDS),
        },
    )
    body = r.json()
    assert body["status"] == "degraded"
    # HTTP 序列化时 completeness 保留 4 位小数（2/3 -> 0.6667）
    assert abs(body["completeness"] - 2 / 3) < 1e-4
    assert body["failed_shards"] == ["orders"]
    # 响应中没有任何底层异常文本
    assert "injected error" not in r.text
    print("[场景] HTTP 部分失败 -> degraded/0.6667，且无底层异常文本透出")


def test_admin_disable_then_failed_no_capacity():
    c = _client({})
    for name in ("orders-0", "orders-1"):
        r = c.post(f"/admin/upstreams/{name}/state", json={"state": "disabled"})
        assert r.status_code == 200
    body = c.post(
        "/orchestrate", json={"request_id": "h3", "shards": ["orders"]}
    ).json()
    assert body["status"] == "failed"
    assert body["failure_reason"] == "no_capacity"
    print("[场景] 管理面停用 orders 全部副本 -> failed/no_capacity")


def test_unknown_shard_rejected():
    c = _client({})
    r = c.post("/orchestrate", json={"request_id": "h4", "shards": ["nope"]})
    assert r.status_code == 400
    print("[场景] 未知分片 -> 400，判定依据: 注册表中不存在该分片")


def test_upstreams_snapshot_endpoint():
    c = _client({})
    c.post("/orchestrate", json={"request_id": "h5", "shards": list(SHARDS)})
    body = c.get("/upstreams").json()["upstreams"]
    assert len(body) == len(SHARDS) * 2
    total_ok = sum(v["success_count"] for v in body.values())
    assert total_ok == len(SHARDS)
    assert all(v["active_calls"] == 0 for v in body.values())
    print(f"[场景] /upstreams 显示 {total_ok} 次成功计数，与 3 个分片一致")
