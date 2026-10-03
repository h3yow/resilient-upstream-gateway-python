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


def test_admin_policy_update_over_http():
    """运行期调权重：调 0 后新请求不再命中该副本，/upstreams 可看到策略。"""
    c = _client({})
    r = c.post("/admin/upstreams/orders-0/policy", json={"weight": 0})
    assert r.status_code == 200
    assert r.json()["policy"]["weight"] == 0
    body = c.post(
        "/orchestrate", json={"request_id": "p1", "shards": ["orders"]}
    ).json()
    orders = next(t for t in body["trace"] if t["shard"] == "orders")
    assert orders["served_by"] == "orders-1"
    snap = c.get("/upstreams").json()["upstreams"]
    assert snap["orders-0"]["policy"]["weight"] == 0
    print("[场景] HTTP 调权重 1000->0（默认1->0）后新请求全部转移 orders-1，"
          "判定依据: served_by 与 /upstreams policy 一致")


def test_admin_policy_unknown_upstream_404():
    c = _client({})
    r = c.post("/admin/upstreams/nope/policy", json={"weight": 2})
    assert r.status_code == 404
    print("[场景] 对不存在副本调策略 -> 404")


def test_admin_shift_begin_and_status_over_http():
    """发起接管后可轮询进度；完成后新副本流量比例为 1。"""
    c = _client({})
    r = c.post("/admin/shifts", json={
        "shard": "orders", "new_replica": "orders-1",
        "old_replicas": ["orders-0"], "steps": 2,
        "step_interval_seconds": 0.05,
    })
    assert r.status_code == 200
    shift_id = r.json()["shift_id"]
    st = c.get("/admin/shifts").json()["shifts"][shift_id]
    assert st["state"] == "ramping"
    assert st["traffic_share"]["orders-1"] == 0.5
    import time
    time.sleep(0.12)
    c.post("/orchestrate", json={"request_id": "s1", "shards": ["orders"]})
    st = c.get("/admin/shifts").json()["shifts"][shift_id]
    assert st["state"] == "completed"
    assert st["traffic_share"]["orders-1"] == 1.0
    print(f"[场景] HTTP 发起接管 {shift_id}：ramping(0.5) -> completed(1.0)，"
          "进度与流量比例可轮询观测")


def test_admin_shift_unknown_replica_404():
    c = _client({})
    r = c.post("/admin/shifts", json={
        "shard": "orders", "new_replica": "orders-9", "old_replicas": ["orders-0"],
    })
    assert r.status_code == 404
    print("[场景] 对不存在副本发起接管 -> 404")
