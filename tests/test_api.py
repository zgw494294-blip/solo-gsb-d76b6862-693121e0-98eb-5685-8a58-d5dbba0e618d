"""HTTP 层冒烟测试（FastAPI TestClient）。"""
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_health():
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_resolve_endpoint_ok():
    resp = client.post(
        "/resolve",
        json={
            "replica_ids": ["A", "B"],
            "events": [
                {"replica_id": "A", "seq": 1, "op": "put", "key": "k1",
                 "value": "v1", "clock": [1, 0]},
                {"replica_id": "B", "seq": 1, "op": "put", "key": "k1",
                 "value": "v2", "clock": [1, 1]},
                {"replica_id": "B", "seq": 2, "op": "delete", "key": "k2",
                 "clock": [1, 2]},
            ],
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert [r["key"] for r in body["results"]] == ["k1", "k2"]
    k1, k2 = body["results"]
    assert k1["status"] == "present" and k1["value"] == "v2"
    assert k2["status"] == "deleted" and k2["value"] is None


def test_resolve_endpoint_semantic_422_points_to_event():
    resp = client.post(
        "/resolve",
        json={
            "replica_ids": ["A", "B"],
            "events": [
                {"replica_id": "A", "seq": 1, "op": "put", "key": "k",
                 "value": "v", "clock": [1, 0]},
                {"replica_id": "A", "seq": 2, "op": "put", "key": "k",
                 "value": "w", "clock": [9, 2]},
            ],
        },
    )
    assert resp.status_code == 422
    detail = resp.json()["detail"]
    assert any(e["replica_id"] == "A" and e["seq"] == 2 for e in detail)
    assert any(e["rule"] == "clock_points_to_missing_event" for e in detail)


def test_resolve_endpoint_shape_422():
    # 只有 1 个副本，违反 2~8 个副本的要求
    resp = client.post(
        "/resolve",
        json={
            "replica_ids": ["only-one"],
            "events": [
                {"replica_id": "only-one", "seq": 1, "op": "delete",
                 "key": "k", "clock": [1]}
            ],
        },
    )
    assert resp.status_code == 422


def test_resolve_endpoint_put_requires_value():
    resp = client.post(
        "/resolve",
        json={
            "replica_ids": ["A", "B"],
            "events": [
                {"replica_id": "A", "seq": 1, "op": "put", "key": "k",
                 "clock": [1, 0]}
            ],
        },
    )
    assert resp.status_code == 422
