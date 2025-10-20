from fastapi.testclient import TestClient
from app.main import create_app

client = TestClient(create_app())


def test_health():
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["message"] == "hello world"


def test_root():
    resp = client.get("/")
    assert resp.status_code == 200
    data = resp.json()
    assert data["service"] == "python-queue"
    assert data["node_id"] == 0  # default when no config provided
    assert data["peers"] == []
