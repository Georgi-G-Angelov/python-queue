from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_health():
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["message"] == "hello world"


def test_root():
    resp = client.get("/")
    assert resp.status_code == 200
    assert resp.json()["service"] == "python-queue"
