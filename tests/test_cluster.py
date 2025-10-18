from fastapi.testclient import TestClient
from app.main import create_app
from app.config import build_config


def test_cluster_info_default():
    app = create_app()
    client = TestClient(app)
    r = client.get("/cluster/info")
    assert r.status_code == 200
    data = r.json()
    assert data["node_id"] == 0
    assert data["peers"] == []
    assert data["peer_count"] == 0


def test_cluster_info_custom_config():
    cfg = build_config(3, ["http://a:1", "http://b:2"])  # type: ignore[arg-type]
    app = create_app(cfg)
    client = TestClient(app)
    r = client.get("/cluster/info")
    assert r.status_code == 200
    data = r.json()
    assert data["node_id"] == 3
    assert set(data["peers"]) == {"http://a:1", "http://b:2"}
    assert data["peer_count"] == 2
