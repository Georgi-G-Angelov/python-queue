import os
from fastapi.testclient import TestClient
from importlib import reload
import app.main as main_mod


def test_create_app_from_env(monkeypatch):
    monkeypatch.setenv("QUEUE_NODE_ID", "7")
    monkeypatch.setenv("QUEUE_PEERS", "http://x:1,http://y:2")
    # Reload module to ensure any caching doesn't interfere (not strictly needed here)
    reload(main_mod)
    app = main_mod.create_app_from_env()
    client = TestClient(app)
    r = client.get("/cluster/info")
    assert r.status_code == 200
    data = r.json()
    assert data["node_id"] == 7
    assert set(data["peers"]) == {"http://x:1", "http://y:2"}
    assert data["peer_count"] == 2
