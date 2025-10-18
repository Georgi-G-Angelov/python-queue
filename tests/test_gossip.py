from fastapi.testclient import TestClient
from app.main import create_app
from app.config import build_config
import time


def test_gossip_endpoint_snapshot_merge():
    cfg = build_config(0, ["http://peer1:8001"])  # peers seeded
    app = create_app(cfg)
    client = TestClient(app)
    initial_members = set(client.get("/cluster/members").json()["members"])
    assert "http://peer1:8001" in initial_members
    # Prepare snapshot with new peers and artificial timestamps
    ts = time.time() - 5  # older timestamp
    newer_ts = time.time()
    snapshot = {
        "http://peer2:8002": ts,
        "http://peer3:8003": newer_ts,
    }
    resp = client.post("/cluster/gossip", json={"members": snapshot, "sender": "http://peer2:8002"})
    assert resp.status_code == 200
    known_map = resp.json()["known"]
    assert "http://peer2:8002" in known_map
    assert "http://peer3:8003" in known_map
    # peer2 should have been 'touched' so its last_seen should be > provided ts
    assert known_map["http://peer2:8002"] > ts
    # peer3 retains its provided newer timestamp (no touch since sender != peer3)
    assert abs(known_map["http://peer3:8003"] - newer_ts) < 1.0
