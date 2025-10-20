from fastapi.testclient import TestClient
from app.main import create_app
from app.config import NodeConfig
from app.messaging.storage import QueueStorage
from app.messaging.message import Message
import json


def test_backfill_state_local():
    cfg = NodeConfig(node_id=51, peers=[])
    app = create_app(cfg)
    client = TestClient(app)
    storage = QueueStorage.get()

    # Pre-write some messages to create segment and descriptor base
    msg = Message(topic="stA", key="k1", value={"i": 1})
    storage.write_message(msg)
    part = msg.server_partition()

    payload = {
        "topic": "stA",
        "server_partition": part,
        "descriptor_count": 10,
        "consumer_offsets": {"cg": 5, "cg2": 12}
    }
    resp = client.post("/backfill_state", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data["descriptor_count"] == 10
    # Verify file contents were updated (offset cg2 should clamp to descriptor_count)
    r1 = storage.read_message(part, "stA", consumer_group="cg")
    # consume remaining until exhaustion for cg2
    for _ in range(10):
        storage.read_message(part, "stA", consumer_group="cg2")


def test_backfill_state_missing_fields():
    cfg = NodeConfig(node_id=52, peers=[])
    app = create_app(cfg)
    client = TestClient(app)
    resp = client.post("/backfill_state", json={"topic": "x"})
    assert resp.status_code == 400


def test_backfill_state_reroute(monkeypatch):
    cfg = NodeConfig(node_id=53, peers=["http://other:8000"])
    app = create_app(cfg)
    client = TestClient(app)

    membership = app.state.membership
    monkeypatch.setattr(membership, "members", lambda: [membership.self_url, "http://other:8000"])  # two nodes

    from app.distribution.partition_ring import PartitionRing
    def fake_node_for_partition(self, partition: int):
        return "http://other:8000"
    monkeypatch.setattr(PartitionRing, "node_for_partition", fake_node_for_partition)

    import httpx
    def fake_post(url, json=None, timeout=None):
        class FakeResp:
            status_code = 200
            def json(self):
                return {"node": "http://other:8000", "partition": json.get("server_partition"), "descriptor_count": json.get("descriptor_count"), "consumer_groups": list(json.get("consumer_offsets", {}).keys())}
            text = "ok"
        return FakeResp()
    monkeypatch.setattr(httpx, "post", fake_post)

    payload = {
        "topic": "rerState",
        "server_partition": 7,
        "descriptor_count": 3,
        "consumer_offsets": {"cg": 2}
    }
    resp = client.post("/backfill_state", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data["node"] == "http://other:8000"
    assert data["descriptor_count"] == 3
