from fastapi.testclient import TestClient
from app.main import create_app
from app.config import NodeConfig
from app.messaging.storage import QueueStorage
from app.messaging.message import Message
import json


def test_post_message_local_write(monkeypatch):
    cfg = NodeConfig(node_id=11, peers=[])
    app = create_app(cfg)
    client = TestClient(app)
    storage = QueueStorage.get()
    # Post message without key (auto-assigned)
    resp = client.post("/post_message", json={"topic": "tpost", "value": {"x": 1}})
    assert resp.status_code == 200
    data = resp.json()
    assert data["written"] is True
    assert data["topic"] == "tpost"
    # Verify it exists via storage read (need consumer group to consume)
    # Use the returned key and compute partition
    key = data["key"]
    msg = Message(topic="tpost", key=key, value={"x": 1})
    partition = msg.server_partition()
    fetched = storage.read_message(partition, "tpost", consumer_group="cgpost")
    assert fetched is not None
    parsed = json.loads(fetched.to_json())
    assert parsed["value"] == {"x": 1}


def test_post_message_missing_fields():
    cfg = NodeConfig(node_id=12, peers=[])
    app = create_app(cfg)
    client = TestClient(app)
    resp = client.post("/post_message", json={"value": 1})
    assert resp.status_code == 400


def test_post_message_reroute(monkeypatch):
    cfg = NodeConfig(node_id=13, peers=["http://other:8000"])
    app = create_app(cfg)
    client = TestClient(app)

    membership = app.state.membership
    monkeypatch.setattr(membership, "members", lambda: [membership.self_url, "http://other:8000"])  # two nodes

    # Force PartitionRing.node_for_partition to always return other node
    from app.distribution.partition_ring import PartitionRing
    original_node_for_partition = PartitionRing.node_for_partition
    def fake_node_for_partition(self, partition: int):
        return "http://other:8000"
    monkeypatch.setattr(PartitionRing, "node_for_partition", fake_node_for_partition)

    # Mock httpx.post to simulate upstream write
    import httpx
    def fake_post(url, json=None, timeout=None):
        class FakeResp:
            status_code = 200
            def json(self):
                return {"node": "http://other:8000", "partition": 123, "key": str(json.get("key", "5")), "topic": json.get("topic"), "written": True}
            text = "ok"
        return FakeResp()
    monkeypatch.setattr(httpx, "post", fake_post)

    resp = client.post("/post_message", json={"topic": "reroute", "value": {"z": 9}, "key": "k1"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["node"] == "http://other:8000"
    assert data["written"] is True

    monkeypatch.setattr(PartitionRing, "node_for_partition", original_node_for_partition)
