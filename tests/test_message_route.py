from fastapi.testclient import TestClient
from app.main import create_app
from app.config import NodeConfig
from app.messaging.message import Message
from app.messaging.storage import QueueStorage
import json


def test_message_route_local_partition(monkeypatch):
    # Configure single node so it owns every partition
    cfg = NodeConfig(node_id=5, peers=[])
    app = create_app(cfg)
    client = TestClient(app)
    storage = QueueStorage.get()
    # Write a message
    msg = Message(topic="tloc", key="42", value={"v": 1})
    storage.write_message(msg)
    partition = msg.server_partition()
    # Read via route
    resp = client.get("/message", params={"topic": "tloc", "key": "42", "consumer_group": "cg1"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["partition"] == partition
    assert data["message"] is not None
    parsed = json.loads(data["message"])
    assert parsed["topic"] == "tloc" and parsed["key"] == "42"


def test_message_route_missing_fields():
    cfg = NodeConfig(node_id=6, peers=[])
    app = create_app(cfg)
    client = TestClient(app)
    resp = client.get("/message", params={"topic": "a"})
    # Missing required 'key' and 'consumer_group' query params -> FastAPI validation error 422
    assert resp.status_code == 422


def test_message_route_reroute(monkeypatch):
    # Two-node scenario; we simulate reroute by monkeypatching PartitionRing.node_for_partition
    cfg = NodeConfig(node_id=7, peers=["http://other:8000"])
    app = create_app(cfg)
    client = TestClient(app)

    # Monkeypatch membership.members to return two nodes
    membership = app.state.membership
    monkeypatch.setattr(membership, "members", lambda: [membership.self_url, "http://other:8000"])

    # Force PartitionRing.node_for_partition to return other node for target partition
    from app.distribution.partition_ring import PartitionRing
    original_node_for_partition = PartitionRing.node_for_partition
    def fake_node_for_partition(self, partition: int):
        return "http://other:8000"
    monkeypatch.setattr(PartitionRing, "node_for_partition", fake_node_for_partition)

    # Mock httpx.get to simulate upstream response
    import httpx
    def fake_get(url, params=None, json=None, timeout=None):
        class FakeResp:
            status_code = 200
            def json(self):
                return {"node": "http://other:8000", "partition": 123, "message": None}
            text = "ok"
        return FakeResp()
    monkeypatch.setattr(httpx, "get", fake_get)

    resp = client.get("/message", params={"topic": "x", "key": "y", "consumer_group": "cg"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["node"] == "http://other:8000"
    assert "message" in data

    # Restore original (not strictly necessary in test context)
    monkeypatch.setattr(PartitionRing, "node_for_partition", original_node_for_partition)
