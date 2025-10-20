from fastapi.testclient import TestClient
from app.main import create_app
from app.config import NodeConfig
from app.messaging.storage import QueueStorage
from app.messaging.message import Message
import json


def test_backfill_segment_local(monkeypatch):
    cfg = NodeConfig(node_id=31, peers=[])
    app = create_app(cfg)
    client = TestClient(app)
    storage = QueueStorage.get()

    # Determine correct server_partition for provided messages
    tmp = Message(topic="btopic", key="k1", value=None)
    part = tmp.server_partition()
    payload = {
        "topic": "btopic",
        "server_partition": part,
        "segment_index": 0,
        "messages": [
            {"key": "k1", "value": {"a": 1}},
            {"key": "k1", "value": {"a": 2}},
        ]
    }
    resp = client.post("/backfill_segment", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data["count_written"] == 2
    # Read two messages via consumer group
    msg_template = Message(topic="btopic", key="k1", value=None)
    part = msg_template.server_partition()
    r1 = storage.read_message(part, "btopic", consumer_group="cg")
    r2 = storage.read_message(part, "btopic", consumer_group="cg")
    assert r1 and r2
    parsed2 = json.loads(r2.to_json())
    assert parsed2["value"]["a"] == 2


def test_backfill_segment_missing_fields():
    cfg = NodeConfig(node_id=32, peers=[])
    app = create_app(cfg)
    client = TestClient(app)
    resp = client.post("/backfill_segment", json={"topic": "x"})
    assert resp.status_code == 400


def test_backfill_segment_reroute(monkeypatch):
    cfg = NodeConfig(node_id=33, peers=["http://other:8000"])
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
                return {"node": "http://other:8000", "partition": 123, "segment_index": json.get("segment_index"), "count_written": len(json.get("messages", []))}
            text = "ok"
        return FakeResp()
    monkeypatch.setattr(httpx, "post", fake_post)

    tmp2 = Message(topic="rertopic", key="k1", value=None)
    part2 = tmp2.server_partition()
    payload = {
        "topic": "rertopic",
        "server_partition": part2,
        "segment_index": 5,
        "messages": [
            {"key": "k1", "value": 1}
        ]
    }
    resp = client.post("/backfill_segment", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data["node"] == "http://other:8000"
    assert data["count_written"] == 1

    monkeypatch.setattr(PartitionRing, "node_for_partition", original_node_for_partition)
