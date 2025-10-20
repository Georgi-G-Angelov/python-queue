from fastapi.testclient import TestClient
from app.main import create_app
from app.config import NodeConfig
from app.messaging.storage import QueueStorage
from app.messaging.message import Message
import json
import pytest


def test_migration_triggered_once(monkeypatch):
    # Start with single node owning everything
    cfg = NodeConfig(node_id=61, peers=["http://newnode:8000"])  # anticipate future node
    app = create_app(cfg)
    client = TestClient(app)
    storage = QueueStorage.get()

    # Write a couple of messages on a topic to create segment and descriptor
    msg1 = Message(topic="migtopic", key="k1", value={"v":1})
    storage.write_message(msg1)
    msg2 = Message(topic="migtopic", key="k1", value={"v":2})
    storage.write_message(msg2)
    part = msg1.server_partition()

    # Counters for backfill calls
    calls = {"segment":0, "state":0}

    import httpx
    def fake_post(url, json=None, timeout=None):
        if url.endswith("/backfill_segment"):
            calls["segment"] += 1
        if url.endswith("/backfill_state"):
            calls["state"] += 1
        class FakeResp:
            status_code = 200
            def json(self):
                return {"ok": True}
            text = "ok"
        return FakeResp()
    monkeypatch.setattr(httpx, "post", fake_post)

    # Monkeypatch membership.members and PartitionRing to simulate owner change after first post_message
    membership = app.state.membership
    monkeypatch.setattr(membership, "members", lambda: [membership.self_url, "http://newnode:8000"])

    from app.distribution.partition_ring import PartitionRing
    original_node_for_partition = PartitionRing.node_for_partition
    # First call: owner is self; second call: owner becomes new node
    state = {"phase":0}
    def fake_node_for_partition(self, p:int):
        if p == part:
            if state["phase"] == 0:
                return membership.self_url  # initial owner
            else:
                return "http://newnode:8000"  # new owner
        return original_node_for_partition(self, p)
    monkeypatch.setattr(PartitionRing, "node_for_partition", fake_node_for_partition)

    # First post_message (no migration expected)
    resp1 = client.post("/post_message", json={"topic":"migtopic", "key":"k1", "value":3})
    assert resp1.status_code == 200
    assert calls["segment"] == 0 and calls["state"] == 0

    # Flip phase to simulate ring change
    state["phase"] = 1

    # Second post_message triggers migration
    resp2 = client.post("/post_message", json={"topic":"migtopic", "key":"k1", "value":4})
    assert resp2.status_code == 200
    assert calls["segment"] == 1 and calls["state"] == 1

    # Third post_message should NOT re-trigger migration
    resp3 = client.post("/post_message", json={"topic":"migtopic", "key":"k1", "value":5})
    assert resp3.status_code == 200
    assert calls["segment"] == 1 and calls["state"] == 1

    # Restore original
    monkeypatch.setattr(PartitionRing, "node_for_partition", original_node_for_partition)


def test_migration_lock_no_duplicate(monkeypatch):
    cfg = NodeConfig(node_id=62, peers=["http://newnode:8000"])
    app = create_app(cfg)
    client = TestClient(app)
    storage = QueueStorage.get()
    m = Message(topic="locktopic", key="lk", value=1)
    storage.write_message(m)
    part = m.server_partition()

    import httpx
    calls = {"segment":0, "state":0}
    def fake_post(url, json=None, timeout=None):
        if url.endswith("/backfill_segment"):
            calls["segment"] += 1
        if url.endswith("/backfill_state"):
            calls["state"] += 1
        class FakeResp:
            status_code = 200
            def json(self): return {"ok":True}
            text = "ok"
        return FakeResp()
    monkeypatch.setattr(httpx, "post", fake_post)

    membership = app.state.membership
    monkeypatch.setattr(membership, "members", lambda: [membership.self_url, "http://newnode:8000"])

    from app.distribution.partition_ring import PartitionRing
    real = PartitionRing.node_for_partition
    def fake(self, p:int):
        return "http://newnode:8000" if p == part else real(self,p)
    monkeypatch.setattr(PartitionRing, "node_for_partition", fake)

    # Fire multiple concurrent posts to attempt duplicate migration
    for i in range(5):
        client.post("/post_message", json={"topic":"locktopic", "key":"lk", "value":i})
    # Expect single migration
    assert calls["segment"] == 1 and calls["state"] == 1
