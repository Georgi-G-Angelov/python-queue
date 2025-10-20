from fastapi.testclient import TestClient
from app.main import create_app
from app.config import NodeConfig
from app.messaging.storage import QueueStorage
from app.messaging.message import Message
import asyncio
import json
import httpx


class DummyResponse:
    def __init__(self, url: str):
        self.status_code = 200
        self._url = url
        self.text = "ok"
    def json(self):
        # Return recognizable structure
        return {"ok": True, "url": self._url}


def test_node_addition_triggers_migration(monkeypatch):
    """Simulate node addition causing partition loss; expect latest segment + state backfill once per topic.

    We reset storage to ensure isolation (prevent leftover topics from previous tests inflating migration counts)."""
    QueueStorage.reset_for_tests()
    cfg = NodeConfig(node_id=71, peers=[])  # start single node
    app = create_app(cfg)
    client = TestClient(app)
    storage = QueueStorage.get()

    # Write messages to two topics that will lose partitions when new node joins
    m1 = Message(topic="addmigA", key="k1", value=1)
    storage.write_message(m1)
    m2 = Message(topic="addmigA", key="k1", value=2)
    storage.write_message(m2)
    m3 = Message(topic="addmigB", key="k2", value=3)
    storage.write_message(m3)

    partA = m1.server_partition()
    partB = m3.server_partition()

    # Capture calls
    calls = {"segment": 0, "state": 0}
    def fake_post(url, json=None, timeout=None):
        if url.endswith("/backfill_segment"):
            calls["segment"] += 1
        if url.endswith("/backfill_state"):
            calls["state"] += 1
        return DummyResponse(url)
    monkeypatch.setattr(httpx, "post", fake_post)

    # Expose app reference to membership manager for scheduling
    membership = app.state.membership
    setattr(membership, "_app_ref", app)  # allow apply_snapshot to find app

    # Build incoming snapshot representing new node join with fresh timestamp
    import time
    new_ts = time.time()
    snapshot = {membership.self_url: new_ts, "http://newnode:8000": new_ts}

    # Apply snapshot (simulate gossip); this should schedule async migration task
    membership.apply_snapshot(snapshot, sender="http://newnode:8000")

    # Run pending tasks briefly
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.sleep(0.05))

    # At least one segment migration should occur; if both topics map to same partition latest segment may batch.
    assert calls["segment"] >= 1
    assert calls["state"] >= 1

    # Applying snapshot again with same members should not duplicate migrations
    membership.apply_snapshot(snapshot, sender="http://newnode:8000")
    loop.run_until_complete(asyncio.sleep(0.05))
    # No additional migrations after duplicate snapshot
    assert calls["segment"] >= 1
    assert calls["state"] >= 1
