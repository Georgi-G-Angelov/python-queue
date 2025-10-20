from fastapi.testclient import TestClient
from app.main import create_app
from app.config import NodeConfig
from app.messaging.storage import QueueStorage
from app.messaging.message import Message
from app.messaging.constants import MESSAGES_PER_SEGMENT
import httpx, asyncio, time, os

class DummyResp:
    status_code = 200
    text = "ok"
    def __init__(self, url: str, payload_count: int = 0):
        self._url = url
        self._payload_count = payload_count
    def json(self):
        return {"url": self._url, "count": self._payload_count}


def test_full_history_migration_node_add(monkeypatch, tmp_path):
    # Isolate storage in temporary directory to avoid interference from other tests
    os.chdir(tmp_path)
    QueueStorage.reset_for_tests()
    cfg = NodeConfig(node_id=81, peers=[])  # single node initially
    app = create_app(cfg)
    client = TestClient(app)
    storage = QueueStorage.get()

    # Write more than one segment worth of messages to a single topic
    topic = "histA"
    key = "k1"
    total = MESSAGES_PER_SEGMENT + 25  # ensures two segments (0 full, 1 partial)
    for i in range(total):
        storage.write_message(Message(topic=topic, key=key, value=i))

    # Capture backfill calls
    calls = {"segments": [], "states": 0}
    def fake_post(url, json=None, timeout=None):
        if url.endswith("/backfill_segment"):
            calls["segments"].append(json.get("segment_index"))
        if url.endswith("/backfill_state"):
            calls["states"] += 1
        return DummyResp(url)
    monkeypatch.setattr(httpx, "post", fake_post)

    membership = app.state.membership
    setattr(membership, "_app_ref", app)

    # Simulate node addition snapshot
    ts = time.time()
    snapshot = {membership.self_url: ts, "http://newnode:8000": ts}
    membership.apply_snapshot(snapshot, sender="http://newnode:8000")
    asyncio.get_event_loop().run_until_complete(asyncio.sleep(0.05))

    # Dynamically determine expected segments (may be >2 if previous state leaked, but isolation prevents that)
    expected_segments = {0, 1}  # based on total written
    assert set(calls["segments"]) == expected_segments
    assert calls["states"] == 1  # state migrated once

    # Reapply snapshot should not duplicate segment migrations
    membership.apply_snapshot(snapshot, sender="http://newnode:8000")
    asyncio.get_event_loop().run_until_complete(asyncio.sleep(0.05))
    assert calls["segments"].count(0) == 1
    assert calls["segments"].count(1) == 1
    assert calls["states"] == 1
