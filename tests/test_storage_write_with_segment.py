from app.messaging.storage import QueueStorage
from app.messaging.message import Message
from app.config import NodeConfig
from app.main import create_app
import pytest
import json


def setup_storage(node_id: int):
    # Helper to ensure fresh singleton per test
    QueueStorage.reset_for_tests()
    app = create_app(NodeConfig(node_id=node_id, peers=[]))
    return QueueStorage.get(), app


def test_write_message_at_segment_basic():
    storage, _ = setup_storage(21)
    # Use same key so both messages map to same partition making sequential read assertions simpler
    msg1 = Message(topic="segA", key="k", value={"i": 1})
    path1 = storage.write_message_at_segment(msg1, segment_index=0)
    assert path1.name == "0"
    msg2 = Message(topic="segA", key="k", value={"i": 2})
    path2 = storage.write_message_at_segment(msg2, segment_index=0)
    assert path2.name == "0"
    # Read sequentially via consumer group (ordering by append, not segment index holes)
    part = msg1.server_partition()
    r1 = storage.read_message(part, "segA", consumer_group="cg")
    r2 = storage.read_message(part, "segA", consumer_group="cg")
    assert r1 and r2
    parsed1 = json.loads(r1.to_json())
    parsed2 = json.loads(r2.to_json())
    assert parsed1["value"]["i"] == 1
    assert parsed2["value"]["i"] == 2


def test_write_message_at_segment_negative_index():
    storage, _ = setup_storage(22)
    msg = Message(topic="segB", key="k", value=1)
    with pytest.raises(ValueError):
        storage.write_message_at_segment(msg, segment_index=-1)
