from fastapi.testclient import TestClient
from app.main import create_app
from app.config import NodeConfig
from app.messaging.message import Message


def test_backfill_segment_partition_mismatch():
    cfg = NodeConfig(node_id=41, peers=[])
    app = create_app(cfg)
    client = TestClient(app)

    # Construct a message to discover real partition
    tmp = Message(topic="mismatch", key="k1", value=1)
    real_part = tmp.server_partition()
    wrong_part = (real_part + 1) % 10  # NUM_SERVER_PARTITIONS is 10 in constants

    payload = {
        "topic": "mismatch",
        "server_partition": wrong_part,
        "segment_index": 0,
        "messages": [
            {"key": "k1", "value": 1}
        ]
    }
    resp = client.post("/backfill_segment", json=payload)
    assert resp.status_code == 400
    assert "Declared server_partition" in resp.text
