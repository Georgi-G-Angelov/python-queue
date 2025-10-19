import json
from fastapi.testclient import TestClient
from app.main import create_app
from app.config import NodeConfig


def test_node_partitions_route_basic():
    cfg = NodeConfig(node_id=7, peers=["http://n2:8000", "http://n3:8000"])
    app = create_app(cfg)
    client = TestClient(app)
    resp = client.get("/node/partitions")
    assert resp.status_code == 200
    data = resp.json()
    assert "node" in data and "partitions" in data
    assert isinstance(data["partitions"], list)
    assert data["partition_count"] == len(data["partitions"])
    assert data["total_partitions"] >= data["partition_count"]
    # Ensure partitions are ints and sorted
    assert data["partitions"] == sorted(data["partitions"])
    assert all(isinstance(p, int) for p in data["partitions"])
