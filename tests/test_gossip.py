from fastapi.testclient import TestClient
from app.main import create_app
from app.config import build_config
from app.gossip import MembershipManager


def test_membership_merge():
    mm = MembershipManager("http://self:8000")
    mm.merge(["http://a:1", "http://b:2", "http://a:1"])  # duplicate should not create multiple
    members = set(mm.members())
    assert "http://self:8000" in members
    assert "http://a:1" in members
    assert "http://b:2" in members
    assert len(members) == 3


def test_gossip_endpoint_merges():
    cfg = build_config(0, ["http://peer1:8001"])  # peers seeded
    app = create_app(cfg)
    client = TestClient(app)
    # Initially membership should contain self_url default and peer1
    initial = client.get("/cluster/members").json()["members"]
    assert any("peer1" in m for m in initial)
    # Post gossip with new peers
    resp = client.post("/cluster/gossip", json={"members": ["http://peer2:8002", "http://peer3:8003"]})
    assert resp.status_code == 200
    merged = set(resp.json()["known"])
    assert any("peer2" in m for m in merged)
    assert any("peer3" in m for m in merged)
