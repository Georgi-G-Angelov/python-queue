from fastapi.testclient import TestClient
from app.main import create_app


def test_members_last_seen_present():
    app = create_app()
    client = TestClient(app)
    data = client.get("/cluster/members").json()
    assert "members" in data
    assert "last_seen" in data
    # last_seen map should have same keys
    assert set(data["members"]) == set(data["last_seen"].keys())
    # values should be ISO8601 strings ending with 'Z'
    from datetime import datetime
    for ts in data["last_seen"].values():
        assert isinstance(ts, str)
        assert ts.endswith("Z")
        # parse
        parsed = datetime.fromisoformat(ts.rstrip("Z"))
        assert parsed.year >= 2024
