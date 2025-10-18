from app.gossip import MembershipManager


def test_pick_peer_seed_first_then_union():
    mm = MembershipManager("http://self:8000", seeds=["http://seed1:8001", "http://seed2:8002"])
    first = mm.pick_peer()
    assert first in {"http://seed1:8001", "http://seed2:8002"}
    # Add a dynamic member after first gossip
    mm.apply_snapshot({"http://dyn:8003": mm.snapshot()[mm.self_url]}, sender="http://dyn:8003")
    second = mm.pick_peer()
    # After first pass, could be either a seed or dynamic
    assert second in {"http://seed1:8001", "http://seed2:8002", "http://dyn:8003"}
