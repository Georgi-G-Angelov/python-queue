from app.gossip import MembershipManager


def test_pick_peer_uses_seeds_when_no_members():
    mm = MembershipManager("http://self:8000", seeds=["http://seed1:8001", "http://seed2:8002"])
    # At this point members() only has self, but seeds should be considered
    selections = set()
    for _ in range(50):
        peer = mm.pick_peer()
        assert peer in {"http://seed1:8001", "http://seed2:8002"}
        selections.add(peer)
        if len(selections) == 2:
            break
    assert selections == {"http://seed1:8001", "http://seed2:8002"}
