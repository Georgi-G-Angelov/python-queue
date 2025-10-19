from app.distribution.partition_ring import PartitionRing
from app.messaging.constants import NUM_SERVER_PARTITIONS


def test_remove_node_rebalance():
    ring = PartitionRing(["http://n1:8000", "http://n2:8000", "http://n3:8000"])
    dist_before = ring.distribution()
    assert len(dist_before) == 3
    removed = ring.remove_node("http://n2:8000/")  # trailing slash variant
    assert removed is True
    dist_after = ring.distribution()
    assert len(dist_after) == 2
    assert set(dist_after.keys()) == {"http://n1:8000", "http://n3:8000"}
    assert sum(dist_after.values()) == NUM_SERVER_PARTITIONS
    avg = NUM_SERVER_PARTITIONS / 2
    for count in dist_after.values():
        # Should be within gap of average
        assert abs(count - avg) <= ring.gap()


def test_remove_nonexistent_node():
    ring = PartitionRing(["http://a:8000", "http://b:8000"])
    removed = ring.remove_node("http://c:8000")
    assert removed is False


def test_remove_last_node_disallowed():
    ring = PartitionRing(["http://solo:8000"])
    removed = ring.remove_node("http://solo:8000")
    assert removed is False
    assert len(ring.distribution()) == 1
