from app.distribution.partition_ring import PartitionRing, VIRTUAL_REPLICAS
from app.messaging.constants import NUM_SERVER_PARTITIONS


def test_add_node_rebalance():
    initial = ["http://n1:8000", "http://n2:8000"]
    ring = PartitionRing(initial)
    dist_before = ring.distribution()
    assert set(dist_before.keys()) == {"http://n1:8000", "http://n2:8000"}
    total_before = sum(dist_before.values())
    assert total_before == NUM_SERVER_PARTITIONS

    added = ring.add_node("http://n3:8000")
    assert added is True
    dist_after = ring.distribution()
    assert set(dist_after.keys()) == {"http://n1:8000", "http://n2:8000", "http://n3:8000"}
    total_after = sum(dist_after.values())
    assert total_after == NUM_SERVER_PARTITIONS

    # Distribution should be closer to even with 3 nodes; difference from average <= gap
    avg = NUM_SERVER_PARTITIONS / 3
    for count in dist_after.values():
        assert abs(count - avg) <= ring.gap()


def test_add_existing_node_no_change():
    ring = PartitionRing(["http://a:8000", "http://b:8000"])
    dist_before = ring.distribution().copy()
    added = ring.add_node("http://a:8000/")  # trailing slash variant
    assert added is False
    dist_after = ring.distribution()
    assert dist_after == dist_before
