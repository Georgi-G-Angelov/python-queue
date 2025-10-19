from app.distribution.partition_ring import PartitionRing
from app.messaging.constants import NUM_SERVER_PARTITIONS


def test_partitions_for_node_basic():
    ring = PartitionRing(["http://n1:8000", "http://n2:8000", "http://n3:8000"])
    dist = ring.distribution()
    total = sum(dist.values())
    assert total == NUM_SERVER_PARTITIONS
    parts_n2 = ring.partitions_for_node("http://n2:8000")
    assert len(parts_n2) == dist["http://n2:8000"]
    # Ensure partitions are within range and sorted
    assert parts_n2 == sorted(parts_n2)
    assert all(0 <= p < NUM_SERVER_PARTITIONS for p in parts_n2)


def test_partitions_for_node_unknown():
    ring = PartitionRing(["http://a:8000", "http://b:8000"])
    assert ring.partitions_for_node("http://c:8000") == []


def test_partitions_for_node_after_add_remove():
    ring = PartitionRing(["http://x:8000", "http://y:8000"])
    before_x = set(ring.partitions_for_node("http://x:8000"))
    ring.add_node("http://z:8000")
    after_x = set(ring.partitions_for_node("http://x:8000"))
    # Some partitions may move; ensure still correct count
    dist = ring.distribution()
    assert len(after_x) == dist["http://x:8000"]
    # Remove node y and verify x partitions updated again
    ring.remove_node("http://y:8000")
    final_x = set(ring.partitions_for_node("http://x:8000"))
    dist_final = ring.distribution()
    assert len(final_x) == dist_final["http://x:8000"]
    # All partitions accounted for across remaining nodes
    all_parts = ring.partitions_for_node("http://x:8000") + ring.partitions_for_node("http://z:8000")
    assert sorted(all_parts) == list(range(NUM_SERVER_PARTITIONS))
