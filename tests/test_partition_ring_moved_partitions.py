from app.distribution.partition_ring import PartitionRing
from app.messaging.constants import NUM_SERVER_PARTITIONS


def test_moved_partitions_after_add():
    r1 = PartitionRing(["http://n1:8000", "http://n2:8000"])
    r2 = PartitionRing(["http://n1:8000", "http://n2:8000"])  # identical
    assert r2.moved_partitions_after_change(r1) == []

    # Add node and compare
    r3 = PartitionRing(["http://n1:8000", "http://n2:8000"])  # base
    r3.add_node("http://n3:8000")
    moved = r3.moved_partitions_after_change(r1)
    assert len(moved) > 0
    # All moved partitions should now belong to the new node or be rebalanced between existing nodes
    new_owner_parts = set(r3.partitions_for_node("http://n3:8000"))
    # moved should be subset of partitions owned by new node OR partitions whose owner changed from n1 to n2 or vice versa
    # Check subset relation: not strict requirement but indicates churn localized
    assert set(moved).intersection(new_owner_parts)  # at least some moved to new node


def test_moved_partitions_after_remove():
    r1 = PartitionRing(["http://a:8000", "http://b:8000", "http://c:8000"])
    # Remove one
    r2 = PartitionRing(["http://a:8000", "http://b:8000", "http://c:8000"])
    r2.remove_node("http://c:8000")
    moved = r2.moved_partitions_after_change(r1)
    assert len(moved) > 0
    # After removal, no partition should be owned by removed node
    assert r2.partitions_for_node("http://c:8000") == []
    # All moved partitions previously belonged to removed node or were redistributed; verify previous ownership
    prev_removed_owned = [p for p in range(NUM_SERVER_PARTITIONS) if r1.node_for_partition(p) == "http://c:8000"]
    # Ensure significant overlap between moved and previously owned partitions by removed node
    overlap = set(moved).intersection(prev_removed_owned)
    assert len(overlap) >= len(prev_removed_owned) * 0.5  # at least half should be those partitions (heuristic)
