from app.distribution.hashring import HashRing, VIRTUAL_REPLICAS
from app.messaging.constants import NUM_SERVER_PARTITIONS
import math


def test_single_node_all_partitions():
    ring = HashRing(["http://n1:8000"])
    for p in range(NUM_SERVER_PARTITIONS):
        assert ring.node_for_partition(p) == "http://n1:8000".rstrip('/')
    dist = ring.distribution()
    assert dist == {"http://n1:8000": NUM_SERVER_PARTITIONS}


def test_multi_node_distribution_reasonable():
    nodes = [f"http://n{i}:8000" for i in range(5)]
    ring = HashRing(nodes)
    dist = ring.distribution()
    # Check that each node got a non-zero share and roughly even (+/- 30%)
    expected_avg = NUM_SERVER_PARTITIONS / len(nodes)
    for n in nodes:
        count = dist[n.rstrip('/')]
        assert count > 0
        assert abs(count - expected_avg) <= expected_avg * 0.30, f"Imbalance for {n}: {count} vs avg {expected_avg}"


def test_node_for_partition_bounds():
    ring = HashRing(["http://a:8000", "http://b:8000"])  # two nodes
    assert ring.node_for_partition(0) in {"http://a:8000", "http://b:8000"}
    assert ring.node_for_partition(NUM_SERVER_PARTITIONS - 1) in {"http://a:8000", "http://b:8000"}


def test_invalid_partition_raises():
    ring = HashRing(["http://a:8000"])  # one node
    try:
        ring.node_for_partition(-1)
        assert False, "Expected ValueError for negative partition"
    except ValueError:
        pass
    try:
        ring.node_for_partition(NUM_SERVER_PARTITIONS)
        assert False, "Expected ValueError for out-of-range partition"
    except ValueError:
        pass


def test_virtual_replicas_constant():
    assert VIRTUAL_REPLICAS == 10
