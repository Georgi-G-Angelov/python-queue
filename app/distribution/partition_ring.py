from __future__ import annotations

from typing import List, Dict, Sequence

from app.messaging.constants import NUM_SERVER_PARTITIONS

VIRTUAL_REPLICAS = 10


class PartitionRing:
    """Deterministic evenly spaced replica ring.

    We divide the partition index space [0, NUM_SERVER_PARTITIONS) into intervals using evenly spaced
    virtual replicas laid out in round-robin node order. With N nodes and R virtual replicas per node
    we have T = N * R replica anchors. Anchors are placed every gap = floor(NUM_SERVER_PARTITIONS / T)
    partitions. If the space does not divide evenly, the tail partitions are included in the last
    interval.

    Interval ownership: partition p belongs to the node whose anchor starts the interval containing p.
    This yields near-perfect balance differing by at most 'gap' partitions (usually 1 when evenly divisible).
    No hashing of partitions is performed.
    """

    def __init__(self, nodes: Sequence[str]):
        if not nodes:
            raise ValueError("PartitionRing requires at least one node")
        norm_nodes = [n.rstrip('/') for n in nodes]
        self._nodes = norm_nodes
        total_replicas = len(norm_nodes) * VIRTUAL_REPLICAS
        gap = max(1, NUM_SERVER_PARTITIONS // total_replicas)
        self._gap = gap
        # Build ordered list of (start_partition, node)
        anchors: List[tuple[int, str]] = []
        current = 0
        for replica_index in range(total_replicas):
            node = norm_nodes[replica_index % len(norm_nodes)]
            anchors.append((current, node))
            current += gap
            if current >= NUM_SERVER_PARTITIONS:
                break
        # Tail implicitly belongs to last anchor if not aligned
        self._anchors = anchors
        # Precompute partition owners
        self._partition_owner: Dict[int, str] = {}
        anchor_indices = [a[0] for a in anchors]
        for i, (start, node) in enumerate(anchors):
            end = anchor_indices[i + 1] if i + 1 < len(anchor_indices) else NUM_SERVER_PARTITIONS
            for p in range(start, end):
                self._partition_owner[p] = node

    def node_for_partition(self, partition: int) -> str:
        if partition < 0 or partition >= NUM_SERVER_PARTITIONS:
            raise ValueError("partition out of range")
        return self._partition_owner[partition]

    def distribution(self) -> Dict[str, int]:
        """Return a count of partitions owned per node (for diagnostics)."""
        counts: Dict[str, int] = {n: 0 for n in self._nodes}
        for owner in self._partition_owner.values():
            counts[owner] += 1
        return counts

    def gap(self) -> int:
        """Return spacing (partitions) between consecutive virtual replicas."""
        return self._gap
