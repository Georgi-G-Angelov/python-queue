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
        self._nodes: List[str] = []
        self._gap = 0
        self._anchors: List[tuple[int, str]] = []
        self._partition_owner: Dict[int, str] = {}
        self._rebuild(nodes)

    def _rebuild(self, nodes: Sequence[str]) -> None:
        """(Re)construct internal structures from supplied node list."""
        norm_nodes = sorted({n.rstrip('/') for n in nodes})
        self._nodes = norm_nodes
        total_replicas = len(norm_nodes) * VIRTUAL_REPLICAS
        gap = max(1, NUM_SERVER_PARTITIONS // total_replicas)
        self._gap = gap
        anchors: List[tuple[int, str]] = []
        current = 0
        for replica_index in range(total_replicas):
            node = norm_nodes[replica_index % len(norm_nodes)]
            anchors.append((current, node))
            current += gap
            if current >= NUM_SERVER_PARTITIONS:
                break
        self._anchors = anchors
        self._partition_owner = {}
        anchor_indices = [a[0] for a in anchors]
        for i, (start, node) in enumerate(anchors):
            end = anchor_indices[i + 1] if i + 1 < len(anchor_indices) else NUM_SERVER_PARTITIONS
            for p in range(start, end):
                self._partition_owner[p] = node

    def add_node(self, node: str) -> bool:
        """Add a node to the ring and rebalance. Returns False if node already present.

        Node URL is normalized (rstrip('/')). Rebuild is full (O(NUM_SERVER_PARTITIONS)) which is acceptable
        for cluster membership changes of modest frequency. For higher churn we'd need incremental math.
        """
        norm = node.rstrip('/')
        if norm in self._nodes:
            return False
        new_nodes = self._nodes + [norm]
        self._rebuild(new_nodes)
        return True

    def remove_node(self, node: str) -> bool:
        """Remove a node from the ring and rebalance. Returns False if node absent or if removal
        would leave the ring empty.

        Node URL is normalized (rstrip('/')). Cannot remove last remaining node; caller must
        handle decommission sequencing. Rebuild is full (O(NUM_SERVER_PARTITIONS)).
        """
        norm = node.rstrip('/')
        if norm not in self._nodes:
            return False
        if len(self._nodes) == 1:
            return False  # preserve at least one node
        new_nodes = [n for n in self._nodes if n != norm]
        self._rebuild(new_nodes)
        return True

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

    def partitions_for_node(self, node: str) -> List[int]:
        """Return sorted list of partition indices owned by the given node.

        Node URL is normalized (rstrip('/')). If node not present returns empty list.
        """
        norm = node.rstrip('/')
        if norm not in self._nodes:
            return []
        # Collect partitions where owner matches norm
        return [p for p, owner in self._partition_owner.items() if owner == norm]

    def moved_partitions_after_change(self, old_ring: 'PartitionRing') -> List[int]:
        """Return list of partition indices whose owner changed compared to old_ring.

        Both rings must cover the same NUM_SERVER_PARTITIONS. Partitions are considered moved if
        the owning node differs (string comparison after normalization performed in rings). The
        result list is sorted ascending.
        """
        moved: List[int] = []
        # Fast path: if old_ring has identical owner mapping reference-wise (rare) return empty
        if old_ring is self:
            return moved
        for p in range(NUM_SERVER_PARTITIONS):
            if self._partition_owner.get(p) != old_ring._partition_owner.get(p):
                moved.append(p)
        return moved
