from __future__ import annotations

from fastapi import FastAPI
from app.distribution.partition_ring import PartitionRing
from app.messaging.constants import NUM_SERVER_PARTITIONS
from app.config import NodeConfig
from app.gossip import MembershipManager

# We keep a simple cached ring per request cycle; for now build on demand.

def register_node_routes(app: FastAPI) -> None:
    @app.get("/node/partitions", tags=["node"])
    async def node_partitions() -> dict[str, object]:
        cfg: NodeConfig = app.state.config  # type: ignore[attr-defined]
        membership: MembershipManager = app.state.membership  # type: ignore[attr-defined]
        nodes = membership.members()
        # Build ring with current members (deterministic ordering applied inside PartitionRing)
        ring = PartitionRing(nodes)
        self_url = membership.self_url if hasattr(membership, "self_url") else nodes[0]
        parts = ring.partitions_for_node(self_url)
        return {
            "node": self_url,
            "partition_count": len(parts),
            "total_partitions": NUM_SERVER_PARTITIONS,
            "partitions": parts,
        }
