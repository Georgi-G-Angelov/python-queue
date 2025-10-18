from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence
import os


@dataclass(frozen=True)
class NodeConfig:
    """Configuration for a queue node.

    Attributes:
        node_id: Integer identifier for this node (unique within cluster).
        peers: Sequence of peer base URLs (e.g. http://host:port) excluding self.
    """

    node_id: int
    peers: Sequence[str]


def build_config(node_id: int, peers: Iterable[str]) -> NodeConfig:
    """Construct a NodeConfig from raw pieces."""
    return NodeConfig(node_id=node_id, peers=tuple(peers))


def load_config_from_env() -> NodeConfig:
    """Load configuration from environment variables.

    QUEUE_NODE_ID: int (default 0)
    QUEUE_PEERS: comma or space separated list of peer URLs
    """
    raw_id = os.getenv("QUEUE_NODE_ID", "0")
    try:
        node_id = int(raw_id)
    except ValueError:
        node_id = 0
    raw_peers = os.getenv("QUEUE_PEERS", "").strip()
    if raw_peers:
        if "," in raw_peers:
            parts = [p.strip() for p in raw_peers.split(",") if p.strip()]
        else:
            parts = [p.strip() for p in raw_peers.split() if p.strip()]
    else:
        parts = []
    return NodeConfig(node_id=node_id, peers=tuple(parts))
