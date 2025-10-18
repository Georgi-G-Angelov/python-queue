from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence
import os

from fastapi import FastAPI


@dataclass(frozen=True)
class NodeConfig:
    """Configuration for a queue node.

    Attributes:
        node_id: Integer identifier for this node (unique within cluster).
        peers: Sequence of peer base URLs (e.g. http://host:port) excluding self.
    """

    node_id: int
    peers: Sequence[str]


def create_app(config: NodeConfig | None = None) -> FastAPI:
    """Application factory allowing injection of node configuration.

    The config is stored in ``app.state`` so endpoints can access it.
    If no config is provided, defaults are used (node_id=0, peers=[]).
    """

    if config is None:
        config = NodeConfig(node_id=0, peers=[])

    app = FastAPI(title="python-queue", version="0.1.0")
    app.state.config = config

    @app.get("/health", tags=["system"])  # Simple health/hello endpoint
    async def health() -> dict[str, str]:  # noqa: D401 - short description fine
        """Return a basic health payload."""
        return {"status": "ok", "message": "hello world"}

    @app.get("/")
    async def root() -> dict[str, object]:
        cfg: NodeConfig = app.state.config
        return {
            "service": "python-queue",
            "docs": "/docs",
            "node_id": cfg.node_id,
            "peers": list(cfg.peers),
        }

    @app.get("/cluster/info", tags=["cluster"])
    async def cluster_info() -> dict[str, object]:
        cfg: NodeConfig = app.state.config
        return {"node_id": cfg.node_id, "peers": list(cfg.peers), "peer_count": len(cfg.peers)}

    return app


# Default app instance for ASGI auto-discovery (uvicorn app.main:app)
app = create_app()


def build_config(node_id: int, peers: Iterable[str]) -> NodeConfig:
    # Simple helper for converting iterable to tuple and constructing NodeConfig
    return NodeConfig(node_id=node_id, peers=tuple(peers))


def create_app_from_env() -> FastAPI:
    """Create app using environment variables.

    Supported env vars:
        QUEUE_NODE_ID: int (default 0)
        QUEUE_PEERS: comma or space separated list of peer URLs
    """
    raw_id = os.getenv("QUEUE_NODE_ID", "0")
    try:
        node_id = int(raw_id)
    except ValueError:  # fallback to 0 if invalid
        node_id = 0
    raw_peers = os.getenv("QUEUE_PEERS", "").strip()
    if raw_peers:
        # Allow both comma and space separated
        if "," in raw_peers:
            parts = [p.strip() for p in raw_peers.split(",") if p.strip()]
        else:
            parts = [p.strip() for p in raw_peers.split() if p.strip()]
    else:
        parts = []
    return create_app(NodeConfig(node_id=node_id, peers=tuple(parts)))
