from __future__ import annotations

from fastapi import FastAPI
from datetime import datetime, UTC
from app.config import NodeConfig
from app.gossip import MembershipManager


def register_routes(app: FastAPI) -> None:
    @app.get("/health", tags=["system"])
    async def health() -> dict[str, str]:
        return {"status": "ok", "message": "hello world"}

    @app.get("/")
    async def root() -> dict[str, object]:
        cfg: NodeConfig = app.state.config  # type: ignore[attr-defined]
        return {
            "service": "python-queue",
            "docs": "/docs",
            "node_id": cfg.node_id,
            "peers": list(cfg.peers),
        }

    @app.get("/cluster/info", tags=["cluster"])
    async def cluster_info() -> dict[str, object]:
        cfg: NodeConfig = app.state.config  # type: ignore[attr-defined]
        membership: MembershipManager = app.state.membership  # type: ignore[attr-defined]
        return {
            "node_id": cfg.node_id,
            "peers": list(cfg.peers),
            "peer_count": len(cfg.peers),
            "members": membership.members(),
        }

    @app.get("/cluster/members", tags=["cluster"])
    async def cluster_members() -> dict[str, object]:
        membership: MembershipManager = app.state.membership  # type: ignore[attr-defined]
        snapshot = membership.snapshot()
        iso_map = {u: datetime.fromtimestamp(ts, UTC).isoformat().replace('+00:00', 'Z') for u, ts in snapshot.items()}
        return {"members": membership.members(), "last_seen": iso_map}

    @app.post("/cluster/gossip", tags=["cluster"])
    async def cluster_gossip(payload: dict[str, object]) -> dict[str, object]:
        membership: MembershipManager = app.state.membership  # type: ignore[attr-defined]
        incoming_map = payload.get("members", {})  # type: ignore[assignment]
        if not isinstance(incoming_map, dict):
            incoming_map = {}
        sender = payload.get("sender") if isinstance(payload.get("sender"), str) else None
        membership.apply_snapshot(incoming_map, sender)
        return {"known": membership.snapshot()}
