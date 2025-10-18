from __future__ import annotations

from fastapi import FastAPI
from datetime import datetime, UTC
from contextlib import asynccontextmanager
from app.config import NodeConfig, load_config_from_env, build_config
from app.gossip import MembershipManager, DEFAULT_GOSSIP_INTERVAL, DEFAULT_PRUNE_AGE
import os
import asyncio
import httpx


def create_app(config: NodeConfig | None = None) -> FastAPI:
    """Application factory allowing injection of node configuration.

    The config is stored in ``app.state`` so endpoints can access it.
    If no config is provided, defaults are used (node_id=0, peers=[]).
    """
    if config is None:
        config = NodeConfig(node_id=0, peers=[])

    # Lifespan context handles startup/shutdown of background tasks
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Start background gossip task
        app.state.gossip_task = asyncio.create_task(_gossip_loop())
        try:
            yield
        finally:
            task: asyncio.Task = app.state.gossip_task
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    app = FastAPI(title="python-queue", version="0.1.0", lifespan=lifespan)
    app.state.config = config
    # Determine self URL (priority: env var set by run_server.py, else http://127.0.0.1:port assumed later)
    self_url = os.getenv("QUEUE_SELF_URL", "http://127.0.0.1:8000")
    app.state.membership = MembershipManager(self_url, seeds=config.peers)

    @app.get("/health", tags=["system"])  # Simple health/hello endpoint
    async def health() -> dict[str, str]:
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
        membership: MembershipManager = app.state.membership
        return {
            "node_id": cfg.node_id,
            "peers": list(cfg.peers),
            "peer_count": len(cfg.peers),
            "members": membership.members(),
        }

    @app.get("/cluster/members", tags=["cluster"])
    async def cluster_members() -> dict[str, object]:
        membership: MembershipManager = app.state.membership
        snapshot = membership.snapshot()
        iso_map = {u: datetime.fromtimestamp(ts, UTC).isoformat().replace('+00:00', 'Z') for u, ts in snapshot.items()}
        return {"members": membership.members(), "last_seen": iso_map}

    @app.post("/cluster/gossip", tags=["cluster"])
    async def cluster_gossip(payload: dict[str, object]) -> dict[str, object]:
        """Receive gossip membership snapshot.

        Expected payload: {'members': {url: last_seen_ts, ...}, 'sender': 'http://host:port'}
        We update our membership timestamps only if incoming ts is newer.
        The sender is marked alive (its last_seen set to now).
        """
        membership: MembershipManager = app.state.membership
        incoming_map = payload.get("members", {})  # type: ignore[assignment]
        if not isinstance(incoming_map, dict):
            incoming_map = {}
        sender = payload.get("sender") if isinstance(payload.get("sender"), str) else None
        membership.apply_snapshot(incoming_map, sender)
        return {"known": membership.snapshot()}

    async def _gossip_loop() -> None:
        membership: MembershipManager = app.state.membership
        interval = float(os.getenv("GOSSIP_INTERVAL", str(DEFAULT_GOSSIP_INTERVAL)))
        prune_age = float(os.getenv("GOSSIP_PRUNE_AGE", str(DEFAULT_PRUNE_AGE)))
        async with httpx.AsyncClient(timeout=2.0) as client:
            while True:
                membership.prune(prune_age)
                peer = membership.pick_peer()
                if peer:
                    try:
                        await client.post(
                            f"{peer}/cluster/gossip",
                            json={
                                "members": membership.snapshot(),
                                "sender": membership.self_url,
                            },
                        )
                    except Exception:
                        print("Couldn't find peer " + str(peer))
                        pass
                await asyncio.sleep(interval)

    return app


def create_app_from_env() -> FastAPI:
    return create_app(load_config_from_env())

# Default app instance for ASGI auto-discovery (uvicorn app.main:app)
app = create_app()
