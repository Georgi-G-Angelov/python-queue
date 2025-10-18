from __future__ import annotations

from fastapi import FastAPI
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

    app = FastAPI(title="python-queue", version="0.1.0")
    app.state.config = config
    # Determine self URL (priority: env var set by run_server.py, else http://127.0.0.1:port assumed later)
    self_url = os.getenv("QUEUE_SELF_URL", "http://127.0.0.1:8000")
    app.state.membership = MembershipManager(self_url)
    # Seed peers from config
    for p in config.peers:
        app.state.membership.add_or_touch(p)

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
        return {"members": membership.members()}

    @app.post("/cluster/gossip", tags=["cluster"])
    async def cluster_gossip(payload: dict[str, list[str]]) -> dict[str, object]:
        """Receive gossip membership list and merge it."""
        membership: MembershipManager = app.state.membership
        incoming = payload.get("members", [])
        membership.merge(incoming)
        return {"known": membership.members()}

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
                            json={"members": membership.members()},
                        )
                    except Exception:
                        # Ignore transient errors; peer may be down
                        pass
                await asyncio.sleep(interval)

    @app.on_event("startup")
    async def on_startup() -> None:
        # Start background gossip task
        app.state.gossip_task = asyncio.create_task(_gossip_loop())

    @app.on_event("shutdown")
    async def on_shutdown() -> None:
        task: asyncio.Task = app.state.gossip_task
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    return app


def create_app_from_env() -> FastAPI:
    return create_app(load_config_from_env())


# Default app instance for ASGI auto-discovery (uvicorn app.main:app)
app = create_app()
