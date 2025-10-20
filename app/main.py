from __future__ import annotations

from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.config import NodeConfig, load_config_from_env, build_config
from app.gossip import MembershipManager, gossip_loop
from app.routes import register_routes
from app.messaging.storage import QueueStorage
import os
import asyncio


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
        app.state.gossip_task = asyncio.create_task(gossip_loop(app))
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
    # Initialize storage singleton for this node
    QueueStorage(config.node_id)
    # print(config.node_id)


    # Determine self URL (priority: env var set by run_server.py, else http://127.0.0.1:port assumed later)
    self_url = os.getenv("QUEUE_SELF_URL", "http://127.0.0.1:8000")
    app.state.membership = MembershipManager(self_url, seeds=config.peers)

    # Register routes from separate module
    register_routes(app)

    return app


def create_app_from_env() -> FastAPI:
    return create_app(load_config_from_env())

# Note: Removed module-level app instance to prevent premature initialization under uvicorn reload.
# Use factory style: uvicorn app.main:create_app_from_env --factory
