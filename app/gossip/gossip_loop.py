from __future__ import annotations

import asyncio
import os
import httpx
from fastapi import FastAPI
from .membership import MembershipManager, DEFAULT_GOSSIP_INTERVAL, DEFAULT_PRUNE_AGE


async def gossip_loop(app: FastAPI) -> None:
    """Periodic gossip task.

    On each tick: prune stale members, pick a peer, send snapshot.
    Environment variables control interval & prune age.
    """
    membership: MembershipManager = app.state.membership  # type: ignore[attr-defined]
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
            await asyncio.sleep(interval)
