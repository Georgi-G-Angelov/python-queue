from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query
from app.messaging.message import Message
from app.messaging.storage import QueueStorage
from app.gossip import MembershipManager
from app.distribution.partition_ring import PartitionRing


def register_messaging_routes(app: FastAPI) -> None:
    @app.get("/message", tags=["messaging"])
    async def get_message(
        topic: str = Query(...),
        key: str = Query(...),
        consumer_group: str = Query(...),
    ) -> dict[str, object]:
        """Fetch next message for a (topic,key,consumer_group) triple.

        Payload JSON fields:
          topic: str (required)
          key: str (required)
          consumer_group: str (required)

        Behavior:
          1. Compute server partition from (topic,key) using Message helper.
          2. Build PartitionRing for current membership; check if this node owns the partition.
          3. If owner: read next message via storage and return {"node": self, "message": <msg or None>}.
          4. If not owner: proxy by performing internal HTTP request to owner node and return its response.

        NOTE: For simplicity the reroute performs a synchronous HTTP GET to the peer. In production you may
        want client pooling, retries, and timeout handling.
        """
        # Compute partition using a temporary Message (value unused)
        tmp_msg = Message(topic=topic, key=key, value=None)
        partition = tmp_msg.server_partition()

        membership: MembershipManager = app.state.membership  # type: ignore[attr-defined]
        nodes = membership.members()
        ring = PartitionRing(nodes)
        self_url = membership.self_url if hasattr(membership, "self_url") else nodes[0]
        owner = ring.node_for_partition(partition)

        if owner == self_url:
            storage = QueueStorage.get()
            msg = storage.read_message(partition, topic, consumer_group)
            return {"node": self_url, "partition": partition, "message": msg.to_json() if msg else None}

        # Reroute: simple client request
        import httpx
        try:
            resp = httpx.get(
                f"{owner}/message",
                params={"topic": topic, "key": key, "consumer_group": consumer_group},
                timeout=5.0,
            )
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Failed to reach owner {owner}: {e}") from e
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=f"Upstream error from {owner}: {resp.text}")
        upstream = resp.json()
        return {"node": upstream.get("node", owner), "partition": partition, "message": upstream.get("message")}