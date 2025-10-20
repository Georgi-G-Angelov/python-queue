from __future__ import annotations

from fastapi import FastAPI, HTTPException
from app.messaging.storage import QueueStorage
from app.messaging.message import Message
from app.distribution.partition_ring import PartitionRing
from app.gossip import MembershipManager
from typing import List, Dict, Any


SEGMENT_MESSAGES_FIELD = "messages"  # list of message objects


def register_backfill_routes(app: FastAPI) -> None:
    @app.post("/backfill_segment", tags=["backfill"])
    async def backfill_segment(payload: Dict[str, Any]) -> Dict[str, Any]:
        """Backfill a full segment for a topic.

                Body fields:
                    topic: str (required)
                    server_partition: int (required) partition this segment belongs to
                    segment_index: int (required, >=0)
                    messages: list[object] (required) each must at least have 'value' and optional 'key' (if absent auto-generated)

        Behavior:
          1. Validate input.
          2. Determine partition from first message (using a temporary Message with first msg key or generated key).
             NOTE: All messages in batch MUST hash to same partition via (topic,key); if not we raise 400.
          3. Build PartitionRing; if local node owns partition -> write all messages using write_message_at_segment.
             - Messages are appended sequentially; descriptor count increases per message.
          4. If not owner -> reroute entire payload via POST to owner /backfill_segment and return its response.

        Returns JSON with { node, partition, segment_index, count_written }.
        """
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Payload must be a JSON object")
        missing = [f for f in ("topic", "server_partition", "segment_index", SEGMENT_MESSAGES_FIELD) if f not in payload]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {', '.join(missing)}")
        topic = str(payload["topic"])
        # Validate server_partition
        try:
            declared_partition = int(payload["server_partition"])
        except Exception:
            raise HTTPException(status_code=400, detail="server_partition must be an integer")
        try:
            segment_index = int(payload["segment_index"])
        except Exception:
            raise HTTPException(status_code=400, detail="segment_index must be an integer")
        if segment_index < 0:
            raise HTTPException(status_code=400, detail="segment_index must be non-negative")
        raw_messages = payload.get(SEGMENT_MESSAGES_FIELD)
        if not isinstance(raw_messages, list) or len(raw_messages) == 0:
            raise HTTPException(status_code=400, detail="messages must be a non-empty list")

        # Prepare Message objects and validate partition consistency
        msgs: List[Message] = []
        first_partition = None
        for entry in raw_messages:
            if not isinstance(entry, dict):
                raise HTTPException(status_code=400, detail="Each message entry must be an object")
            if "value" not in entry:
                raise HTTPException(status_code=400, detail="Each message requires a 'value' field")
            key = entry.get("key")
            msg = Message(topic=topic, key=str(key) if key is not None else None, value=entry["value"])
            part = msg.server_partition()
            if first_partition is None:
                first_partition = part
            elif part != first_partition:
                raise HTTPException(status_code=400, detail="All messages must hash to the same partition")
            msgs.append(msg)

        # Validate declared partition matches computed partition from messages
        if first_partition is None:
            raise HTTPException(status_code=400, detail="Unable to determine partition from messages")
        if declared_partition != first_partition:
            raise HTTPException(status_code=400, detail="Declared server_partition does not match messages' partition")

        # Partition ring and ownership check
        membership: MembershipManager = app.state.membership  # type: ignore[attr-defined]
        nodes = membership.members()
        ring = PartitionRing(nodes)
        self_url = membership.self_url if hasattr(membership, "self_url") else nodes[0]
        owner = ring.node_for_partition(first_partition)

        if owner != self_url:
            # Reroute entire payload
            import httpx
            try:
                resp = httpx.post(f"{owner}/backfill_segment", json=payload, timeout=30.0)
            except Exception as e:
                raise HTTPException(status_code=502, detail=f"Failed to reach owner {owner}: {e}") from e
            if resp.status_code != 200:
                raise HTTPException(status_code=resp.status_code, detail=f"Upstream error from {owner}: {resp.text}")
            return resp.json()

        storage = QueueStorage.get()
        for m in msgs:
            storage.write_message_at_segment(m, segment_index=segment_index)
        return {
            "node": self_url,
            "partition": first_partition,
            "segment_index": segment_index,
            "count_written": len(msgs),
        }

    @app.post("/backfill_state", tags=["backfill"])
    async def backfill_state(payload: Dict[str, Any]) -> Dict[str, Any]:
        """Backfill descriptor count and consumer group offsets for a partition/topic.

        Body fields:
          topic: str (required)
          server_partition: int (required)
          descriptor_count: int (required, >=0)
          consumer_offsets: object mapping consumer_group -> int (optional, defaults empty)

        Validation: descriptor_count >= 0; all offsets >=0; offsets > descriptor_count are clamped.
        Reroutes to owning node if this node does not own server_partition.
        """
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Payload must be a JSON object")
        missing = [f for f in ("topic", "server_partition", "descriptor_count") if f not in payload]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {', '.join(missing)}")
        topic = str(payload["topic"])
        try:
            server_partition = int(payload["server_partition"])
        except Exception:
            raise HTTPException(status_code=400, detail="server_partition must be an integer")
        try:
            descriptor_count = int(payload["descriptor_count"])
        except Exception:
            raise HTTPException(status_code=400, detail="descriptor_count must be an integer")
        if descriptor_count < 0:
            raise HTTPException(status_code=400, detail="descriptor_count must be non-negative")
        raw_offsets = payload.get("consumer_offsets", {})
        if not isinstance(raw_offsets, dict):
            raise HTTPException(status_code=400, detail="consumer_offsets must be an object")
        consumer_offsets: Dict[str, int] = {}
        for cg, off in raw_offsets.items():
            if not isinstance(cg, str):
                raise HTTPException(status_code=400, detail="consumer group names must be strings")
            try:
                off_val = int(off)
            except Exception:
                raise HTTPException(status_code=400, detail=f"Offset for consumer group {cg} must be an integer")
            if off_val < 0:
                raise HTTPException(status_code=400, detail=f"Offset for consumer group {cg} must be non-negative")
            consumer_offsets[cg] = off_val

        # Ownership check
        membership: MembershipManager = app.state.membership  # type: ignore[attr-defined]
        nodes = membership.members()
        ring = PartitionRing(nodes)
        self_url = membership.self_url if hasattr(membership, "self_url") else nodes[0]
        owner = ring.node_for_partition(server_partition)
        if owner != self_url:
            import httpx
            try:
                resp = httpx.post(f"{owner}/backfill_state", json=payload, timeout=10.0)
            except Exception as e:
                raise HTTPException(status_code=502, detail=f"Failed to reach owner {owner}: {e}") from e
            if resp.status_code != 200:
                raise HTTPException(status_code=resp.status_code, detail=f"Upstream error from {owner}: {resp.text}")
            return resp.json()

        storage = QueueStorage.get()
        try:
            storage.set_topic_state(server_partition, topic, descriptor_count, consumer_offsets)
        except ValueError as ve:
            raise HTTPException(status_code=400, detail=str(ve)) from ve
        return {
            "node": self_url,
            "partition": server_partition,
            "descriptor_count": descriptor_count,
            "consumer_groups": list(consumer_offsets.keys()),
        }
