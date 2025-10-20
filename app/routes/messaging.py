from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query, Body
import random
from app.messaging.constants import DEFAULT_TOPIC_PARTITIONS
from app.messaging.message import Message
from app.messaging.storage import QueueStorage
from app.gossip import MembershipManager
from app.distribution.partition_ring import PartitionRing
from typing import Dict, Tuple
import threading


def register_messaging_routes(app: FastAPI) -> None:
    # Initialize owner cache and migration locks if not present
    if not hasattr(app.state, "partition_owner_cache"):
        app.state.partition_owner_cache = {}  # type: ignore[attr-defined]
    if not hasattr(app.state, "migration_locks"):
        app.state.migration_locks = {}  # type: ignore[attr-defined]

    def _get_migration_lock(partition: int, topic: str) -> threading.Lock:
        key = (partition, topic)
        locks: Dict[Tuple[int, str], threading.Lock] = app.state.migration_locks  # type: ignore[attr-defined]
        lock = locks.get(key)
        if lock is None:
            lock = threading.Lock()
            locks[key] = lock
        return lock

    def _maybe_migrate_partition(partition: int, topic: str, old_owner: str, new_owner: str) -> None:
        """Backfill latest segment and state for a topic that moved to another owner.

        Only migrates the single latest segment (if any) plus descriptor/offset files.
        Executed under per (partition, topic) lock to avoid duplicate migrations.
        """
        lock = _get_migration_lock(partition, topic)
        if not lock.acquire(blocking=False):
            return  # migration in progress elsewhere
        try:
            storage = QueueStorage.get()
            # Introspect topic state
            segments = storage.list_segments(partition, topic)
            descriptor_count, offsets = storage.get_descriptor_and_offsets(partition, topic)
            import httpx
            # Migrate all segments (full historical migration)
            migrated_set: set = getattr(app.state, "_migrated_segments", set())  # type: ignore[attr-defined]
            for seg_idx in segments:
                migrated_flag = f"__migrated_segment_{partition}_{topic}_{seg_idx}"
                if migrated_flag in migrated_set:
                    continue
                msgs = storage.read_segment_messages(partition, topic, seg_idx)
                if msgs:
                    seg_payload = {
                        "topic": topic,
                        "server_partition": partition,
                        "segment_index": seg_idx,
                        "messages": [{"key": m.key, "value": m.value} for m in msgs],
                    }
                    try:
                        httpx.post(f"{new_owner}/backfill_segment", json=seg_payload, timeout=30.0)
                    except Exception:
                        pass
                migrated_set.add(migrated_flag)
            app.state._migrated_segments = migrated_set  # type: ignore[attr-defined]
            # Backfill state (descriptor + offsets)
            state_flag = f"__migrated_state_{partition}_{topic}"
            migrated_state: set = getattr(app.state, "_migrated_states", set())  # type: ignore[attr-defined]
            if state_flag not in migrated_state:
                # Mark before network call to avoid race counting duplicates
                migrated_state.add(state_flag)
                app.state._migrated_states = migrated_state  # type: ignore[attr-defined]
                state_payload = {
                    "topic": topic,
                    "server_partition": partition,
                    "descriptor_count": descriptor_count,
                    "consumer_offsets": offsets,
                }
                try:
                    httpx.post(f"{new_owner}/backfill_state", json=state_payload, timeout=10.0)
                except Exception:
                    pass
            else:
                app.state._migrated_states = migrated_state  # ensure attribute persists
        finally:
            lock.release()
    @app.post("/post_message", tags=["messaging"])
    async def post_message(payload: dict = Body(...)) -> dict[str, object]:
        """Publish a message.

        JSON body fields:
          topic: str (required)
          key: str | int (optional -> auto-assigned random int in [0, DEFAULT_TOPIC_PARTITIONS-1])
          value: any (required)

        Behavior:
          1. Ensure required fields present (topic, value).
          2. If key missing, assign random integer key within topic partition range.
          3. Construct Message, compute server partition.
          4. Determine partition owner via PartitionRing; if local, write and return metadata; else reroute POST to owner.
        """
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Payload must be a JSON object")
        if "topic" not in payload or "value" not in payload:
            raise HTTPException(status_code=400, detail="Missing required fields: topic,value")
        topic = str(payload["topic"])
        key_val = payload.get("key")
        if key_val is None:
            key_val = str(random.randint(0, DEFAULT_TOPIC_PARTITIONS - 1))
        else:
            key_val = str(key_val)
        value = payload["value"]

        msg = Message(topic=topic, key=key_val, value=value)
        partition = msg.server_partition()

        membership: MembershipManager = app.state.membership  # type: ignore[attr-defined]
        nodes = membership.members()
        ring = PartitionRing(nodes)
        self_url = membership.self_url if hasattr(membership, "self_url") else nodes[0]
        owner = ring.node_for_partition(partition)

        cache: Dict[int, str] = app.state.partition_owner_cache  # type: ignore[attr-defined]
        previous_owner = cache.get(partition, self_url)
        # Detect ownership change: cached owner was self but ring assigns different owner now
        if previous_owner == self_url and owner != self_url:
            # Migrate each topic under this partition (latest segment only)
            storage = QueueStorage.get()
            for t in storage.list_topics(partition):
                d_count, _ = storage.get_descriptor_and_offsets(partition, t)
                latest_seg = storage.latest_segment_index(partition, t)
                if d_count == 0 and latest_seg is None:
                    continue  # skip empty topic directories
                _maybe_migrate_partition(partition, t, previous_owner, owner)
        # Update cache after potential migration
        cache[partition] = owner

        if owner == self_url:
            storage = QueueStorage.get()
            storage.write_message(msg)
            return {"node": self_url, "partition": partition, "key": key_val, "topic": topic, "written": True}

        import httpx
        try:
            resp = httpx.post(f"{owner}/post_message", json={"topic": topic, "key": key_val, "value": value}, timeout=5.0)
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Failed to reach owner {owner}: {e}") from e
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=f"Upstream error from {owner}: {resp.text}")
        upstream = resp.json()
        return upstream

    @app.get("/read_message", tags=["messaging"])
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
                f"{owner}/read_message",
                params={"topic": topic, "key": key, "consumer_group": consumer_group},
                timeout=5.0,
            )
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Failed to reach owner {owner}: {e}") from e
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=f"Upstream error from {owner}: {resp.text}")
        upstream = resp.json()
        return {"node": upstream.get("node", owner), "partition": partition, "message": upstream.get("message")}