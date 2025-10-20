from __future__ import annotations

"""Cluster-level data migration helpers.

Triggered when a new node joins the membership set. We compare the previous
ring vs the new ring and identify partitions that moved away from this node.
For each such partition we backfill ONLY the latest segment (if any) and the
descriptor / consumer group offsets to the new owner. Historical segments are
not migrated (future enhancement).
"""

from typing import Sequence, List
from fastapi import FastAPI
from app.gossip import MembershipManager
from app.distribution.partition_ring import PartitionRing
from app.messaging.storage import QueueStorage


async def migrate_partitions_on_node_add(app: FastAPI, previous_members: Sequence[str], new_members: Sequence[str]) -> None:
    """Perform one-shot migration for partitions that this node lost after a node addition.

    Steps:
      1. Build old ring and new ring.
      2. For each partition whose owner changed from self -> other, iterate topics.
      3. For each topic: if descriptor_count>0 or segment exists, send latest segment (if any) then state.
      4. Use same de-dup flags as write-path migration: _migrated_segments / _migrated_states.

    Network errors are swallowed (best-effort)."""
    membership: MembershipManager = app.state.membership  # type: ignore[attr-defined]
    self_url = membership.self_url
    # Fast return if self not in previous or new (should not happen)
    if self_url not in previous_members or self_url not in new_members:
        return
    old_ring = PartitionRing(previous_members)
    new_ring = PartitionRing(new_members)

    # Compute partitions whose owner changed away from self
    lost_partitions: List[int] = []
    from app.messaging.constants import NUM_SERVER_PARTITIONS
    for p in range(NUM_SERVER_PARTITIONS):
        try:
            old_owner = old_ring.node_for_partition(p)
            new_owner = new_ring.node_for_partition(p)
        except Exception:
            continue
        if old_owner == self_url and new_owner != self_url:
            lost_partitions.append(p)
    if not lost_partitions:
        return

    storage = QueueStorage.get()
    import httpx
    processed: set[tuple[int, str]] = set()
    for partition in lost_partitions:
        new_owner = new_ring.node_for_partition(partition)
        topics = storage.list_topics(partition)
        for topic in topics:
            key = (partition, topic)
            if key in processed:
                continue  # safeguard against any accidental duplicate traversal
            processed.add(key)
            descriptor_count, offsets = storage.get_descriptor_and_offsets(partition, topic)
            segments = storage.list_segments(partition, topic)
            if descriptor_count == 0 and not segments:
                continue  # nothing to migrate
            # Migrate all segments in ascending order
            seg_set: set = getattr(app.state, "_migrated_segments", set())  # type: ignore[attr-defined]
            for seg_idx in segments:
                seg_flag = f"__migrated_segment_{partition}_{topic}_{seg_idx}"
                if seg_flag in seg_set:
                    continue
                msgs = storage.read_segment_messages(partition, topic, seg_idx)
                if msgs:
                    payload = {
                        "topic": topic,
                        "server_partition": partition,
                        "segment_index": seg_idx,
                        "messages": [{"key": m.key, "value": m.value} for m in msgs],
                    }
                    try:
                        httpx.post(f"{new_owner}/backfill_segment", json=payload, timeout=30.0)
                    except Exception:
                        pass
                seg_set.add(seg_flag)
            app.state._migrated_segments = seg_set  # type: ignore[attr-defined]
            # State migration
            state_flag = f"__migrated_state_{partition}_{topic}"
            state_set: set = getattr(app.state, "_migrated_states", set())  # type: ignore[attr-defined]
            if state_flag not in state_set:
                state_set.add(state_flag)  # mark before network call
                app.state._migrated_states = state_set  # type: ignore[attr-defined]
                payload_state = {
                    "topic": topic,
                    "server_partition": partition,
                    "descriptor_count": descriptor_count,
                    "consumer_offsets": offsets,
                }
                try:
                    httpx.post(f"{new_owner}/backfill_state", json=payload_state, timeout=10.0)
                except Exception:
                    pass
            else:
                app.state._migrated_states = state_set  # ensure persistence
