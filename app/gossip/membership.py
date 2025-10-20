from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence


@dataclass
class Member:
    url: str
    last_seen: float = field(default_factory=lambda: time.time())

    def touch(self) -> None:
        self.last_seen = time.time()


class MembershipManager:
    """Manages known cluster members and gossip merging.

    Eventually consistent membership with timestamps.
    """

    def __init__(self, self_url: str, seeds: Optional[Sequence[str]] = None):
        self.self_url = self_url.rstrip('/')
        self._members: Dict[str, Member] = {self.self_url: Member(self.self_url)}
        self._seeds: List[str] = []
        self._initial_gossip_done = False
        # Track nodes whose addition-trigger migration has already been processed
        self._seen_added_nodes: set[str] = set()
        if seeds:
            for s in seeds:
                s_norm = s.rstrip('/')
                if s_norm != self.self_url:
                    self._seeds.append(s_norm)
                    if s_norm not in self._members:
                        self._members[s_norm] = Member(s_norm)

    def members(self) -> List[str]:
        return list(self._members.keys())

    def apply_snapshot(self, snapshot: Dict[str, float], sender: Optional[str]) -> None:
        """Merge incoming snapshot and detect node additions.

        If one or more new nodes are discovered (i.e. were absent in previous membership),
        schedule an asynchronous migration task to backfill partitions that moved away
        from this node due to ring rebalance. Only latest segment + state are migrated.
        """
        previous_members = set(self._members.keys())  # capture before merge
        now = time.time()
        for url, ts in snapshot.items():
            url_norm = url.rstrip('/')
            existing = self._members.get(url_norm)
            if existing is None:
                self._members[url_norm] = Member(url_norm)
                self._members[url_norm].last_seen = ts
            else:
                if ts > existing.last_seen:
                    existing.last_seen = ts
                if existing.url == self.self_url:
                    existing.last_seen = now
        if sender:
            sender_norm = sender.rstrip('/')
            if sender_norm in self._members:
                self._members[sender_norm].last_seen = now
            else:
                self._members[sender_norm] = Member(sender_norm)
                self._members[sender_norm].last_seen = now
        # Detect additions
        new_members = set(self._members.keys())
        added = new_members - previous_members
        # Only trigger if there is at least one truly new node not processed before
        truly_new = {a for a in added if a not in self._seen_added_nodes}
        if truly_new:
            self._seen_added_nodes.update(truly_new)
            # Fire async migration task (best-effort). Import inside to avoid cycle.
            try:
                import asyncio
                from fastapi import FastAPI
                from app.distribution.data_migration import migrate_partitions_on_node_add
                # Attempt to locate global FastAPI app via running tasks; we rely on caller attaching it via closure/state.
                # Expect apply_snapshot invoked from an endpoint with 'app' available as sender context.
                # We'll store a reference on self the first time we see one (caller should set self.app externally).
                app: FastAPI | None = getattr(self, "_app_ref", None)  # type: ignore[attr-defined]
                if app is not None:
                    try:
                        loop = asyncio.get_running_loop()
                    except RuntimeError:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                    loop.create_task(
                        migrate_partitions_on_node_add(app, list(previous_members), list(new_members))
                    )
            except Exception:
                # Swallow errors: migration is best-effort
                pass

    def candidate_peers(self) -> List[str]:
        return list(set(self._seeds) | {u for u in self._members if u != self.self_url})

    def pick_peer(self) -> Optional[str]:
        if not self._initial_gossip_done and self._seeds:
            candidates = set(self._seeds)
        else:
            candidates = {u for u in self._members if u != self.self_url}
        if not candidates:
            return None
        choice = random.choice(list(candidates))
        if not self._initial_gossip_done:
            self._initial_gossip_done = True
        return choice

    def snapshot(self) -> Dict[str, float]:
        return {u: m.last_seen for u, m in self._members.items()}

    def prune(self, max_age: float) -> None:
        cutoff = time.time() - max_age
        for u in list(self._members.keys()):
            if u == self.self_url:
                self._members[u].last_seen = time.time()
                continue
            if self._members[u].last_seen < cutoff:
                del self._members[u]


DEFAULT_GOSSIP_INTERVAL = 2.0
DEFAULT_PRUNE_AGE = 20.0
