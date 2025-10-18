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
