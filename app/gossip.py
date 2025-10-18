from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Set


@dataclass
class Member:
    url: str
    last_seen: float = field(default_factory=lambda: time.time())

    def touch(self) -> None:
        self.last_seen = time.time()


class MembershipManager:
    """Manages known cluster members and gossip merging.

    This is an eventually consistent view of the cluster. Gossip exchanges
    a set of member URLs; receivers merge the set updating last_seen.
    """

    def __init__(self, self_url: str):
        self.self_url = self_url.rstrip('/')
        self._members: Dict[str, Member] = {self.self_url: Member(self.self_url)}

    def members(self) -> List[str]:
        return list(self._members.keys())

    def add_or_touch(self, url: str) -> None:
        url = url.rstrip('/')
        if url in self._members:
            self._members[url].touch()
        else:
            self._members[url] = Member(url)

    def merge(self, urls: Iterable[str]) -> None:
        for u in urls:
            self.add_or_touch(u)

    def pick_peer(self) -> Optional[str]:
        # Choose a random peer excluding self
        peers = [u for u in self._members if u != self.self_url]
        if not peers:
            return None
        return random.choice(peers)

    def snapshot(self) -> Dict[str, float]:
        return {u: m.last_seen for u, m in self._members.items()}

    def prune(self, max_age: float) -> None:
        cutoff = time.time() - max_age
        # Keep self even if old
        for u in list(self._members.keys()):
            if u == self.self_url:
                continue
            if self._members[u].last_seen < cutoff:
                del self._members[u]


# Simple policy constants
DEFAULT_GOSSIP_INTERVAL = 10.0  # seconds
DEFAULT_PRUNE_AGE = 60.0  # seconds
