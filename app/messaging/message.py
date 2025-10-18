from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional, Type, TypeVar
import time
import json
import random
from .constants import DEFAULT_TOPIC_PARTITIONS, NUM_SERVER_PARTITIONS

M = TypeVar("M", bound="Message")


@dataclass(slots=True)
class Message:
    """Represents a queue message.

    Fields:
        topic: logical grouping or stream name.
        key: partitioning or routing key (string identifier).
        value: payload (kept generic as Any for now).
        timestamp: epoch seconds float when message was created/received. Optional; defaults to current time if not provided.
    """
    topic: str
    value: Any
    key: Optional[str] = None
    timestamp: Optional[float] = field(default=None)

    def __post_init__(self) -> None:
        if self.timestamp is None:
            self.timestamp = time.time()
        if self.key is None:
            # Generate key in [1, DEFAULT_TOPIC_PARTITIONS]
            self.key = str(random.randint(1, DEFAULT_TOPIC_PARTITIONS))

    def server_partition(self) -> int:
        """Deterministically map this message's (topic,key) to a partition number in [0,1000].

        Uses SHA-256 for stability across process runs (unlike Python's built-in
        hash which is randomized per interpreter). Truncates via modulo.
        """
        import hashlib
        combo = f"{self.topic}:{self.key or ''}".encode("utf-8")
        digest = hashlib.sha256(combo).digest()
        num = int.from_bytes(digest[:8], "big")
        return num % NUM_SERVER_PARTITIONS

    def to_json(self) -> str:
        return json.dumps({
            "topic": self.topic,
            "key": self.key,
            "value": self.value,
            "timestamp": self.timestamp,
        })

    @classmethod
    def from_json(cls: Type[M], raw: str) -> M:
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON: {e}") from e
        if not isinstance(parsed, dict):
            raise ValueError("JSON must decode to an object")
        # key is now optional; if absent we will auto-generate
        missing = [f for f in ("topic", "value") if f not in parsed]
        if missing:
            raise ValueError(f"Missing required fields: {', '.join(missing)}")
        return cls(
            topic=str(parsed["topic"]),
            key=str(parsed["key"]) if "key" in parsed else None,
            value=parsed["value"],
            timestamp=parsed.get("timestamp"),
        )
