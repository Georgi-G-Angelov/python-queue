from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import Optional, Dict
from .message import Message


class QueueStorage:
    """Singleton storage manager for a node.

    On first initialization, ensures a directory named after the node id
    exists under the current working directory.
    """

    _instance_lock = threading.Lock()
    _instance: Optional["QueueStorage"] = None

    def __new__(cls, node_id: int):  # type: ignore[override]
        # Enforce singleton: reuse existing instance ignoring new node_id
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False  # type: ignore[attr-defined]
        return cls._instance

    def __init__(self, node_id: int):  # idempotent due to singleton check
        if getattr(self, "_initialized", False):  # type: ignore[attr-defined]
            return
        self.node_id = node_id
        self.base_dir = Path(os.getcwd()) / str(node_id)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        # Change process working directory to this node's base directory
        os.chdir(self.base_dir)
        # Caches to avoid redundant disk existence checks
        self._partition_dirs: Dict[int, Path] = {}
        self._topic_dirs: Dict[tuple[int, str], Path] = {}
        self._initialized = True  # type: ignore[attr-defined]

    @classmethod
    def get(cls) -> "QueueStorage":
        if cls._instance is None:
            raise RuntimeError("QueueStorage not initialized. Call QueueStorage(node_id) first.")
        return cls._instance

    def path_for(self, relative: str) -> Path:
        return self.base_dir / relative

    @classmethod
    def reset_for_tests(cls) -> None:
        """Reset singleton (test-only helper)."""
        with cls._instance_lock:
            cls._instance = None

    def write_message(self, message: Message) -> Path:
        """Persist a message to disk using partition/topic segmentation.

        Layout (relative to node base dir / CWD after init):
        <partition>/<topic>/descriptor.txt
        <partition>/<topic>/<segment_file>

    descriptor.txt stores the TOTAL number of messages recorded for that (partition, topic); it is rewritten
    on every append. We treat a non-integer descriptor as corruption and reset count to 0 before incrementing.

    Segment file naming: integer division by 100 groups messages into fixed-size buckets to keep file sizes
    manageable and enable future compaction or indexing. (Messages 1..100 => segment '0', 101..200 => '1', etc.)
    The current implementation appends newline-delimited JSON (NDJSON) without pretty formatting for space efficiency.

        Returns the path to the segment file the message was written to.
        """
        partition = message.server_partition()

        # Ensure partition directory
        part_dir = self._partition_dirs.get(partition)
        if part_dir is None:
            part_dir = Path(str(partition))
            part_dir.mkdir(parents=True, exist_ok=True)
            self._partition_dirs[partition] = part_dir

        # Ensure topic directory under partition
        topic_key = (partition, message.topic)
        topic_dir = self._topic_dirs.get(topic_key)
        if topic_dir is None:
            topic_dir = part_dir / message.topic
            topic_dir.mkdir(parents=True, exist_ok=True)
            self._topic_dirs[topic_key] = topic_dir

        descriptor_path = topic_dir / "descriptor.txt"
        if not descriptor_path.exists():
            num_messages = 1
            descriptor_path.write_text(str(num_messages), encoding="utf-8")
        else:
            # Read, increment
            try:
                current_raw = descriptor_path.read_text(encoding="utf-8").strip()
                current_val = int(current_raw) if current_raw else 0
            except ValueError:
                # Corruption fallback: treat as zero
                current_val = 0
            num_messages = current_val + 1
            descriptor_path.write_text(str(num_messages), encoding="utf-8")

        # Segment file determined by integer division by 100
        segment_index = (num_messages - 1) // 100  # so messages 1-100 => 0, 101-200 => 1
        segment_file = topic_dir / f"{segment_index}"

        # Append serialized JSON on its own line
        with segment_file.open("a", encoding="utf-8") as fh:
            fh.write(message.to_json())
            fh.write("\n")

        return segment_file
