from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import Optional, Dict
from .message import Message
from .constants import MESSAGES_PER_SEGMENT


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
        # Topic state: (partition, topic) -> { 'dir': Path, 'count': int, 'descriptor': file handle }
        self._topic_state: Dict[tuple[int, str], dict] = {}
        # Per (partition, topic) locks to serialize operations within each topic
        self._topic_locks: Dict[tuple[int, str], threading.Lock] = {}
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
        """Persist a message to disk using partition/topic segmentation (append-then-descriptor update).

        Layout (relative to node base dir / CWD after init):
          <partition>/<topic>/descriptor.txt
          <partition>/<topic>/<segment_file>

        Atomicity model:
          1. Determine segment from current count (pre-increment).
          2. Append message line to segment file.
          3. If append succeeds, increment in-memory count and rewrite descriptor.txt.
          4. If append fails (exception), descriptor and in-memory count remain unchanged (rollback implicit).

        This ordering prevents descriptor.txt from advertising a message that was never fully appended if an error
        occurs while writing the segment file. (Previous implementation incremented descriptor first and could drift.)

        descriptor.txt stores the TOTAL number of successfully recorded messages for that (partition, topic); it is rewritten
        on every successful append. A non-integer descriptor at initialization is treated as corruption -> count resets to 0.

        Segment file naming: integer division by MESSAGES_PER_SEGMENT groups messages into fixed-size buckets to keep file sizes
        manageable and enable future compaction or indexing. (Messages with zero-based index i go to segment i // MESSAGES_PER_SEGMENT.)
        We append newline-delimited JSON (NDJSON) without pretty formatting for space efficiency.

        Returns the path to the segment file the message was written to.
        """
        partition = message.server_partition()
        topic_key = (partition, message.topic)
        lock = self._get_topic_lock(topic_key)
        lock.acquire()
        try:
            # Ensure partition directory via helper
            part_dir = self._ensure_partition_dir(partition)

            # Ensure topic directory under partition via helper
            topic_dir = self._ensure_topic_dir(part_dir, topic_key, message.topic)

            # Obtain or initialize topic state via helper
            state = self._get_or_init_topic_state(topic_key, topic_dir)

            # Current count BEFORE writing new message
            current_count = state["count"]
            # Determine segment index based on zero-based message index (current_count)
            segment_index = current_count // MESSAGES_PER_SEGMENT
            segment_file = topic_dir / f"{segment_index}"

            # Attempt to append message first; if this fails we do NOT update descriptor/count.
            with segment_file.open("a", encoding="utf-8") as fh:
                fh.write(message.to_json())
                fh.write("\n")

            # Append succeeded: update in-memory count and descriptor (descriptor reflects successful writes only)
            state["count"] = current_count + 1
            descriptor_file = state["descriptor"]
            descriptor_file.seek(0)
            descriptor_file.truncate(0)
            descriptor_file.write(str(state["count"]))
            descriptor_file.flush()

            return segment_file
        finally:
            lock.release()

    def write_message_at_segment(self, message: Message, segment_index: int) -> Path:
        """Persist a message to a specific segment file instead of computing segment from count.

        Use cases: backfilling, replay import, or deterministic replay where caller already batched messages
        by segment. This method still ensures descriptor consistency and locking.

        Rules / Validation:
          - segment_index must be >= 0.
          - Writes always append; if chosen segment is 'behind' current progression (i.e. segment_index < current_count // MESSAGES_PER_SEGMENT)
            we still append to the specified segment (allowing retroactive insertion) but descriptor will increase and may create a hole.
          - It is caller's responsibility to avoid creating semantic ordering gaps; consumer offset reading remains sequential by message index,
            so retroactive insertion in an earlier segment after later segments exist will not be visible to consumers that already passed that offset.

        Returns the path to the segment file used.
        """
        if segment_index < 0:
            raise ValueError("segment_index must be non-negative")
        partition = message.server_partition()
        topic_key = (partition, message.topic)
        lock = self._get_topic_lock(topic_key)
        lock.acquire()
        try:
            part_dir = self._ensure_partition_dir(partition)
            topic_dir = self._ensure_topic_dir(part_dir, topic_key, message.topic)
            state = self._get_or_init_topic_state(topic_key, topic_dir)

            current_count = state["count"]
            segment_file = topic_dir / f"{segment_index}"

            with segment_file.open("a", encoding="utf-8") as fh:
                fh.write(message.to_json())
                fh.write("\n")

            # After append update count and descriptor (we consider message appended at new sequential index current_count)
            state["count"] = current_count + 1
            descriptor_file = state["descriptor"]
            descriptor_file.seek(0)
            descriptor_file.truncate(0)
            descriptor_file.write(str(state["count"]))
            descriptor_file.flush()
            return segment_file
        finally:
            lock.release()

    def read_message(self, partition: int, topic: str, consumer_group: str) -> Optional[Message]:
        """Return the next message for a consumer group in a partition/topic or None if exhausted.

        Tracks progress per (partition, topic, consumer_group) via a small offset file:
        <partition>/<topic>/consumer_group_<consumer_group>
        containing an integer count of messages already consumed.

        Assumption: caller supplies the topic explicitly. Original request omitted a topic
        parameter; without it we cannot locate correct segment files because storage layout
        is partition/topic based. If desired we could iterate topics under a partition, but
        that is not implemented here for performance and clarity.
        """
        topic_key = (partition, topic)
        lock = self._get_topic_lock(topic_key)
        lock.acquire()
        try:
            # Ensure partition & topic directories exist; if topic doesn't exist yet -> no messages
            part_dir = self._partition_dirs.get(partition)
            if part_dir is None:
                return None
            topic_dir = self._topic_dirs.get(topic_key)
            if topic_dir is None:
                return None

            # Load topic state to get total count (may need initialization from descriptor file)
            state = self._get_or_init_topic_state(topic_key, topic_dir)
            total = state["count"]

            # Consumer offset file
            cg_file = topic_dir / f"consumer_group_{consumer_group}"
            if cg_file.exists():
                try:
                    raw = cg_file.read_text(encoding="utf-8").strip()
                    offset = int(raw) if raw else 0
                except ValueError:
                    offset = 0
            else:
                offset = 0
                cg_file.write_text("0", encoding="utf-8")

            if offset >= total:
                return None  # nothing new

            # Determine segment and line index
            segment_index = offset // MESSAGES_PER_SEGMENT
            line_index = offset % MESSAGES_PER_SEGMENT
            segment_file = topic_dir / f"{segment_index}"
            if not segment_file.exists():
                # Unexpected missing file; treat as no message
                return None

            lines = segment_file.read_text(encoding="utf-8").splitlines()
            if line_index >= len(lines):
                # Segment file shorter than expected; treat as no message
                return None

            raw_line = lines[line_index]
            try:
                msg = Message.from_json(raw_line)
            except Exception:
                # Corrupted line: skip (increment offset anyway to avoid infinite loop)
                msg = None

            # Persist incremented offset
            new_offset = offset + 1
            cg_file.write_text(str(new_offset), encoding="utf-8")

            return msg
        finally:
            lock.release()

    def set_topic_state(self, partition: int, topic: str, descriptor_count: int, consumer_offsets: Dict[str, int]) -> None:
        """Backfill descriptor count and consumer group offset files for a (partition, topic).

        This does NOT create or validate segment files; caller must ensure segment files already exist that
        correspond to descriptor_count messages. Offsets greater than descriptor_count are clamped to descriptor_count.
        """
        if descriptor_count < 0:
            raise ValueError("descriptor_count must be non-negative")
        topic_key = (partition, topic)
        lock = self._get_topic_lock(topic_key)
        lock.acquire()
        try:
            part_dir = self._ensure_partition_dir(partition)
            topic_dir = self._ensure_topic_dir(part_dir, topic_key, topic)
            state = self._get_or_init_topic_state(topic_key, topic_dir)

            # Update descriptor count in-memory and file
            state["count"] = descriptor_count
            descriptor_file = state["descriptor"]
            descriptor_file.seek(0)
            descriptor_file.truncate(0)
            descriptor_file.write(str(descriptor_count))
            descriptor_file.flush()

            # Write consumer offsets
            for cg, off in consumer_offsets.items():
                safe_off = off if off <= descriptor_count else descriptor_count
                cg_file = topic_dir / f"consumer_group_{cg}"
                cg_file.write_text(str(safe_off), encoding="utf-8")
        finally:
            lock.release()

    # Introspection helpers for migration/backfill
    def list_topics(self, partition: int) -> list[str]:
        part_dir = self._partition_dirs.get(partition)
        if part_dir is None:
            part_dir = Path(str(partition))
            if not part_dir.exists():
                return []
        topics = []
        for child in part_dir.iterdir():
            if child.is_dir():
                topics.append(child.name)
        return topics

    def list_segments(self, partition: int, topic: str) -> list[int]:
        topic_dir = self._topic_dirs.get((partition, topic))
        if topic_dir is None:
            topic_dir = Path(str(partition)) / topic
            if not topic_dir.exists():
                return []
        indices = []
        for child in topic_dir.iterdir():
            if child.is_file() and child.name.isdigit():
                try:
                    indices.append(int(child.name))
                except ValueError:
                    continue
        return sorted(indices)

    def latest_segment_index(self, partition: int, topic: str) -> Optional[int]:
        topic_dir = self._topic_dirs.get((partition, topic))
        if topic_dir is None:
            topic_dir = Path(str(partition)) / topic
            if not topic_dir.exists():
                return None
        max_idx: Optional[int] = None
        for child in topic_dir.iterdir():
            if child.is_file() and child.name.isdigit():
                idx = int(child.name)
                if max_idx is None or idx > max_idx:
                    max_idx = idx
        return max_idx

    def read_segment_messages(self, partition: int, topic: str, segment_index: int) -> list[Message]:
        topic_dir = self._topic_dirs.get((partition, topic))
        if topic_dir is None:
            topic_dir = Path(str(partition)) / topic
            if not topic_dir.exists():
                return []
        segment_file = topic_dir / str(segment_index)
        if not segment_file.exists():
            return []
        msgs: list[Message] = []
        for line in segment_file.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                msgs.append(Message.from_json(line))
            except Exception:
                # Skip corrupted lines
                continue
        return msgs

    def get_descriptor_and_offsets(self, partition: int, topic: str) -> tuple[int, Dict[str, int]]:
        topic_dir = self._topic_dirs.get((partition, topic))
        if topic_dir is None:
            topic_dir = Path(str(partition)) / topic
            if not topic_dir.exists():
                return 0, {}
        descriptor_path = topic_dir / "descriptor.txt"
        if descriptor_path.exists():
            try:
                raw = descriptor_path.read_text(encoding="utf-8").strip()
                descriptor_count = int(raw) if raw else 0
            except ValueError:
                descriptor_count = 0
        else:
            descriptor_count = 0
        offsets: Dict[str, int] = {}
        for child in topic_dir.iterdir():
            if child.is_file() and child.name.startswith("consumer_group_"):
                cg = child.name[len("consumer_group_"):]
                try:
                    raw = child.read_text(encoding="utf-8").strip()
                    offsets[cg] = int(raw) if raw else 0
                except ValueError:
                    offsets[cg] = 0
        return descriptor_count, offsets

    def close_all(self) -> None:
        """Close all open descriptor file handles (useful for shutdown/tests)."""
        for state in self._topic_state.values():
            try:
                state["descriptor"].close()
            except Exception:
                pass

    # Internal helpers
    def _ensure_partition_dir(self, partition: int) -> Path:
        """Return Path for partition directory, creating and caching if needed."""
        part_dir = self._partition_dirs.get(partition)
        if part_dir is None:
            part_dir = Path(str(partition))
            part_dir.mkdir(parents=True, exist_ok=True)
            self._partition_dirs[partition] = part_dir
        return part_dir

    def _ensure_topic_dir(self, part_dir: Path, topic_key: tuple[int, str], topic: str) -> Path:
        """Return Path for topic directory within a partition, creating and caching if needed."""
        topic_dir = self._topic_dirs.get(topic_key)
        if topic_dir is None:
            topic_dir = part_dir / topic
            topic_dir.mkdir(parents=True, exist_ok=True)
            self._topic_dirs[topic_key] = topic_dir
        return topic_dir

    def _get_or_init_topic_state(self, topic_key: tuple[int, str], topic_dir: Path) -> dict:
        """Return existing topic state or initialize it (opens descriptor and reads count)."""
        state = self._topic_state.get(topic_key)
        if state is not None:
            return state
        descriptor_path = topic_dir / "descriptor.txt"
        if descriptor_path.exists():
            try:
                raw = descriptor_path.read_text(encoding="utf-8").strip()
                count = int(raw) if raw else 0
            except ValueError:
                count = 0
        else:
            count = 0
            descriptor_path.write_text("0", encoding="utf-8")
        descriptor_file = descriptor_path.open("r+", encoding="utf-8")
        state = {"dir": topic_dir, "count": count, "descriptor": descriptor_file, "path": descriptor_path}
        self._topic_state[topic_key] = state
        return state

    def _get_topic_lock(self, topic_key: tuple[int, str]) -> threading.Lock:
        """Return a per (partition, topic) lock, creating it if necessary."""
        lock = self._topic_locks.get(topic_key)
        if lock is None:
            with self._instance_lock:
                lock = self._topic_locks.get(topic_key)
                if lock is None:
                    lock = threading.Lock()
                    self._topic_locks[topic_key] = lock
        return lock
