import os
from pathlib import Path
import pytest
from app.messaging.storage import QueueStorage
from app.messaging import Message
from app.messaging.constants import MESSAGES_PER_SEGMENT

class InjectedFailure(Exception):
    pass

def test_segment_write_failure_rolls_back_descriptor(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    QueueStorage.reset_for_tests()
    qs = QueueStorage(21)

    # Write an initial message successfully
    m1 = Message(topic="roll", value={"a": 1}, key="k")
    qs.write_message(m1)
    partition = m1.server_partition()
    topic_dir = Path(str(partition)) / m1.topic
    descriptor = topic_dir / "descriptor.txt"
    assert descriptor.read_text(encoding="utf-8").strip() == "1"

    # Monkeypatch Path.open to raise for segment file append only on next call
    original_open = Path.open

    def failing_open(self, mode="r", *args, **kwargs):
        # Only fail when opening the current segment for append ('a') for the target topic
        if self == topic_dir / "0" and "a" in mode:
            raise InjectedFailure("simulated disk write error")
        return original_open(self, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", failing_open)

    m2 = Message(topic="roll", value={"a": 2}, key="k")
    with pytest.raises(InjectedFailure):
        qs.write_message(m2)

    # Descriptor should remain at 1 (unchanged); count in storage state should also remain 1
    assert descriptor.read_text(encoding="utf-8").strip() == "1"

    # Remove failure and write again to confirm recovery
    monkeypatch.setattr(Path, "open", original_open)
    m3 = Message(topic="roll", value={"a": 3}, key="k")
    qs.write_message(m3)
    # Descriptor now increments to 2
    assert descriptor.read_text(encoding="utf-8").strip() == "2"

    # Segment file should contain exactly two lines
    seg0 = topic_dir / "0"
    lines = seg0.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2
