import os
from pathlib import Path
from app.messaging.storage import QueueStorage
from app.messaging import Message
from app.messaging.constants import MESSAGES_PER_SEGMENT


def test_write_message_creates_structure(tmp_path, monkeypatch):
    # Change CWD to temporary path for isolation
    monkeypatch.chdir(tmp_path)
    QueueStorage.reset_for_tests()
    qs = QueueStorage(7)  # creates 7/ and chdir into it

    # Write several messages with same topic/key to ensure descriptor increments and same segment
    messages = [
        Message(topic="alpha", value={"x": 1}, key="5"),
        Message(topic="alpha", value={"x": 2}, key="5"),
        Message(topic="alpha", value={"x": 3}, key="5"),
    ]

    segment_path = None
    for m in messages:
        segment_path = qs.write_message(m)

    # Ensure working directory is node base dir
    assert Path(os.getcwd()).name == "7"

    partition = messages[0].server_partition()
    part_dir = Path(str(partition))
    topic_dir = part_dir / messages[0].topic
    descriptor = topic_dir / "descriptor.txt"

    assert part_dir.exists()
    assert topic_dir.exists()
    assert descriptor.exists()
    # descriptor should have value equal to number of messages written (3)
    assert descriptor.read_text(encoding="utf-8").strip() == "3"
    assert segment_path is not None and segment_path.exists()
    # Messages 1..3 remain in segment index 0 file
    assert segment_path.name == "0"
    content = segment_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(content) == 3
    # Each line should contain the topic name
    for line in content:
        assert '"alpha"' in line


def test_descriptor_increments_and_segment_rolls(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    QueueStorage.reset_for_tests()
    qs = QueueStorage(9)

    # Write MESSAGES_PER_SEGMENT + 5 messages to force rollover: first segment full, remainder in next
    last_msg = None
    extra = 5
    total = MESSAGES_PER_SEGMENT + extra
    for i in range(total):
        m = Message(topic="beta", value=i, key="3")
        last_msg = m
        qs.write_message(m)

    partition = last_msg.server_partition()
    topic_dir = Path(str(partition)) / last_msg.topic
    descriptor = topic_dir / "descriptor.txt"
    assert descriptor.exists()
    assert descriptor.read_text(encoding="utf-8").strip() == str(total)

    seg0 = topic_dir / "0"
    seg1 = topic_dir / "1"
    assert seg0.exists() and seg1.exists()

    seg0_lines = seg0.read_text(encoding="utf-8").strip().splitlines()
    seg1_lines = seg1.read_text(encoding="utf-8").strip().splitlines()
    assert len(seg0_lines) == MESSAGES_PER_SEGMENT
    assert len(seg1_lines) == extra

    # Ensure each line parses as JSON
    import json
    for line in (seg0_lines + seg1_lines):
        parsed = json.loads(line)
        assert parsed["topic"] == "beta"


def test_corrupt_descriptor_ignored_during_runtime(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    QueueStorage.reset_for_tests()
    qs = QueueStorage(11)

    msg = Message(topic="gamma", value="first", key="2")
    path_first = qs.write_message(msg)

    partition = msg.server_partition()
    topic_dir = Path(str(partition)) / msg.topic
    descriptor = topic_dir / "descriptor.txt"

    # Corrupt descriptor on disk after first write
    descriptor.write_text("not-an-int", encoding="utf-8")

    # Second write should ignore on-disk corruption (no re-read) and increment in-memory count to 2
    msg2 = Message(topic="gamma", value="second", key="2")
    path_second = qs.write_message(msg2)

    # Descriptor now overwritten with count 2
    assert descriptor.read_text(encoding="utf-8").strip() == "2"
    assert path_first == path_second  # still segment 0 (under MESSAGES_PER_SEGMENT messages)


def test_multiple_segments_created(tmp_path, monkeypatch):
    """Write more than 2 * MESSAGES_PER_SEGMENT messages to ensure multiple segment files are created.

    For (2 * MESSAGES_PER_SEGMENT + 50) messages we expect:
    - descriptor.txt contains total
    - segment files: 0 (first MESSAGES_PER_SEGMENT), 1 (second MESSAGES_PER_SEGMENT), 2 (remaining 50 lines)
    """
    monkeypatch.chdir(tmp_path)
    QueueStorage.reset_for_tests()
    qs = QueueStorage(13)

    topic = "delta"
    key = "7"
    remainder = 50
    total_messages = 2 * MESSAGES_PER_SEGMENT + remainder
    messages = [Message(topic=topic, value=i, key=key) for i in range(total_messages)]
    for m in messages:
        qs.write_message(m)

    partition = messages[0].server_partition()
    topic_dir = Path(str(partition)) / topic
    descriptor = topic_dir / "descriptor.txt"
    assert descriptor.exists()
    assert descriptor.read_text(encoding="utf-8").strip() == str(total_messages)

    seg0 = topic_dir / "0"
    seg1 = topic_dir / "1"
    seg2 = topic_dir / "2"
    assert seg0.exists() and seg1.exists() and seg2.exists()

    seg0_lines = seg0.read_text(encoding="utf-8").strip().splitlines()
    seg1_lines = seg1.read_text(encoding="utf-8").strip().splitlines()
    seg2_lines = seg2.read_text(encoding="utf-8").strip().splitlines()

    assert len(seg0_lines) == MESSAGES_PER_SEGMENT
    assert len(seg1_lines) == MESSAGES_PER_SEGMENT
    assert len(seg2_lines) == remainder

    import json
    # Quick integrity check: all lines parse and belong to same topic
    for line in seg2_lines:  # sample last segment
        parsed = json.loads(line)
        assert parsed["topic"] == topic
