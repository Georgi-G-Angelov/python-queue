import os
from pathlib import Path
from app.messaging.storage import QueueStorage
from app.messaging import Message


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

    # Write 105 messages to force rollover: messages 1..100 in file 0, 101..105 in file 1
    last_msg = None
    for i in range(105):
        m = Message(topic="beta", value=i, key="3")
        last_msg = m
        qs.write_message(m)

    partition = last_msg.server_partition()
    topic_dir = Path(str(partition)) / last_msg.topic
    descriptor = topic_dir / "descriptor.txt"
    assert descriptor.exists()
    assert descriptor.read_text(encoding="utf-8").strip() == "105"

    seg0 = topic_dir / "0"
    seg1 = topic_dir / "1"
    assert seg0.exists() and seg1.exists()

    seg0_lines = seg0.read_text(encoding="utf-8").strip().splitlines()
    seg1_lines = seg1.read_text(encoding="utf-8").strip().splitlines()
    assert len(seg0_lines) == 100
    assert len(seg1_lines) == 5

    # Ensure each line parses as JSON
    import json
    for line in (seg0_lines + seg1_lines):
        parsed = json.loads(line)
        assert parsed["topic"] == "beta"


def test_corrupt_descriptor_resets(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    QueueStorage.reset_for_tests()
    qs = QueueStorage(11)

    msg = Message(topic="gamma", value="first", key="2")
    path_first = qs.write_message(msg)

    partition = msg.server_partition()
    topic_dir = Path(str(partition)) / msg.topic
    descriptor = topic_dir / "descriptor.txt"

    # Corrupt descriptor
    descriptor.write_text("not-an-int", encoding="utf-8")

    msg2 = Message(topic="gamma", value="second", key="2")
    path_second = qs.write_message(msg2)

    # After corruption we treat previous as 0 and now descriptor should be 1
    assert descriptor.read_text(encoding="utf-8").strip() == "1"
    assert path_first == path_second  # still segment 0
