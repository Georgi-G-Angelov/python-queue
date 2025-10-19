from app.messaging.storage import QueueStorage
from app.messaging import Message
from pathlib import Path
import os


def setup_storage(tmp_path, node=1):
    os.chdir(tmp_path)
    QueueStorage.reset_for_tests()
    return QueueStorage(node)


def test_read_message_sequential(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    qs = setup_storage(tmp_path)

    # Write 5 messages to one topic
    msgs = [Message(topic="t1", value=i, key="1") for i in range(5)]
    for m in msgs:
        qs.write_message(m)

    partition = msgs[0].server_partition()

    received = []
    for _ in range(5):
        msg = qs.read_message(partition, "t1", consumer_group="cgA")
        assert msg is not None
        received.append(msg.value)

    # Now exhausted
    assert qs.read_message(partition, "t1", consumer_group="cgA") is None
    assert received == list(range(5))


def test_consumer_group_independent_offsets(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    qs = setup_storage(tmp_path, node=2)

    msgs = [Message(topic="t2", value=i, key="2") for i in range(3)]
    for m in msgs:
        qs.write_message(m)
    partition = msgs[0].server_partition()

    # cgX reads one
    first = qs.read_message(partition, "t2", consumer_group="cgX")
    assert first is not None and first.value == 0
    # cgY should still see the first message
    first_y = qs.read_message(partition, "t2", consumer_group="cgY")
    assert first_y is not None and first_y.value == 0

    # cgX reads remaining
    second = qs.read_message(partition, "t2", consumer_group="cgX")
    third = qs.read_message(partition, "t2", consumer_group="cgX")
    assert second.value == 1 and third.value == 2
    assert qs.read_message(partition, "t2", consumer_group="cgX") is None

    # cgY only consumed one so far
    second_y = qs.read_message(partition, "t2", consumer_group="cgY")
    third_y = qs.read_message(partition, "t2", consumer_group="cgY")
    assert second_y.value == 1 and third_y.value == 2
    assert qs.read_message(partition, "t2", consumer_group="cgY") is None


def test_read_message_empty_partition(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    qs = setup_storage(tmp_path, node=3)
    # Partition with no messages
    assert qs.read_message(0, "missing", consumer_group="cg") is None


def test_read_message_segment_boundary(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    qs = setup_storage(tmp_path, node=4)

    # Write 105 messages to test segment rollover reading
    msgs = [Message(topic="tseg", value=i, key="5") for i in range(105)]
    for m in msgs:
        qs.write_message(m)
    partition = msgs[0].server_partition()

    # Read first 100 (segment 0)
    for i in range(100):
        msg = qs.read_message(partition, "tseg", consumer_group="cg")
        assert msg is not None and msg.value == i
    # Next messages in segment 1
    for i in range(100, 105):
        msg = qs.read_message(partition, "tseg", consumer_group="cg")
        assert msg is not None and msg.value == i
    # Exhausted
    assert qs.read_message(partition, "tseg", consumer_group="cg") is None
