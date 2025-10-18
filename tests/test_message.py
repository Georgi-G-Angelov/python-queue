from app.messaging import Message


def test_message_auto_timestamp_and_json():
    msg = Message(topic="orders", key="order-123", value={"amount": 10})
    assert msg.timestamp is not None
    raw = msg.to_json()
    # ensure JSON contains expected substrings (lightweight check)
    assert "orders" in raw and "order-123" in raw


def test_message_custom_timestamp():
    ts = 1700000000.0
    msg = Message(topic="orders", key="order-456", value="ok", timestamp=ts)
    assert msg.timestamp == ts


def test_message_json_roundtrip():
    original = Message(topic="metrics", key="cpu", value={"usage": 0.5})
    raw = original.to_json()
    restored = Message.from_json(raw)
    assert restored.topic == original.topic
    assert restored.key == original.key
    assert restored.value == original.value
    assert isinstance(restored.timestamp, float)


def test_message_from_json_missing_value_fails():
    try:
        Message.from_json('{"topic": "a"}')
        assert False, "Expected ValueError for missing fields (value)"
    except ValueError as e:
        assert "Missing required fields" in str(e)


def test_message_auto_generated_key():
    msg = Message(topic="events", value={"x": 1})  # no key
    assert msg.key is not None
    # key should be numeric string between 1 and 10
    assert msg.key.isdigit()
    assert 1 <= int(msg.key) <= 10

    restored = Message.from_json(msg.to_json())
    assert restored.key == msg.key  # serialization preserves generated key


def test_server_partition_determinism():
    m1 = Message(topic="topicA", key="key1", value=1)
    m2 = Message(topic="topicA", key="key1", value=2)
    p1 = m1.server_partition()
    p2 = m2.server_partition()
    assert p1 == p2
    assert 0 <= p1 <= 1000


def test_server_partition_variation():
    a = Message(topic="topicA", key="key1", value=None).server_partition()
    b = Message(topic="topicA", key="key2", value=None).server_partition()
    c = Message(topic="topicB", key="key1", value=None).server_partition()
    assert 0 <= a <= 1000 and 0 <= b <= 1000 and 0 <= c <= 1000
    distinct = len({a, b, c})
    assert distinct >= 2
