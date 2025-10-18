import os
from pathlib import Path
from app.messaging.storage import QueueStorage


def test_queue_storage_directory_creation(tmp_path, monkeypatch):
    # Change working directory to temporary path
    QueueStorage.reset_for_tests()
    monkeypatch.chdir(tmp_path)
    # Initialize storage for node id 42
    qs = QueueStorage(42)
    assert qs.base_dir == tmp_path / "42"
    assert qs.base_dir.exists()
    # Subsequent initialization with different id should NOT change directory (singleton)
    qs2 = QueueStorage(99)
    assert qs2 is qs
    assert qs2.base_dir == tmp_path / "42"
    # Access via get()
    assert QueueStorage.get() is qs
    # Ensure path_for works
    p = qs.path_for("messages")
    assert p.name == "messages"
    assert p.parent.name == "42"
