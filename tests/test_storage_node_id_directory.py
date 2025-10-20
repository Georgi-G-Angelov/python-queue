from app.main import create_app
from app.config import NodeConfig
from app.messaging.storage import QueueStorage
from fastapi.testclient import TestClient
import os
from pathlib import Path

def test_storage_directory_matches_node_id(tmp_path, monkeypatch):
    # Isolate working directory
    monkeypatch.chdir(tmp_path)
    QueueStorage.reset_for_tests()
    cfg = NodeConfig(node_id=55, peers=[])
    app = create_app(cfg)
    client = TestClient(app)
    # Trigger a simple request to ensure routes functional
    r = client.get('/health')
    assert r.status_code == 200
    storage = QueueStorage.get()
    assert storage.base_dir.name == '55'
    # Ensure process CWD switched to node dir
    assert Path(os.getcwd()).name == '55'
