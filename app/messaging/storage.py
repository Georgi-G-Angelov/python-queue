from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import Optional


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
