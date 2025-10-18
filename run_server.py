"""CLI entrypoint to run the FastAPI server with node configuration.

Usage examples (PowerShell):
    python run_server.py --node-id 1 --port 8001 --peers http://127.0.0.1:8000 http://127.0.0.1:8002

This wraps uvicorn programmatically so we can inject the config-produced app.
"""
from __future__ import annotations

import argparse
import os
import sys
import uvicorn
from app.main import create_app, build_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run python-queue node")
    parser.add_argument("--node-id", type=int, default=0, help="Unique integer id for this node")
    parser.add_argument(
        "--peers",
        nargs="*",
        default=[],
        help="List of peer base URLs (e.g. http://host:port) excluding self",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Bind host (default 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8000, help="Bind port (default 8000)")
    parser.add_argument(
        "--reload", action="store_true", help="Enable auto-reload (development only)"
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.reload:
        # For reload to work, uvicorn requires an import string, not a constructed instance.
        # We'll set env vars that create_app_from_env() will read.
        os.environ["QUEUE_NODE_ID"] = str(args.node_id)
        if args.peers:
            os.environ["QUEUE_PEERS"] = ",".join(args.peers)
        else:
            os.environ.pop("QUEUE_PEERS", None)
        # Self URL used by gossip manager
        os.environ["QUEUE_SELF_URL"] = f"http://{args.host}:{args.port}"
        # Run referencing the factory function import string.
        uvicorn.run(
            "app.main:create_app_from_env",
            host=args.host,
            port=args.port,
            reload=True,
            factory=True,
            lifespan="on",
        )
    else:
        cfg = build_config(node_id=args.node_id, peers=args.peers)
        app = create_app(cfg)
        # Attach self_url attribute on state for non-reload construction
        app.state.self_url = f"http://{args.host}:{args.port}"
        uvicorn.run(app, host=args.host, port=args.port, reload=False, lifespan="on")


if __name__ == "__main__":  # pragma: no cover
    main()
