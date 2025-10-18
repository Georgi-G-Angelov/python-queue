# python-queue

Prototype FastAPI service that will become a distributed queue.

## Features
- FastAPI app with `/health` and root endpoints
- Ready for extension with queue logic
- Basic tests using `pytest`

## Quickstart

### 1. Create & activate a virtual environment (Windows PowerShell)
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

### 2. Install dependencies
```powershell
pip install -r requirements.txt
```

### 3. Run the development server (simple)
```powershell
python run_server.py --node-id 0 --port 8000
```
Open: http://127.0.0.1:8000/docs

### 3b. Run with hot reload (development)
When using reload we have to let uvicorn recreate the app from an import string. The helper script handles this automatically.
```powershell
python run_server.py --node-id 0 --port 8000 --reload
```

### 3c. Multiple nodes locally
Terminal 1:
```powershell
python run_server.py --node-id 0 --port 8000 --reload
```
Terminal 2:
```powershell
python run_server.py --node-id 1 --port 8001 --peers http://127.0.0.1:8000 --reload
```
Terminal 3:
```powershell
python run_server.py --node-id 2 --port 8002 --peers http://127.0.0.1:8000 http://127.0.0.1:8001 --reload
```
Check cluster info:
```powershell
curl http://127.0.0.1:8001/cluster/info
```

### 4. Run tests
```powershell
pytest -q
```

## Node Configuration

You can configure a node either via CLI args (recommended) or environment variables:

Env vars (used automatically when `--reload` is active):
- `QUEUE_NODE_ID` integer id
- `QUEUE_PEERS` comma or space separated peer URLs

Example with env vars directly:
```powershell
$env:QUEUE_NODE_ID=5; $env:QUEUE_PEERS="http://127.0.0.1:8000,http://127.0.0.1:8001"; uvicorn app.main:create_app_from_env --factory --port 8005 --reload
```

## Next Steps (for distributed queue)
- Define queue item model & persistence (in-memory first)
- Add enqueue/dequeue endpoints
- Implement node registration & discovery (heartbeat or gossip)
- Add replication / consistency strategy
- Introduce background workers for processing

---
Generated scaffold.
