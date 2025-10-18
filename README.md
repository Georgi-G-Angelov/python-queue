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

### 3. Run the development server (hot reload)
```powershell
uvicorn app.main:app --reload --port 8000
```
Navigate to: http://127.0.0.1:8000/docs for interactive docs.

### 4. Run tests
```powershell
pytest -q
```

## Next Steps (for distributed queue)
- Define queue item model & persistence (in-memory first)
- Add enqueue/dequeue endpoints
- Implement node registration & discovery
- Add replication / consistency strategy
- Introduce background workers for processing

---
Generated scaffold.
