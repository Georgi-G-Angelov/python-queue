from fastapi import FastAPI

app = FastAPI(title="python-queue", version="0.1.0")


@app.get("/health", tags=["system"])  # Simple health/hello endpoint
async def health() -> dict[str, str]:
    """Return a basic health payload."""
    return {"status": "ok", "message": "hello world"}


# Optional root redirect or message
@app.get("/")
async def root() -> dict[str, str]:
    return {"service": "python-queue", "docs": "/docs"}
