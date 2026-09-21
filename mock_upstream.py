"""
mock_upstream.py

A trivial upstream service the gateway reverse-proxies to. Always returns
200 OK with a small JSON payload, regardless of path/method - just enough
to prove end-to-end forwarding works during load tests.
"""

from fastapi import FastAPI, Request

app = FastAPI(title="Mock Upstream")


@app.api_route("/{full_path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def catch_all(full_path: str, request: Request):
    return {"status": "ok", "path": f"/{full_path}", "method": request.method}
