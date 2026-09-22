"""
main.py

FastAPI-based API Gateway with distributed rate limiting.

Responsibilities beyond rate limiting itself:
  * Structured JSON logging with request IDs.
  * /metrics Prometheus endpoint.
  * /healthz and /readyz for orchestrator probes.
  * Reverse-proxy forwarding of all other traffic to an upstream service
    via httpx.AsyncClient, so this process behaves like a real gateway
    rather than a toy that only demonstrates the limiter in isolation.

Run with:
    uvicorn main:app --loop uvloop --workers 1
(use Gunicorn+UvicornWorker for multi-process, see docker-compose.yml)
"""

from __future__ import annotations

import logging
import os
import sys
import time
import uuid
from contextlib import asynccontextmanager

import httpx
import structlog
from fastapi import FastAPI, Request
from fastapi.responses import Response, StreamingResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from core.circuit_breaker import CircuitBreakerConfig
from core.limiter import RateLimiterEngine
from middleware.rate_limit import RateLimitMetrics, RateLimitMiddleware, RuleResolver

# --------------------------------------------------------------------- #
# Structured JSON logging
# --------------------------------------------------------------------- #

def configure_logging() -> None:
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=logging.INFO)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


configure_logging()
logger = structlog.get_logger("ratelimiter.gateway")

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
UPSTREAM_URL = os.environ.get("UPSTREAM_URL", "http://localhost:9000")
if not UPSTREAM_URL.startswith(("http://", "https://")):
    # Render's fromService `hostport` property returns bare "host:port" with
    # no scheme (unlike docker-compose's UPSTREAM_URL=http://upstream:9000,
    # which is a literal value we write ourselves). httpx.AsyncClient's
    # base_url requires a scheme, so add one rather than requiring every
    # deployment target to format this identically.
    UPSTREAM_URL = f"http://{UPSTREAM_URL}"


def _redact_redis_url(url: str) -> str:
    """Strip credentials before a Redis URL ever reaches a log line.
    redis://:PASSWORD@host:port/db -> redis://***@host:port/db
    Logging the raw URL was itself a credential-leak bug (the exact class of
    issue this gateway's own security review flagged for caller_id) - this
    closes it at the one place the full URL is ever logged."""
    if "@" not in url:
        return url
    scheme_and_creds, host_part = url.rsplit("@", 1)
    scheme = scheme_and_creds.split("://", 1)[0]
    return f"{scheme}://***@{host_part}"

# Engine + metrics are created at module scope (not inside lifespan) because
# `add_middleware` must reference a constructed instance before the app
# starts serving. `startup()` (which touches Redis) still happens inside
# the lifespan context below, not here.
engine = RateLimiterEngine(
    redis_url=REDIS_URL,
    circuit_breaker_config=CircuitBreakerConfig(
        failure_threshold=5,
        window_seconds=10.0,
        recovery_timeout=5.0,
        half_open_max_trials=3,
        health_check_interval=2.0,
    ),
    fallback_capacity=60,
    fallback_refill_rate_per_sec=1.0,
)
metrics = RateLimitMetrics()
engine.on_state_change_callbacks.append(metrics.record_transition)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await engine.startup()
    http_client = httpx.AsyncClient(base_url=UPSTREAM_URL, timeout=5.0)

    app.state.engine = engine
    app.state.metrics = metrics
    app.state.http_client = http_client

    logger.info("gateway_startup", redis_url=_redact_redis_url(REDIS_URL), upstream_url=UPSTREAM_URL)
    try:
        yield
    finally:
        await http_client.aclose()
        await engine.shutdown()
        logger.info("gateway_shutdown")


app = FastAPI(title="Distributed Rate Limiter Gateway", lifespan=lifespan)
app.add_middleware(RateLimitMiddleware, engine=engine, rule_resolver=RuleResolver(), metrics=metrics)


# --------------------------------------------------------------------- #
# Request-ID + access logging middleware (runs outside the rate limiter,
# so every request - including ones later rejected with 429 - gets a
# structured access log line)
# --------------------------------------------------------------------- #

@app.middleware("http")
async def request_context_middleware(request: Request, call_next):
    request_id = request.headers.get("x-request-id", str(uuid.uuid4()))
    structlog.contextvars.bind_contextvars(request_id=request_id)
    start = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        logger.exception("unhandled_exception", path=request.url.path)
        raise
    else:
        duration_ms = (time.perf_counter() - start) * 1000
        logger.info(
            "request_completed",
            path=request.url.path,
            method=request.method,
            status_code=response.status_code,
            duration_ms=round(duration_ms, 2),
        )
        return response
    finally:
        structlog.contextvars.clear_contextvars()


# --------------------------------------------------------------------- #
# Operational endpoints
# --------------------------------------------------------------------- #

@app.get("/healthz")
async def healthz():
    """Liveness probe: process is up and serving. Deliberately does NOT
    depend on Redis - a Redis outage should not make Kubernetes kill and
    restart gateway pods, since the whole point of the circuit breaker is
    to keep serving via local fallback during that outage."""
    return {"status": "ok"}


@app.get("/readyz")
async def readyz():
    """Readiness probe: reports circuit state so orchestration/alerting can
    see degraded mode without treating the pod as unhealthy."""
    return {"status": "ready", "circuit_state": app.state.engine.circuit_state.value}


METRICS_TOKEN = os.environ.get("METRICS_TOKEN", "")


@app.get("/metrics")
async def metrics_endpoint(request: Request):
    """Gated behind METRICS_TOKEN once this gateway is internet-facing.
    Unauthenticated /metrics exposes internal traffic volume and
    circuit-breaker state to anyone who can reach the service - fine on
    localhost, not fine once there's a public URL. If METRICS_TOKEN isn't
    set (e.g. running purely locally with no .env), this deliberately
    falls back to open access rather than locking out local dev."""
    if METRICS_TOKEN:
        provided = request.headers.get("x-metrics-token", "")
        if provided != METRICS_TOKEN:
            return Response(status_code=404)  # 404, not 401 - don't confirm the endpoint exists
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


# --------------------------------------------------------------------- #
# Reverse proxy: forward everything else upstream
# --------------------------------------------------------------------- #

HOP_BY_HOP_HEADERS = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade", "host", "content-length",
}


@app.api_route(
    "/{full_path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"],
)
async def reverse_proxy(full_path: str, request: Request):
    client: httpx.AsyncClient = request.app.state.http_client

    forward_headers = {
        k: v for k, v in request.headers.items() if k.lower() not in HOP_BY_HOP_HEADERS
    }
    body = await request.body()

    upstream_request = client.build_request(
        method=request.method,
        url=f"/{full_path}",
        params=request.query_params,
        headers=forward_headers,
        content=body,
    )
    upstream_response = await client.send(upstream_request, stream=True)

    async def stream_body():
        async for chunk in upstream_response.aiter_raw():
            yield chunk
        await upstream_response.aclose()

    response_headers = {
        k: v for k, v in upstream_response.headers.items() if k.lower() not in HOP_BY_HOP_HEADERS
    }

    return StreamingResponse(
        stream_body(),
        status_code=upstream_response.status_code,
        headers=response_headers,
    )