"""
middleware/rate_limit.py

ASGI middleware that:
  1. Identifies the caller (API key > Bearer token > client IP, in that
     priority order).
  2. Resolves a RateLimitRule for (caller_tier, endpoint) via a pluggable
     `RuleResolver`.
  3. Calls the RateLimiterEngine to get a Decision.
  4. Injects standard IETF draft rate-limit headers on every response, and
     Retry-After on 429s.
  5. Records Prometheus metrics (allowed/blocked counters, latency).

Header spec followed (IETF draft-ietf-httpapi-ratelimit-headers):
  RateLimit-Limit, RateLimit-Remaining, RateLimit-Reset (seconds until reset,
  per the draft - NOT an absolute epoch timestamp), Retry-After (seconds,
  RFC 7231, on 429 only).
"""

from __future__ import annotations

import logging
import math
import time
import uuid
from typing import Callable, Optional

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from core.limiter import RateLimitRule, RateLimiterEngine, Algorithm

logger = logging.getLogger("ratelimiter.middleware")


# --------------------------------------------------------------------- #
# Caller identification
# --------------------------------------------------------------------- #

def identify_caller(request: Request) -> tuple[str, str]:
    """Returns (caller_id, identification_method).

    Priority: X-API-Key header > Authorization: Bearer token > client IP.

    Trust assumption: the IP fallback only works correctly if this gateway
    sits behind a trusted reverse proxy (see nginx.conf) that OVERWRITES
    X-Forwarded-For with the real peer address rather than appending to it.
    If a proxy instead appends (e.g. via Nginx's default
    $proxy_add_x_forwarded_for), a client can prepend an arbitrary value and
    have it land first in the header - taking the first entry would then
    trust attacker-controlled input and let every request present a fresh
    identity, bypassing the limiter entirely. Reading the LAST entry is
    defense-in-depth for that scenario: it degrades to "shared bucket on a
    shared NAT/proxy" (the known, documented trade-off) instead of a full
    bypass, even if the edge trust assumption above is ever violated by a
    future config change.
    """
    api_key = request.headers.get("x-api-key")
    if api_key:
        return api_key, "api_key"

    auth = request.headers.get("authorization", "")
    if auth.lower().startswith("bearer "):
        token = auth.split(" ", 1)[1].strip()
        if token:
            return token, "bearer_token"

    client_ip = request.client.host if request.client else "unknown"
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        client_ip = forwarded.split(",")[-1].strip()
    return client_ip, "ip"


# --------------------------------------------------------------------- #
# Rule resolution
# --------------------------------------------------------------------- #

# Example static tier table. In production this would likely be backed by
# a database/config service and cached; kept simple and swappable here.
DEFAULT_TIER_LIMITS: dict[str, RateLimitRule] = {
    "free": RateLimitRule(algorithm=Algorithm.SLIDING_WINDOW, limit=60, window_seconds=60),
    "pro": RateLimitRule(algorithm=Algorithm.SLIDING_WINDOW, limit=1000, window_seconds=60),
    "internal": RateLimitRule(algorithm=Algorithm.TOKEN_BUCKET, limit=10_000, window_seconds=60,
                               capacity=10_000, refill_rate_per_sec=200),
}

# Endpoint-specific overrides take priority over tier defaults, keyed by
# (method, path) - path is matched as a prefix for simplicity.
DEFAULT_ENDPOINT_OVERRIDES: dict[str, RateLimitRule] = {
    "/api/expensive": RateLimitRule(algorithm=Algorithm.TOKEN_BUCKET, limit=5, window_seconds=60,
                                     capacity=5, refill_rate_per_sec=5 / 60),
}


class RuleResolver:
    """Pluggable rule lookup. Swap this out (e.g. for a DB-backed resolver)
    without touching the middleware itself."""

    def __init__(
        self,
        tier_limits: Optional[dict[str, RateLimitRule]] = None,
        endpoint_overrides: Optional[dict[str, RateLimitRule]] = None,
        tier_lookup: Optional[Callable[[Request, str], str]] = None,
    ) -> None:
        self.tier_limits = tier_limits or DEFAULT_TIER_LIMITS
        self.endpoint_overrides = endpoint_overrides or DEFAULT_ENDPOINT_OVERRIDES
        # Defaults every caller to "free" unless a real tier lookup (e.g.
        # from an auth service / DB) is supplied.
        self.tier_lookup = tier_lookup or (lambda request, caller_id: "free")

    def resolve(self, request: Request, caller_id: str) -> tuple[RateLimitRule, str]:
        """Returns (rule, rule_key_suffix) where rule_key_suffix disambiguates
        the Redis key so different rules don't share counters."""
        path = request.url.path
        for prefix, rule in self.endpoint_overrides.items():
            if path.startswith(prefix):
                return rule, f"endpoint:{prefix}"

        tier = self.tier_lookup(request, caller_id)
        rule = self.tier_limits.get(tier, self.tier_limits["free"])
        return rule, f"tier:{tier}"


# --------------------------------------------------------------------- #
# Middleware
# --------------------------------------------------------------------- #

class RateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app,
        engine: RateLimiterEngine,
        rule_resolver: Optional[RuleResolver] = None,
        metrics: Optional["RateLimitMetrics"] = None,
    ) -> None:
        super().__init__(app)
        self.engine = engine
        self.rule_resolver = rule_resolver or RuleResolver()
        self.metrics = metrics

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        request_id = request.headers.get("x-request-id", str(uuid.uuid4()))
        caller_id, method = identify_caller(request)
        rule, rule_key_suffix = self.rule_resolver.resolve(request, caller_id)

        redis_key = f"rl:{rule.algorithm.value}:{caller_id}:{rule_key_suffix}"

        start = time.perf_counter()
        decision = await self.engine.check(redis_key, rule)
        elapsed = time.perf_counter() - start

        if self.metrics:
            self.metrics.observe_latency(elapsed, served_by=decision.served_by)
            self.metrics.record_decision(decision.allowed, served_by=decision.served_by)

        logger.info(
            "rate_limit_decision",
            extra={
                "request_id": request_id,
                "caller_id": caller_id,
                "identification_method": method,
                "path": request.url.path,
                "allowed": decision.allowed,
                "served_by": decision.served_by,
                "remaining": decision.remaining,
            },
        )

        reset_seconds = max(0, math.ceil((decision.reset_epoch_ms - time.time() * 1000) / 1000))

        if not decision.allowed:
            retry_after_seconds = max(1, math.ceil(decision.retry_after_ms / 1000))
            response = JSONResponse(
                status_code=429,
                content={
                    "error": "rate_limit_exceeded",
                    "message": "Too many requests. Please retry later.",
                    "request_id": request_id,
                },
            )
            response.headers["Retry-After"] = str(retry_after_seconds)
        else:
            response = await call_next(request)

        response.headers["RateLimit-Limit"] = str(decision.limit)
        response.headers["RateLimit-Remaining"] = str(max(0, decision.remaining))
        response.headers["RateLimit-Reset"] = str(reset_seconds)
        response.headers["X-Request-ID"] = request_id
        response.headers["X-RateLimit-Served-By"] = decision.served_by

        return response


# --------------------------------------------------------------------- #
# Metrics (Prometheus)
# --------------------------------------------------------------------- #

class RateLimitMetrics:
    """Thin wrapper around prometheus_client collectors, isolated here so
    the middleware doesn't import prometheus_client directly and stays
    easier to unit test without a metrics backend."""

    def __init__(self):
        from prometheus_client import Counter, Histogram

        self.allowed_total = Counter(
            "gateway_requests_allowed_total", "Total requests allowed", ["served_by"]
        )
        self.blocked_total = Counter(
            "gateway_requests_blocked_total", "Total requests blocked (429)", ["served_by"]
        )
        self.decision_latency = Histogram(
            "gateway_rate_limit_decision_seconds",
            "Time to reach an allow/block decision",
            ["served_by"],
            buckets=(0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1),
        )
        self.circuit_state_transitions = Counter(
            "gateway_circuit_breaker_transitions_total",
            "Circuit breaker state transitions",
            ["from_state", "to_state"],
        )

    def record_decision(self, allowed: bool, served_by: str) -> None:
        if allowed:
            self.allowed_total.labels(served_by=served_by).inc()
        else:
            self.blocked_total.labels(served_by=served_by).inc()

    def observe_latency(self, seconds: float, served_by: str) -> None:
        self.decision_latency.labels(served_by=served_by).observe(seconds)

    def record_transition(self, old_state, new_state) -> None:
        self.circuit_state_transitions.labels(
            from_state=old_state.value, to_state=new_state.value
        ).inc()
