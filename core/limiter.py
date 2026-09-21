"""
core/limiter.py

Redis-backed rate limiting engine.

Responsibilities:
  * Own the async Redis connection pool (redis.asyncio).
  * Preload Lua scripts via SCRIPT LOAD at startup and cache their SHA1s.
  * Execute rate-limit checks via EVALSHA, transparently reloading and
    retrying once on NOSCRIPT (e.g. after a Redis restart/FLUSHALL wiped
    the script cache).
  * Wrap every Redis call through the circuit breaker so failures degrade
    to the local in-memory fallback instead of raising to the caller.

This module intentionally knows nothing about HTTP/FastAPI - it exposes a
plain async `check(...)` API so it can be unit tested and load tested in
isolation from the web layer.
"""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Optional

import redis.asyncio as redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import NoScriptError, RedisError, TimeoutError as RedisTimeoutError

from core.circuit_breaker import AsyncCircuitBreaker, CircuitBreakerConfig, CircuitOpenError
from core.local_fallback import LocalTokenBucketFallback

logger = logging.getLogger("ratelimiter.limiter")

SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"


class Algorithm(str, Enum):
    SLIDING_WINDOW = "sliding_window"
    TOKEN_BUCKET = "token_bucket"


@dataclass
class RateLimitRule:
    algorithm: Algorithm
    limit: int                 # sliding_window: max requests per window
    window_seconds: float = 60.0  # sliding_window only
    capacity: Optional[int] = None          # token_bucket only (defaults to `limit`)
    refill_rate_per_sec: Optional[float] = None  # token_bucket only


@dataclass
class Decision:
    allowed: bool
    limit: int
    remaining: int
    reset_epoch_ms: float
    retry_after_ms: float
    served_by: str  # "redis" or "local_fallback"


class RedisUnavailable(Exception):
    """Raised (rarely, and only if the local fallback itself errors) to
    signal a total failure of the rate limiting subsystem. Callers should
    treat this as fail-safe-closed for security-sensitive endpoints, or
    fail-open with heavy logging for low-stakes ones - that policy choice
    belongs to the middleware, not this engine."""


class RateLimiterEngine:
    def __init__(
        self,
        redis_url: str,
        *,
        max_connections: int = 1000,
        socket_timeout: float = 0.25,
        socket_connect_timeout: float = 0.25,
        circuit_breaker_config: Optional[CircuitBreakerConfig] = None,
        fallback_capacity: int = 60,
        fallback_refill_rate_per_sec: float = 1.0,
    ) -> None:
        self._redis_url = redis_url
        # max_connections needs to comfortably exceed peak in-flight EVALSHA
        # calls across all workers in this process. Under-sizing this is a
        # self-inflicted outage: once the pool is exhausted, redis-py raises
        # a connection error for every additional caller, which the circuit
        # breaker (correctly) interprets as Redis being unhealthy and trips
        # to OPEN - even though Redis itself is fine. That failure mode is
        # exactly what caused the local-fallback token leakage seen under
        # concurrency testing (500 concurrent requests against a pool of
        # 100 connections). 1000 gives generous headroom for burst/load
        # testing; tune to (expected concurrent requests per replica) with
        # margin for a real deployment.
        self._pool = redis.ConnectionPool.from_url(
            redis_url,
            max_connections=max_connections,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
            socket_keepalive=True,
            retry_on_timeout=True,
            health_check_interval=30,
        )
        self._client = redis.Redis(connection_pool=self._pool)

        self._script_shas: dict[Algorithm, str] = {}
        self._script_source: dict[Algorithm, str] = {}

        self._breaker = AsyncCircuitBreaker(
            config=circuit_breaker_config or CircuitBreakerConfig(),
            ping_fn=self._ping,
            on_state_change=self._on_breaker_state_change,
        )

        self._fallback = LocalTokenBucketFallback(
            capacity=fallback_capacity,
            refill_rate_per_sec=fallback_refill_rate_per_sec,
        )

        # Simple callback hook the middleware/metrics layer can subscribe to.
        self.on_state_change_callbacks: list = []

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    async def startup(self) -> None:
        """Load Lua scripts into Redis and start the circuit breaker's
        background health checker. Call once during FastAPI startup."""
        for algo, filename in (
            (Algorithm.SLIDING_WINDOW, "sliding_window.lua"),
            (Algorithm.TOKEN_BUCKET, "token_bucket.lua"),
        ):
            source = (SCRIPTS_DIR / filename).read_text()
            self._script_source[algo] = source
            try:
                sha = await self._client.script_load(source)
                self._script_shas[algo] = sha
                logger.info("script_loaded", extra={"algorithm": algo.value, "sha": sha})
            except RedisError as exc:
                # If Redis is down at boot, don't crash the app - the
                # circuit breaker will simply stay effectively "open" (no
                # SHA cached means every call falls back locally) until
                # Redis becomes reachable and scripts get lazily loaded.
                logger.warning(
                    "script_load_failed_at_startup",
                    extra={"algorithm": algo.value, "error": str(exc)},
                )

        self._breaker.start()

    async def shutdown(self) -> None:
        """Stop the breaker's background task and cleanly tear down the
        Redis client and its connection pool.

        Order matters: `aclose()` on the client releases its connections
        back to the pool, then `disconnect()` on the pool actually closes
        the underlying sockets. Without explicitly disconnecting the pool,
        any connections it still holds are only closed via `__del__` at GC
        time - which on Python 3.13 can fire after the event loop that
        owns those sockets has already been torn down (e.g. at the end of
        a pytest-asyncio test), producing noisy
        "RuntimeError: Event loop is closed" messages from `__del__`
        that don't affect test outcomes but pollute output and can mask
        real failures.
        """
        await self._breaker.stop()
        await self._client.aclose()
        await self._pool.disconnect(inuse_connections=True)

    async def _ping(self) -> bool:
        try:
            return bool(await self._client.ping())
        except RedisError:
            return False

    def _on_breaker_state_change(self, old_state, new_state) -> None:
        logger.warning("circuit_state_change", extra={"from": old_state.value, "to": new_state.value})
        if new_state.value == "closed":
            # Wipe locally-accumulated state so a node that spent time in
            # fallback mode doesn't keep enforcing a stale local decision
            # after Redis (the source of truth) is back.
            self._fallback.reset()
        for cb in self.on_state_change_callbacks:
            cb(old_state, new_state)

    # ------------------------------------------------------------------ #
    # Script execution with NOSCRIPT auto-recovery
    # ------------------------------------------------------------------ #

    async def _evalsha_with_reload(self, algorithm: Algorithm, keys: list[str], args: list) -> list:
        sha = self._script_shas.get(algorithm)

        async def _run():
            nonlocal sha
            if sha is None:
                # Never successfully loaded (e.g. Redis was down at boot).
                # Try loading now; if this also fails it propagates and the
                # circuit breaker records it as a failure.
                sha = await self._client.script_load(self._script_source[algorithm])
                self._script_shas[algorithm] = sha

            try:
                return await self._client.evalsha(sha, len(keys), *keys, *args)
            except NoScriptError:
                # Script cache was flushed server-side (e.g. Redis restarted
                # without persistence, or SCRIPT FLUSH ran). Reload once and
                # retry - this is expected occasionally, not an error state.
                logger.info("noscript_reload", extra={"algorithm": algorithm.value})
                sha = await self._client.script_load(self._script_source[algorithm])
                self._script_shas[algorithm] = sha
                return await self._client.evalsha(sha, len(keys), *keys, *args)

        return await self._breaker.call(_run)

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    async def check(self, key: str, rule: RateLimitRule) -> Decision:
        """Evaluate the rate limit for `key` under `rule`.

        Always returns a Decision - never raises for ordinary Redis
        unavailability, since that's routed to the local fallback
        transparently. Only raises RedisUnavailable if the fallback itself
        is somehow broken (should not happen in practice; it's pure memory).
        """
        now_ms = time.time() * 1000

        try:
            if rule.algorithm == Algorithm.SLIDING_WINDOW:
                raw = await self._evalsha_with_reload(
                    Algorithm.SLIDING_WINDOW,
                    keys=[key],
                    args=[int(rule.window_seconds * 1000), rule.limit, int(now_ms), uuid.uuid4().hex],
                )
                allowed, remaining, reset_ms, retry_after_ms = raw
            else:
                capacity = rule.capacity or rule.limit
                refill_rate = rule.refill_rate_per_sec or (rule.limit / max(rule.window_seconds, 1e-9))
                ttl_seconds = int(capacity / max(refill_rate, 1e-9)) + 5
                raw = await self._evalsha_with_reload(
                    Algorithm.TOKEN_BUCKET,
                    keys=[key],
                    args=[capacity, refill_rate, 1, int(now_ms), ttl_seconds],
                )
                allowed, remaining, reset_ms, retry_after_ms = raw

            return Decision(
                allowed=bool(int(allowed)),
                limit=rule.limit,
                remaining=int(remaining),
                reset_epoch_ms=float(reset_ms),
                retry_after_ms=float(retry_after_ms),
                served_by="redis",
            )

        except (CircuitOpenError, RedisConnectionError, RedisTimeoutError, RedisError):
            # Fall back to local, per-node enforcement. This deliberately
            # fails "safe" (still throttles) rather than "open" (lets
            # everything through) or "closed" (blocks everything).
            allowed, remaining, reset_ms, retry_after_ms = self._fallback.check(key)
            return Decision(
                allowed=allowed,
                limit=rule.limit,
                remaining=remaining,
                reset_epoch_ms=reset_ms,
                retry_after_ms=retry_after_ms,
                served_by="local_fallback",
            )

    @property
    def circuit_state(self):
        return self._breaker.state
