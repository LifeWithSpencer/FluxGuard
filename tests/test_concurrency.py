"""
tests/test_concurrency.py

Correctness and concurrency tests for the rate limiter engine.

Uses `fakeredis`'s async server for tests that don't require real network
behavior (script loading, basic decisions), and marks tests that need a
*real* Redis instance (to genuinely exercise Lua atomicity under
concurrent load) so they can be skipped in environments without Docker.

Run:
    pytest tests/test_concurrency.py -v
    REDIS_URL=redis://localhost:6379/1 pytest tests/test_concurrency.py -v -m integration
"""

from __future__ import annotations

import asyncio
import os

import pytest
import pytest_asyncio

from core.circuit_breaker import CircuitBreakerConfig
from core.limiter import Algorithm, RateLimitRule, RateLimiterEngine

REDIS_URL = os.environ.get("TEST_REDIS_URL", "redis://localhost:6379/15")


def _redis_available() -> bool:
    """Best-effort synchronous check so we can skip integration tests
    cleanly in CI environments without a Redis instance, rather than
    failing with a confusing connection error."""
    import redis as sync_redis

    try:
        client = sync_redis.Redis.from_url(REDIS_URL, socket_connect_timeout=0.2)
        return client.ping()
    except Exception:
        return False


requires_redis = pytest.mark.skipif(
    not _redis_available(), reason="requires a live Redis instance at TEST_REDIS_URL"
)


@pytest_asyncio.fixture
async def engine():
    eng = RateLimiterEngine(
        redis_url=REDIS_URL,
        circuit_breaker_config=CircuitBreakerConfig(
            failure_threshold=3, window_seconds=5.0, recovery_timeout=1.0
        ),
        fallback_capacity=10,
        fallback_refill_rate_per_sec=1.0,
    )
    await eng.startup()
    # Clean slate between tests.
    await eng._client.flushdb()
    yield eng
    await eng.shutdown()


@pytest_asyncio.fixture
async def concurrency_engine():
    """Separate fixture for the pure-atomicity/no-race-condition tests.

    These tests deliberately fire hundreds of concurrent requests at a
    single key, which stresses the Redis *connection pool* far more than
    ordinary traffic - a purpose-built high-concurrency test is exactly
    the scenario `max_connections` needs headroom for. Two things
    specifically guard against connection-pool jitter masquerading as a
    correctness failure:

      1. `max_connections` is set well above the largest burst any test
         here fires (500), so the pool itself is never the bottleneck.
      2. `failure_threshold` is set effectively infinite so a handful of
         transient connection errors (e.g. brief pool contention) don't
         trip the circuit breaker mid-test and silently divert some
         requests to the local fallback, which would leak extra
         "allowed" decisions into the count and produce a flaky,
         misleading test failure that looks like a Lua atomicity bug but
         is actually a test-harness sizing issue.

    Tests using this fixture additionally assert `served_by == "redis"`
    on every result (see `_assert_all_served_by_redis` below) as a second,
    independent guard: if the fallback is ever used for any reason, the
    test fails loudly and specifically, instead of just producing a count
    that's off by however many requests leaked through the fallback.
    """
    eng = RateLimiterEngine(
        redis_url=REDIS_URL,
        max_connections=1000,
        circuit_breaker_config=CircuitBreakerConfig(
            failure_threshold=1_000_000,  # effectively disable tripping for this test
            window_seconds=5.0,
            recovery_timeout=1.0,
        ),
        fallback_capacity=10,
        fallback_refill_rate_per_sec=1.0,
    )
    await eng.startup()
    await eng._client.flushdb()
    yield eng
    await eng.shutdown()


def _assert_all_served_by_redis(results, context: str) -> None:
    fallback_hits = [r for r in results if r.served_by != "redis"]
    assert not fallback_hits, (
        f"{context}: {len(fallback_hits)}/{len(results)} requests were served by "
        f"'{fallback_hits[0].served_by}' instead of 'redis' - the circuit breaker "
        f"tripped during a pure-atomicity test (likely connection pool pressure), "
        f"which invalidates the race-condition count. Increase max_connections or "
        f"investigate why Redis calls are failing."
    )


# --------------------------------------------------------------------- #
# Sliding window correctness
# --------------------------------------------------------------------- #

@requires_redis
@pytest.mark.asyncio
async def test_sliding_window_allows_up_to_limit(engine):
    rule = RateLimitRule(algorithm=Algorithm.SLIDING_WINDOW, limit=5, window_seconds=60)
    key = "test:sw:allow"

    results = [await engine.check(key, rule) for _ in range(5)]
    assert all(r.allowed for r in results)

    sixth = await engine.check(key, rule)
    assert sixth.allowed is False
    assert sixth.remaining == 0


@requires_redis
@pytest.mark.asyncio
async def test_sliding_window_expires_old_entries(engine):
    rule = RateLimitRule(algorithm=Algorithm.SLIDING_WINDOW, limit=2, window_seconds=1)
    key = "test:sw:expire"

    assert (await engine.check(key, rule)).allowed
    assert (await engine.check(key, rule)).allowed
    assert (await engine.check(key, rule)).allowed is False

    await asyncio.sleep(1.1)

    # Window has fully rolled over; should be allowed again.
    assert (await engine.check(key, rule)).allowed is True


# --------------------------------------------------------------------- #
# Token bucket correctness
# --------------------------------------------------------------------- #

@requires_redis
@pytest.mark.asyncio
async def test_token_bucket_allows_burst_up_to_capacity(engine):
    rule = RateLimitRule(
        algorithm=Algorithm.TOKEN_BUCKET, limit=3, capacity=3, refill_rate_per_sec=1, window_seconds=60
    )
    key = "test:tb:burst"

    results = [await engine.check(key, rule) for _ in range(3)]
    assert all(r.allowed for r in results)

    fourth = await engine.check(key, rule)
    assert fourth.allowed is False
    assert fourth.retry_after_ms > 0


@requires_redis
@pytest.mark.asyncio
async def test_token_bucket_refills_over_time(engine):
    rule = RateLimitRule(
        algorithm=Algorithm.TOKEN_BUCKET, limit=1, capacity=1, refill_rate_per_sec=2, window_seconds=60
    )
    key = "test:tb:refill"

    assert (await engine.check(key, rule)).allowed is True
    assert (await engine.check(key, rule)).allowed is False

    await asyncio.sleep(0.6)  # 2 tokens/sec * 0.6s = 1.2 tokens refilled

    assert (await engine.check(key, rule)).allowed is True


# --------------------------------------------------------------------- #
# Concurrency: no race conditions leak extra tokens
# --------------------------------------------------------------------- #

@requires_redis
@pytest.mark.asyncio
async def test_no_race_condition_under_concurrent_bursts(concurrency_engine):
    """Fire far more concurrent requests than the limit allows and assert
    the number of *allowed* requests exactly equals the limit - proving
    the Lua script's atomicity holds even when hundreds of coroutines hit
    the same key at once.

    Uses `concurrency_engine` (high max_connections, circuit breaker
    effectively disabled) rather than the default `engine` fixture so that
    connection-pool pressure from 500 concurrent callers can't trip the
    breaker and divert some requests to the local fallback, which would
    inflate the allowed count for reasons unrelated to Lua atomicity.
    """
    rule = RateLimitRule(algorithm=Algorithm.SLIDING_WINDOW, limit=50, window_seconds=60)
    key = "test:sw:race"

    concurrency = 500
    results = await asyncio.gather(*[concurrency_engine.check(key, rule) for _ in range(concurrency)])

    _assert_all_served_by_redis(results, "test_no_race_condition_under_concurrent_bursts")

    allowed_count = sum(1 for r in results if r.allowed)
    assert allowed_count == 50, (
        f"expected exactly 50 allowed requests out of {concurrency} concurrent "
        f"attempts, got {allowed_count} - indicates a race condition"
    )


@requires_redis
@pytest.mark.asyncio
async def test_no_race_condition_token_bucket_under_concurrent_bursts(concurrency_engine):
    rule = RateLimitRule(
        algorithm=Algorithm.TOKEN_BUCKET, limit=20, capacity=20, refill_rate_per_sec=0, window_seconds=60
    )
    key = "test:tb:race"

    concurrency = 300
    results = await asyncio.gather(*[concurrency_engine.check(key, rule) for _ in range(concurrency)])

    _assert_all_served_by_redis(results, "test_no_race_condition_token_bucket_under_concurrent_bursts")

    allowed_count = sum(1 for r in results if r.allowed)
    assert allowed_count == 20


# --------------------------------------------------------------------- #
# NOSCRIPT recovery
# --------------------------------------------------------------------- #

@requires_redis
@pytest.mark.asyncio
async def test_recovers_from_noscript_after_flush(engine):
    rule = RateLimitRule(algorithm=Algorithm.SLIDING_WINDOW, limit=10, window_seconds=60)
    key = "test:sw:noscript"

    assert (await engine.check(key, rule)).allowed is True

    # Simulate Redis losing its script cache (e.g. restart without
    # persistence, or an operator running SCRIPT FLUSH).
    await engine._client.script_flush()

    # This call should transparently reload the script and succeed rather
    # than raising NoScriptError up to the caller.
    result = await engine.check(key, rule)
    assert result.allowed is True
    assert result.served_by == "redis"


# --------------------------------------------------------------------- #
# Circuit breaker + local fallback (no live Redis needed - point the
# engine at an unreachable address to force failures deterministically)
# --------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_falls_back_to_local_when_redis_unreachable():
    eng = RateLimiterEngine(
        redis_url="redis://127.0.0.1:1",  # nothing listens here
        circuit_breaker_config=CircuitBreakerConfig(
            failure_threshold=1, window_seconds=5.0, recovery_timeout=60.0
        ),
        fallback_capacity=3,
        fallback_refill_rate_per_sec=1.0,
    )
    await eng.startup()  # should not raise even though Redis is unreachable

    rule = RateLimitRule(algorithm=Algorithm.SLIDING_WINDOW, limit=100, window_seconds=60)

    results = [await eng.check("test:fallback:key", rule) for _ in range(5)]

    # First call trips the breaker (failure_threshold=1); all should be
    # served by the local fallback and still enforce *some* throttle
    # (fallback_capacity=3, refill 1/sec) rather than raising or passing
    # every request through unchecked.
    assert all(r.served_by == "local_fallback" for r in results)
    allowed_count = sum(1 for r in results if r.allowed)
    assert allowed_count == 3  # fallback_capacity, since no refill time elapsed

    await eng.shutdown()


@pytest.mark.asyncio
async def test_local_fallback_never_raises_and_never_fails_open():
    from core.local_fallback import LocalTokenBucketFallback

    fb = LocalTokenBucketFallback(capacity=2, refill_rate_per_sec=0.001)
    outcomes = [fb.check("k")[0] for _ in range(10)]
    assert outcomes.count(True) == 2
    assert outcomes.count(False) == 8
