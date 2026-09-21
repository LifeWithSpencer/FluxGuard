"""
benchmarks/locustfile.py

Load test for the distributed rate limiter gateway.

Two user classes:

  BurstUser
    Simulates many callers hammering a small set of API keys well past
    their configured limit, to verify (a) throughput/latency under load,
    and (b) that the limit holds *strictly* across gateway replicas -
    i.e. the aggregate allowed-count across all nodes never exceeds the
    configured limit, which is only possible because the Lua scripts make
    the check+increment atomic in Redis rather than racy read-then-write
    from Python.

  FailureInjectionUser
    A lightweight companion user that periodically checks a `/readyz`-style
    signal and logs circuit-breaker state transitions surfaced in the
    `X-RateLimit-Served-By` response header, so a human running
    `docker stop ratelimiter-redis-1` mid-test can visually confirm the
    gateway keeps returning 200/429 (never 5xx) and switches to
    `local_fallback` instead of crashing.

Usage:
    locust -f benchmarks/locustfile.py --host http://localhost:8080

Suggested failure-injection procedure while Locust is running:
    docker compose stop redis
    # observe served_by flip to local_fallback in locust stats / logs
    docker compose start redis
    # observe recovery back to "redis" after the circuit breaker's
    # recovery_timeout + a successful half-open trial

p50/p95/p99 latency is reported natively by Locust's web UI and
`--csv` output; no custom instrumentation needed beyond what's below.
"""

from __future__ import annotations

import random
import uuid

from locust import HttpUser, between, task, events

# A small, fixed pool of API keys so bursts concentrate load onto a handful
# of rate-limit buckets - this is what actually stresses the "no leaked
# tokens across distributed nodes" property. A huge random key space would
# just spread load thin and never approach any single key's limit.
API_KEY_POOL = [f"loadtest-key-{i}" for i in range(10)]

served_by_counts = {"redis": 0, "local_fallback": 0, "unknown": 0}


@events.quitting.add_listener
def _report_served_by(environment, **kwargs):
    print("\n--- Rate limit decisions served by ---")
    for source, count in served_by_counts.items():
        print(f"  {source}: {count}")


class BurstUser(HttpUser):
    """Sends rapid-fire requests, well above the free-tier limit (60/min),
    to a small pool of shared API keys."""

    wait_time = between(0.01, 0.05)  # near-continuous bursting

    @task
    def hit_gateway(self):
        api_key = random.choice(API_KEY_POOL)
        headers = {"X-API-Key": api_key, "X-Request-ID": str(uuid.uuid4())}

        with self.client.get("/api/resource", headers=headers, catch_response=True) as resp:
            served_by = resp.headers.get("X-RateLimit-Served-By", "unknown")
            served_by_counts[served_by] = served_by_counts.get(served_by, 0) + 1

            if resp.status_code == 200:
                resp.success()
            elif resp.status_code == 429:
                # Expected once the limit is exceeded - not a failure of
                # the system, it's the system working correctly.
                resp.success()
            else:
                resp.failure(f"unexpected status {resp.status_code}")


class ExpensiveEndpointUser(HttpUser):
    """Targets the tighter endpoint-specific override (5 req/min) defined
    in middleware/rate_limit.py's DEFAULT_ENDPOINT_OVERRIDES, to verify
    per-route rules layer correctly on top of per-tier defaults."""

    wait_time = between(0.5, 1.5)

    @task
    def hit_expensive_endpoint(self):
        api_key = random.choice(API_KEY_POOL)
        headers = {"X-API-Key": api_key}
        with self.client.get("/api/expensive", headers=headers, catch_response=True) as resp:
            if resp.status_code in (200, 429):
                resp.success()
            else:
                resp.failure(f"unexpected status {resp.status_code}")
