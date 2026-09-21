# Distributed Rate Limiter & API Gateway

A production-grade, horizontally-scalable rate limiter and reverse-proxy
gateway built on FastAPI, `redis.asyncio`, and atomic Lua scripting.

## Contents

```
scripts/sliding_window.lua   Atomic ZSET-based sliding window log
scripts/token_bucket.lua     Atomic HASH-based token bucket
core/limiter.py              Redis engine: SCRIPT LOAD, EVALSHA, NOSCRIPT recovery
core/circuit_breaker.py      Async Closed/Open/Half-Open state machine
core/local_fallback.py       In-memory per-node token bucket fallback
middleware/rate_limit.py     ASGI middleware: identification, rules, headers, metrics
main.py                      FastAPI app, reverse proxy, /metrics, /healthz, /readyz
mock_upstream.py             Trivial upstream target for the reverse proxy
docker-compose.yml           Redis + 2 gateway replicas + mock upstream + Nginx LB
nginx.conf                   Load balancer config
tests/test_concurrency.py    Pytest-asyncio correctness & race-condition tests
benchmarks/locustfile.py     Locust load test (burst + endpoint-specific limits)
benchmarks/failure_injection_test.py  Automated Redis-outage pass/fail harness
```

## Quick start

```bash
docker compose up --build
# Gateway (via Nginx LB): http://localhost:8080
curl -i -H "X-API-Key: demo" http://localhost:8080/api/resource
```

Run tests (needs a local Redis for integration tests; falls back-only
tests run without one):

```bash
pip install -r requirements.txt
docker run -d -p 6379:6379 redis:7.4-alpine   # for integration tests
pytest tests/test_concurrency.py -v
```

Run the load test:

```bash
locust -f benchmarks/locustfile.py --host http://localhost:8080
```

---

## Architecture

### Request path

```
Client -> Nginx (LB) -> Gateway replica (FastAPI)
                            -> RateLimitMiddleware
                                 -> identify_caller()        (API key / bearer / IP)
                                 -> RuleResolver.resolve()   (tier + endpoint overrides)
                                 -> RateLimiterEngine.check()
                                      -> CircuitBreaker.call(EVALSHA ...)   [normal path]
                                      -> LocalTokenBucketFallback.check()  [degraded path]
                                 -> inject RateLimit-* / Retry-After headers
                            -> reverse_proxy() -> httpx.AsyncClient -> upstream
```

Every gateway replica is stateless except for its local fallback buckets
and its own circuit breaker state - both are per-process, by design (see
"Disaster Recovery" below). The shared source of truth is Redis.

### Why Lua scripts instead of Python-side read-modify-write

A naive `GET` -> compute -> `SET` from Python is inherently racy: two
concurrent requests can both read "9 out of 10 used" and both decide to
allow, leaking an 11th token. Wrapping the check-and-increment in a single
Lua script makes it atomic from Redis's point of view - Redis executes
scripts single-threadedly, so no other command (including another EVALSHA
for the same key from a different gateway replica) can interleave between
the read and the write. This is what `tests/test_concurrency.py::test_no_race_condition_under_concurrent_bursts`
verifies directly: 500 concurrent requests against a limit of 50 must
produce *exactly* 50 allowed, not "50 plus or minus a few from a race."

### Why EVALSHA + SCRIPT LOAD instead of EVAL

`EVAL` sends the full script body over the wire on every call. `EVALSHA`
sends only a 40-byte SHA1 digest, which matters at the request volumes
this gateway is designed for. Scripts are loaded once at startup
(`RateLimiterEngine.startup()`) and their SHAs cached in memory. If Redis
loses its script cache mid-flight (a restart without the appendonly file
replaying `SCRIPT LOAD`, or an operator running `SCRIPT FLUSH`), the next
`EVALSHA` raises `NOSCRIPT`; `_evalsha_with_reload()` catches that
specific error, reloads the script, and retries exactly once - transparent
to the caller and covered by `test_recovers_from_noscript_after_flush`.

---

## Sliding Window vs. Token Bucket: trade-offs

| | Sliding Window (ZSET log) | Token Bucket (HASH) |
|---|---|---|
| **Burst behavior** | Strictly bounds requests in *any* rolling window - no burst above the limit is ever possible, even at window boundaries. | Allows a burst up to `capacity` instantly, then throttles to the steady-state refill rate. Often what users actually want (e.g. "60/min average, but don't punish a quick double-click"). |
| **Memory per key** | O(requests in window) - one ZSET entry per request until it ages out. A key doing 1000 req/min under a 60s window holds ~1000 entries. | O(1) - two scalar fields regardless of traffic volume. |
| **CPU cost** | `ZREMRANGEBYSCORE` + `ZCARD` + `ZADD` per call; cost scales with entries pruned, which is bounded by traffic in one window but is real work at high QPS per key. | Two float multiplications and a comparison; effectively constant time. |
| **Precision** | Exact - reflects the literal count of requests in the last `window_seconds`, no approximation. | Exact for the model it implements (a physical bucket), but that model itself is an approximation of "N per minute" that permits bursts. |
| **Best for** | Strict compliance-style limits (e.g. "hard cap of 5 password reset attempts per 15 minutes") where any burst tolerance is unacceptable. | General API throttling where smoothing bursts is desirable and memory/CPU efficiency matters at high key cardinality (e.g. per-IP limits with millions of distinct IPs). |

This implementation exposes both as first-class algorithms
(`Algorithm.SLIDING_WINDOW` / `Algorithm.TOKEN_BUCKET`) selectable per rule
in `RuleResolver`, so different endpoints or tiers can use whichever model
fits - e.g. the free/pro tier defaults use sliding window for predictable
fairness, while the `/api/expensive` override uses a token bucket to allow
a small burst without permanently amplifying cost.

---

## Standard rate-limit headers

Every response (2xx or 429) includes, per the IETF
`draft-ietf-httpapi-ratelimit-headers` draft:

- `RateLimit-Limit` - the configured ceiling for the caller's current rule.
- `RateLimit-Remaining` - tokens/requests left, floored at 0.
- `RateLimit-Reset` - **seconds** until the window/bucket resets (the draft
  specifies a delta, not an absolute timestamp - this implementation
  computes the delta from the engine's absolute `reset_epoch_ms` at the
  moment of response construction).

429 responses additionally carry `Retry-After` (seconds, RFC 7231),
computed from the engine's `retry_after_ms` rather than reusing
`RateLimit-Reset`, because for a token bucket the time until *a single
token* is available (retry-after) can be much shorter than the time until
the bucket is back to *full* (reset).

A `X-RateLimit-Served-By` header (`redis` or `local_fallback`) is also
included - not part of the IETF draft, but invaluable operationally and in
the failure-injection test for confirming which code path served a given
decision.

---

## Circuit Breaker & Disaster Recovery

### State machine

```
CLOSED --[N failures within window_seconds]--> OPEN
OPEN --[recovery_timeout elapsed, next request OR background PING]--> HALF_OPEN
HALF_OPEN --[trial call succeeds, all in-flight trials drain]--> CLOSED
HALF_OPEN --[any trial call fails]--> OPEN (timer restarts)
```

A background task (`_health_check_loop`) independently PINGs Redis every
`health_check_interval` seconds while OPEN, so recovery isn't solely
dependent on live user traffic happening to arrive after the timeout -
important for low-traffic services where minutes could pass between
requests to a given process.

### Fail-safe, not fail-open, not fail-closed

When Redis is unreachable (`CircuitOpenError`, connection error, or
timeout propagate out of `_evalsha_with_reload`), `RateLimiterEngine.check()`
catches those specific exceptions and routes to `LocalTokenBucketFallback`
instead of either:

- **Failing open** (letting all traffic through unchecked) - this would
  turn a Redis blip into an accidental DDoS-enablement window for exactly
  the traffic the limiter exists to control.
- **Failing closed** (rejecting all traffic) - this would turn a Redis
  blip into a full outage of the entire API, which is usually a worse
  outcome than slightly-imprecise throttling.

Instead, each gateway replica enforces its own local, in-memory limit
during the outage. This is a **known, deliberate trade-off**:

> With `R` replicas behind the load balancer, the effective ceiling during
> a Redis outage becomes approximately `local_fallback_capacity × R`
> rather than the globally-coordinated limit, because each replica's
> fallback bucket is independent. Traffic is still meaningfully throttled
> per-node - just not perfectly coordinated across nodes - until Redis
> recovers.

Operators should size `fallback_capacity` / `fallback_refill_rate_per_sec`
conservatively (e.g. `configured_limit / expected_replica_count`) so that
worst-case aggregate throughput during an outage stays close to the
intended limit rather than multiplying by replica count.

When the circuit closes again, `RateLimiterEngine._on_breaker_state_change`
calls `LocalTokenBucketFallback.reset()` to discard accumulated local
state, so a replica that just spent five minutes in fallback mode doesn't
keep enforcing a stale local decision once Redis - the actual source of
truth - is back online.

### Verifying it: `benchmarks/failure_injection_test.py`

This script drives continuous load against the live Docker Compose stack,
runs `docker compose stop redis` mid-test, and asserts:

1. Zero transport-level exceptions and zero 5xx responses throughout -
   the gateway must never crash a request just because Redis vanished.
2. `X-RateLimit-Served-By` is observed to flip to `local_fallback` during
   the outage.
3. After `docker compose start redis`, it's observed to flip back to
   `redis` once the breaker's `recovery_timeout` elapses and a half-open
   trial succeeds.

---

### Empirically Verified Benchmarks & Resilience Metrics

All benchmarks executed locally against the multi-container Docker Compose cluster (Dual Uvicorn replicas + Nginx reverse proxy + Redis 7.4).

#### 1. Concurrency & Race-Condition Verification
* **Test Suite:** 9/9 passed (`pytest tests/test_concurrency.py -v`) in 2.94s.
* **Concurrent Burst Atomicity:** Fired 500 parallel requests against a limit of 50. Exactly 50 requests were allowed (0 leaked tokens).
* **Token Bucket Burst Atomicity:** Fired 300 parallel requests against a capacity of 20. Exactly 20 requests were allowed.
* **Bytecode Cache Recovery:** Confirmed transparent runtime reload via `SCRIPT LOAD` on `NOSCRIPT` error after manual `SCRIPT FLUSH`.

#### 2. Chaos / Failure-Injection Run (`benchmarks/failure_injection_test.py`)
Continuous load executed across three operational phases including hard Redis shutdown and restart:

* **Phase 1 (Healthy Redis):** 1,091 requests served via Redis (`X-RateLimit-Served-By: redis`).
* **Phase 2 (Outage Injection):** Redis stopped mid-traffic; 526 requests seamlessly degraded to in-memory fallback (`X-RateLimit-Served-By: local_fallback`).
* **Phase 3 (Auto-Recovery):** Redis restarted; circuit breaker detected health and restored primary path.
* **Availability & Reliability:** 0 dropped connections, 0 transport exceptions, and 0.00% 5xx errors across the entire lifecycle.
## Configuration surface

Key tunables (see `core/circuit_breaker.py:CircuitBreakerConfig` and
`core/limiter.py:RateLimiterEngine.__init__`):

| Setting | Default | Notes |
|---|---|---|
| `failure_threshold` | 5 | Failures within `window_seconds` before tripping OPEN |
| `window_seconds` | 10 | Sliding window for failure counting |
| `recovery_timeout` | 5 | Seconds OPEN before allowing a HALF_OPEN trial |
| `half_open_max_trials` | 3 | Concurrent trial requests allowed during HALF_OPEN |
| `health_check_interval` | 2 | Background PING interval while OPEN |
| `fallback_capacity` | 60 | Local bucket size per key during an outage |
| `fallback_refill_rate_per_sec` | 1.0 | Local bucket refill rate during an outage |
| `socket_timeout` / `socket_connect_timeout` | 0.25s | How fast a hung Redis call is treated as a failure |

## Known limitations / honest caveats

- IP-based identification trusts `X-Forwarded-For` from the first proxy
  hop; deploy behind a trusted reverse proxy that sets this correctly, or
  callers behind shared NAT will share one bucket, and spoofed headers
  from untrusted clients could let an attacker pick another caller's
  bucket if the gateway is exposed directly to the internet without a
  trusted edge in front of it.
- The tier lookup (`RuleResolver.tier_lookup`) defaults every caller to
  "free" - wiring it to a real auth/entitlement service is left as an
  integration point (`tier_lookup` callback), not implemented here since
  it's inherently specific to each deployment's auth system.
- Local fallback throttling is per-replica, not globally coordinated (see
  Disaster Recovery above) - this is a deliberate trade-off, not an
  oversight.
