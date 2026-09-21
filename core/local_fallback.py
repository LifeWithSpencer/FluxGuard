"""
core/local_fallback.py

An in-process, per-node token bucket used when Redis is unreachable and the
circuit breaker has tripped OPEN. This is deliberately simple and lock-based
rather than async: the operation is pure CPU/memory (no I/O), so a
`threading.Lock` held for microseconds is the correct primitive and never
blocks the event loop in a way that matters.

Important trade-off (documented, not hidden): this fallback is *local* to
each gateway process. In a multi-replica deployment, each replica enforces
its own limit independently, so the effective ceiling during a Redis outage
is `limit * number_of_replicas` rather than the globally coordinated limit.
This is a deliberate "fail-safe, not fail-open" choice: traffic is still
throttled per-node instead of passing through unchecked. See the README
"Disaster Recovery" section for the full discussion.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Dict


@dataclass
class _BucketState:
    tokens: float
    last_refill: float  # monotonic seconds


@dataclass
class LocalTokenBucketFallback:
    """A thread-safe, LRU-bounded collection of local token buckets.

    One bucket per rate-limit key (e.g. per API key / IP / tier+route combo).
    Bounded via a simple size cap + eviction of the least-recently-used
    entries, so a flood of distinct keys (e.g. spoofed IPs) can't grow this
    structure without bound while Redis is down.
    """

    capacity: float
    refill_rate_per_sec: float
    max_tracked_keys: int = 50_000

    _buckets: Dict[str, _BucketState] = field(default_factory=dict)
    _lru: "dict[str, None]" = field(default_factory=dict)  # ordered dict as LRU
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def _evict_if_needed(self) -> None:
        while len(self._buckets) > self.max_tracked_keys:
            oldest_key, _ = next(iter(self._lru.items()))
            del self._lru[oldest_key]
            self._buckets.pop(oldest_key, None)

    def check(self, key: str, requested: float = 1.0) -> tuple[bool, int, float, float]:
        """Consume `requested` tokens for `key`.

        Returns (allowed, remaining_int, reset_epoch_ms, retry_after_ms),
        matching the shape the middleware expects from the Redis path so it
        can build identical response headers regardless of which backend
        served the decision.
        """
        now = time.monotonic()
        now_wall_ms = time.time() * 1000

        with self._lock:
            state = self._buckets.get(key)
            if state is None:
                state = _BucketState(tokens=self.capacity, last_refill=now)
                self._buckets[key] = state
            else:
                # Refresh LRU position.
                self._lru.pop(key, None)

            self._lru[key] = None

            elapsed = max(now - state.last_refill, 0.0)
            state.tokens = min(self.capacity, state.tokens + elapsed * self.refill_rate_per_sec)
            state.last_refill = now

            allowed = state.tokens >= requested
            retry_after_ms = 0.0

            if allowed:
                state.tokens -= requested
            else:
                deficit = requested - state.tokens
                if self.refill_rate_per_sec > 0:
                    retry_after_ms = (deficit / self.refill_rate_per_sec) * 1000.0
                else:
                    retry_after_ms = 3600 * 1000.0

            if self.refill_rate_per_sec > 0:
                missing = self.capacity - state.tokens
                reset_ms = now_wall_ms + (missing / self.refill_rate_per_sec) * 1000.0
            else:
                reset_ms = now_wall_ms

            self._evict_if_needed()

            return allowed, int(state.tokens), reset_ms, retry_after_ms

    def reset(self) -> None:
        """Clear all local state. Called when the circuit closes again, so
        stale local decisions don't linger and confuse metrics."""
        with self._lock:
            self._buckets.clear()
            self._lru.clear()
