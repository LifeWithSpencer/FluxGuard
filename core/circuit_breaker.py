"""
core/circuit_breaker.py

A small async circuit breaker specialized for guarding calls to Redis.

States:
  CLOSED     - normal operation, calls go to Redis.
  OPEN       - Redis is considered down; calls are short-circuited straight
               to the local fallback without even attempting Redis.
  HALF_OPEN  - a probation period after `recovery_timeout` has elapsed since
               tripping OPEN. A limited number of trial calls are allowed
               through to Redis; if they succeed, the circuit closes; if any
               fails, it re-opens and the timeout restarts.

Design notes:
  * Failure counting uses a fixed-size sliding window of recent outcomes
    (a deque of booleans) rather than a raw counter, so the breaker reacts
    to *rate* of failure within a window instead of an ever-growing total
    that would require a periodic reset.
  * All state mutation happens under an `asyncio.Lock` because multiple
    concurrent request handlers touch this object.
  * A background health-check task independently probes Redis with a
    cheap PING while OPEN, so recovery isn't solely dependent on user
    traffic arriving to trigger a half-open trial.
"""

from __future__ import annotations

import asyncio
import enum
import logging
import time
from collections import deque
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional, TypeVar

logger = logging.getLogger("ratelimiter.circuit_breaker")

T = TypeVar("T")


class CircuitState(str, enum.Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5          # failures within window to trip OPEN
    window_seconds: float = 10.0        # sliding window for failure counting
    recovery_timeout: float = 5.0       # seconds to wait before HALF_OPEN
    half_open_max_trials: int = 3       # concurrent trial calls allowed in HALF_OPEN
    health_check_interval: float = 2.0  # background PING interval while OPEN


class CircuitOpenError(Exception):
    """Raised internally to signal callers should use the fallback path."""


class AsyncCircuitBreaker:
    def __init__(
        self,
        config: CircuitBreakerConfig,
        ping_fn: Callable[[], Awaitable[bool]],
        on_state_change: Optional[Callable[[CircuitState, CircuitState], None]] = None,
    ) -> None:
        self._config = config
        self._ping_fn = ping_fn
        self._on_state_change = on_state_change

        self._state = CircuitState.CLOSED
        self._lock = asyncio.Lock()
        self._outcomes: deque[float] = deque()  # timestamps of failures within window
        self._opened_at: Optional[float] = None
        self._half_open_inflight = 0
        self._health_task: Optional[asyncio.Task] = None

    @property
    def state(self) -> CircuitState:
        return self._state

    def start(self) -> None:
        """Start the background health-check loop. Call once at app startup."""
        if self._health_task is None:
            self._health_task = asyncio.create_task(self._health_check_loop())

    async def stop(self) -> None:
        if self._health_task is not None:
            self._health_task.cancel()
            try:
                await self._health_task
            except asyncio.CancelledError:
                pass
            self._health_task = None

    def _transition(self, new_state: CircuitState) -> None:
        if new_state == self._state:
            return
        old_state = self._state
        self._state = new_state
        logger.info("circuit_breaker_transition", extra={"from": old_state, "to": new_state})
        if self._on_state_change:
            self._on_state_change(old_state, new_state)

    async def _record_failure(self) -> None:
        async with self._lock:
            now = time.monotonic()
            self._outcomes.append(now)
            cutoff = now - self._config.window_seconds
            while self._outcomes and self._outcomes[0] < cutoff:
                self._outcomes.popleft()

            if self._state == CircuitState.HALF_OPEN:
                # A single failure during trial re-opens immediately.
                self._trip_open()
            elif self._state == CircuitState.CLOSED and len(self._outcomes) >= self._config.failure_threshold:
                self._trip_open()

    def _trip_open(self) -> None:
        self._opened_at = time.monotonic()
        self._transition(CircuitState.OPEN)

    async def _record_success(self) -> None:
        async with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._half_open_inflight = max(0, self._half_open_inflight - 1)
                if self._half_open_inflight == 0:
                    self._outcomes.clear()
                    self._transition(CircuitState.CLOSED)

    async def _maybe_enter_half_open(self) -> bool:
        """Returns True if the caller is allowed to attempt a trial call.
        Only called when the breaker is not (or was not, moments ago)
        CLOSED - i.e. state is OPEN or HALF_OPEN."""
        async with self._lock:
            if self._state == CircuitState.CLOSED:
                # State changed concurrently (e.g. another coroutine's
                # success closed it) between our outer check and the lock.
                return True

            if self._state == CircuitState.OPEN:
                assert self._opened_at is not None
                if time.monotonic() - self._opened_at >= self._config.recovery_timeout:
                    self._transition(CircuitState.HALF_OPEN)
                    self._half_open_inflight = 0
                else:
                    return False

            # self._state is now HALF_OPEN (either already was, or just
            # transitioned above).
            if self._half_open_inflight < self._config.half_open_max_trials:
                self._half_open_inflight += 1
                return True
            return False

    async def allow_request(self) -> bool:
        """Check (and possibly transition state) whether Redis should be
        attempted for this request. Does not mutate on the CLOSED fast path
        beyond a lock-free read, to keep the hot path cheap."""
        if self._state == CircuitState.CLOSED:
            return True
        return await self._maybe_enter_half_open()

    async def call(self, coro_fn: Callable[[], Awaitable[T]]) -> T:
        """Execute `coro_fn` guarded by the breaker. Raises CircuitOpenError
        if the circuit is OPEN (or HALF_OPEN with no trial slots available)
        so the caller can fall back to local rate limiting."""
        if not await self.allow_request():
            raise CircuitOpenError("circuit is open; redis calls suspended")

        try:
            result = await coro_fn()
        except Exception:
            await self._record_failure()
            raise
        else:
            await self._record_success()
            return result

    async def _health_check_loop(self) -> None:
        """While OPEN, periodically PING Redis in the background so recovery
        doesn't depend purely on live traffic hitting the half-open window."""
        try:
            while True:
                await asyncio.sleep(self._config.health_check_interval)
                if self._state != CircuitState.OPEN:
                    continue
                try:
                    healthy = await self._ping_fn()
                except Exception:
                    healthy = False
                if healthy:
                    async with self._lock:
                        if self._state == CircuitState.OPEN:
                            self._transition(CircuitState.HALF_OPEN)
                            self._half_open_inflight = 0
        except asyncio.CancelledError:
            raise
