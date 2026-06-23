"""
core/resilience.py

Enterprise-grade resilience primitives for high-throughput job ingestion.
- CircuitBreaker: skip failing sources after N consecutive failures
- AdaptiveRateLimiter: token-bucket rate limiter that backs off on 429s
- RetryWithBackoff: retry decorator with exponential backoff + jitter
"""

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps
from typing import Any, Callable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


# ─── Circuit Breaker ─────────────────────────────────────────────────────────


class CircuitState(str, Enum):
    CLOSED = "closed"      # Normal operation — requests flow through
    OPEN = "open"          # Tripped — all requests fast-fail
    HALF_OPEN = "half_open"  # Probing — allow one request to test recovery


@dataclass
class CircuitBreaker:
    """
    Tracks consecutive failures per source. Opens the circuit after
    `failure_threshold` failures, fast-fails all calls for `cooldown_seconds`,
    then moves to half-open to probe recovery.

    Usage:
        cb = CircuitBreaker(name="jsearch", failure_threshold=3, cooldown_seconds=60)
        if cb.allow_request():
            try:
                result = await fetch()
                cb.record_success()
            except Exception:
                cb.record_failure()
        else:
            logger.warning("Circuit open, skipping jsearch")
    """

    name: str
    failure_threshold: int = 3
    cooldown_seconds: float = 60.0

    # Internal state
    _state: CircuitState = field(default=CircuitState.CLOSED, init=False)
    _failure_count: int = field(default=0, init=False)
    _last_failure_time: float = field(default=0.0, init=False)
    _success_count: int = field(default=0, init=False)
    _total_failures: int = field(default=0, init=False)

    @property
    def state(self) -> CircuitState:
        if self._state == CircuitState.OPEN:
            elapsed = time.monotonic() - self._last_failure_time
            if elapsed >= self.cooldown_seconds:
                self._state = CircuitState.HALF_OPEN
                logger.info(
                    f"CircuitBreaker[{self.name}]: cooldown expired, "
                    f"moving to HALF_OPEN (will probe next request)"
                )
        return self._state

    def allow_request(self) -> bool:
        """Returns True if the circuit allows a request through."""
        current = self.state
        if current == CircuitState.CLOSED:
            return True
        if current == CircuitState.HALF_OPEN:
            return True  # Allow one probe request
        return False  # OPEN — fast fail

    def record_success(self) -> None:
        """Record a successful call. Resets failure count, closes circuit."""
        self._failure_count = 0
        self._success_count += 1
        if self._state in (CircuitState.HALF_OPEN, CircuitState.OPEN):
            logger.info(
                f"CircuitBreaker[{self.name}]: probe succeeded, "
                f"circuit CLOSED (recovered)"
            )
        self._state = CircuitState.CLOSED

    def record_failure(self) -> None:
        """Record a failed call. May trip the circuit to OPEN."""
        self._failure_count += 1
        self._total_failures += 1
        self._last_failure_time = time.monotonic()

        if self._state == CircuitState.HALF_OPEN:
            self._state = CircuitState.OPEN
            logger.warning(
                f"CircuitBreaker[{self.name}]: half-open probe FAILED, "
                f"circuit re-opened for {self.cooldown_seconds}s"
            )
        elif self._failure_count >= self.failure_threshold:
            self._state = CircuitState.OPEN
            logger.warning(
                f"CircuitBreaker[{self.name}]: {self._failure_count} consecutive "
                f"failures — circuit OPEN for {self.cooldown_seconds}s"
            )

    def stats(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "state": self.state.value,
            "consecutive_failures": self._failure_count,
            "total_failures": self._total_failures,
            "total_successes": self._success_count,
        }


# ─── Adaptive Rate Limiter ───────────────────────────────────────────────────


class AdaptiveRateLimiter:
    """
    Token-bucket rate limiter that auto-adjusts when it detects 429 responses.

    - Normal mode: allows `max_rpm` requests per minute
    - Backoff mode: halves the rate on each 429, recovers gradually on success
    - Minimum floor: never goes below `min_rpm`

    Usage:
        limiter = AdaptiveRateLimiter(max_rpm=60)
        await limiter.acquire()  # blocks until a token is available
        try:
            response = await make_request()
            limiter.record_success()
        except RateLimitError:
            limiter.record_throttle()
    """

    def __init__(
        self,
        max_rpm: int = 60,
        min_rpm: int = 5,
        recovery_factor: float = 1.2,
    ):
        self.max_rpm = max_rpm
        self.min_rpm = min_rpm
        self.recovery_factor = recovery_factor
        self._current_rpm = float(max_rpm)
        self._lock = asyncio.Lock()
        self._last_request_time: float = 0.0
        self._total_throttles: int = 0

    @property
    def current_interval(self) -> float:
        """Seconds between allowed requests at current rate."""
        return 60.0 / self._current_rpm

    async def acquire(self) -> None:
        """Wait until a request is allowed under the current rate."""
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_request_time
            wait_time = self.current_interval - elapsed

            if wait_time > 0:
                await asyncio.sleep(wait_time)

            self._last_request_time = time.monotonic()

    def record_success(self) -> None:
        """Gradually recover rate toward max after success."""
        if self._current_rpm < self.max_rpm:
            self._current_rpm = min(
                self.max_rpm,
                self._current_rpm * self.recovery_factor,
            )

    def record_throttle(self) -> None:
        """Back off aggressively on rate limit hit (halve the rate)."""
        self._total_throttles += 1
        old_rpm = self._current_rpm
        self._current_rpm = max(self.min_rpm, self._current_rpm / 2)
        logger.warning(
            f"RateLimiter: throttled! Rate reduced {old_rpm:.0f} → "
            f"{self._current_rpm:.0f} RPM (total throttles: {self._total_throttles})"
        )

    def stats(self) -> dict[str, Any]:
        return {
            "current_rpm": round(self._current_rpm, 1),
            "max_rpm": self.max_rpm,
            "total_throttles": self._total_throttles,
            "interval_seconds": round(self.current_interval, 3),
        }


# ─── Retry with Backoff ─────────────────────────────────────────────────────


async def retry_with_backoff(
    coro_fn: Callable[..., Any],
    *args: Any,
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    jitter: bool = True,
    retry_on: tuple[type, ...] = (Exception,),
    operation_name: str = "operation",
    **kwargs: Any,
) -> Any:
    """
    Retry an async callable with exponential backoff and optional jitter.

    Args:
        coro_fn: Async function to retry
        max_retries: Maximum number of retry attempts
        base_delay: Initial delay in seconds (doubles each retry)
        max_delay: Cap on delay between retries
        jitter: Add random jitter to prevent thundering herd
        retry_on: Tuple of exception types to retry on
        operation_name: For logging
    """
    last_exception = None

    for attempt in range(1, max_retries + 1):
        try:
            return await coro_fn(*args, **kwargs)
        except retry_on as e:
            last_exception = e
            if attempt == max_retries:
                logger.error(
                    f"Retry[{operation_name}]: all {max_retries} attempts "
                    f"exhausted. Last error: {e}"
                )
                raise

            delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
            if jitter:
                delay = delay * (0.5 + random.random())

            logger.warning(
                f"Retry[{operation_name}]: attempt {attempt}/{max_retries} "
                f"failed ({type(e).__name__}: {e}). "
                f"Retrying in {delay:.1f}s..."
            )
            await asyncio.sleep(delay)

    raise last_exception  # Should never reach here, but satisfies type checker
