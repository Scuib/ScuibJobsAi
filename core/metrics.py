"""
core/metrics.py

In-memory metrics collector for pipeline observability.
Tracks per-run stats, per-source rates, and parse latency distributions.
Exposes snapshots via get_snapshot() for the /metrics API endpoint.
"""

import time
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any


@dataclass
class RunMetrics:
    """Metrics for a single ingestion run."""

    run_id: str
    started_at: float = field(default_factory=time.monotonic)
    finished_at: float | None = None

    # Counters
    fetched: int = 0
    parsed: int = 0
    validated: int = 0
    flagged: int = 0
    errors: int = 0
    duplicates: int = 0

    # Per-source breakdown
    per_source: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    # Parse latencies in seconds
    parse_latencies: list[float] = field(default_factory=list)

    @property
    def duration_seconds(self) -> float:
        end = self.finished_at or time.monotonic()
        return round(end - self.started_at, 2)

    @property
    def jobs_per_second(self) -> float:
        d = self.duration_seconds
        return round(self.parsed / d, 2) if d > 0 else 0.0

    def record_parse_latency(self, latency: float) -> None:
        self.parse_latencies.append(latency)

    def latency_percentiles(self) -> dict[str, float]:
        """Compute p50, p95, p99 from recorded parse latencies."""
        if not self.parse_latencies:
            return {"p50": 0.0, "p95": 0.0, "p99": 0.0}

        sorted_latencies = sorted(self.parse_latencies)
        n = len(sorted_latencies)

        def percentile(p: float) -> float:
            idx = int(p / 100.0 * n)
            idx = min(idx, n - 1)
            return round(sorted_latencies[idx], 4)

        return {
            "p50": percentile(50),
            "p95": percentile(95),
            "p99": percentile(99),
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "duration_seconds": self.duration_seconds,
            "jobs_per_second": self.jobs_per_second,
            "fetched": self.fetched,
            "parsed": self.parsed,
            "validated": self.validated,
            "flagged": self.flagged,
            "errors": self.errors,
            "duplicates": self.duplicates,
            "per_source": dict(self.per_source),
            "latency": self.latency_percentiles(),
        }


class MetricsCollector:
    """
    Thread-safe, in-memory metrics store.
    Keeps the last N completed runs + the current active run.

    Usage:
        collector = MetricsCollector()
        run = collector.start_run("run-123")
        run.fetched += 1
        run.per_source["jsearch_api"] += 1
        collector.finish_run("run-123")
        snapshot = collector.get_snapshot()
    """

    MAX_HISTORY = 50  # Keep last 50 completed runs

    def __init__(self):
        self._lock = threading.Lock()
        self._active_runs: dict[str, RunMetrics] = {}
        self._completed_runs: list[dict[str, Any]] = []

        # Lifetime counters
        self._total_fetched: int = 0
        self._total_parsed: int = 0
        self._total_errors: int = 0
        self._total_runs: int = 0

        # Per-source lifetime counters
        self._source_totals: dict[str, int] = defaultdict(int)
        self._source_errors: dict[str, int] = defaultdict(int)

    def start_run(self, run_id: str) -> RunMetrics:
        """Start tracking a new ingestion run."""
        with self._lock:
            run = RunMetrics(run_id=run_id)
            self._active_runs[run_id] = run
            self._total_runs += 1
            return run

    def get_run(self, run_id: str) -> RunMetrics | None:
        """Get an active run's metrics (for progress polling)."""
        with self._lock:
            return self._active_runs.get(run_id)

    def finish_run(self, run_id: str) -> dict[str, Any] | None:
        """Mark a run as complete and archive its stats."""
        with self._lock:
            run = self._active_runs.pop(run_id, None)
            if not run:
                return None

            run.finished_at = time.monotonic()
            summary = run.to_dict()

            # Update lifetime counters
            self._total_fetched += run.fetched
            self._total_parsed += run.parsed
            self._total_errors += run.errors
            for source, count in run.per_source.items():
                self._source_totals[source] += count

            # Archive
            self._completed_runs.append(summary)
            if len(self._completed_runs) > self.MAX_HISTORY:
                self._completed_runs = self._completed_runs[-self.MAX_HISTORY:]

            return summary

    def record_source_error(self, source: str) -> None:
        """Track a source-level error (outside of a specific run)."""
        with self._lock:
            self._source_errors[source] += 1

    def get_snapshot(self) -> dict[str, Any]:
        """Full metrics snapshot for the /metrics endpoint."""
        with self._lock:
            active = {
                rid: run.to_dict()
                for rid, run in self._active_runs.items()
            }

            return {
                "lifetime": {
                    "total_runs": self._total_runs,
                    "total_fetched": self._total_fetched,
                    "total_parsed": self._total_parsed,
                    "total_errors": self._total_errors,
                },
                "per_source": {
                    "totals": dict(self._source_totals),
                    "errors": dict(self._source_errors),
                },
                "active_runs": active,
                "recent_runs": self._completed_runs[-10:],
            }


# ─── Singleton ────────────────────────────────────────────────────────────────

_global_collector: MetricsCollector | None = None


def get_metrics_collector() -> MetricsCollector:
    """Get or create the global metrics collector singleton."""
    global _global_collector
    if _global_collector is None:
        _global_collector = MetricsCollector()
    return _global_collector
