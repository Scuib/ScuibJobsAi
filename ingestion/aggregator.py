"""
ingestion/aggregator.py

Enterprise multi-source aggregation engine.
Runs N ingesters concurrently, deduplicates across sources,
and yields a unified stream of unique RawJob objects.

This is the core of the "hundreds of jobs per request" capability.
"""

import asyncio
import hashlib
import logging
from collections import defaultdict
from typing import AsyncIterator

from core.interfaces import BaseIngester
from core.models import RawJob
from core.resilience import CircuitBreaker

logger = logging.getLogger(__name__)


class MultiSourceAggregator(BaseIngester):
    """
    Enterprise aggregation engine. Runs N ingesters concurrently
    and yields deduplicated jobs from all sources.

    Capabilities:
    - Concurrent multi-source fetching (all sources run in parallel)
    - Cross-source deduplication by external_id + title/company fingerprint
    - Per-source balance caps so one fast board can't eat the whole target
    - Per-source circuit breakers (skip failing sources, don't block others)
    - Configurable target volume with early termination
    - Per-source metrics/counters

    Balancing: while fetching, each source may yield at most `max_per_source`
    jobs; extras are buffered. Once every source is exhausted, buffered jobs
    drain FIFO until the target is met. Result: every live source is
    represented, and slow sources aren't starved by fast ones.

    Fetch timeout: the fetch phase ends when ALL sources finish OR when
    `fetch_timeout_seconds` elapses — whichever comes first. Without this,
    one hanging board stalls parse+handoff for the whole run (and on
    free hosting the instance can sleep first, delivering zero jobs).

    Usage:
        aggregator = MultiSourceAggregator(
            ingesters=[jsearch, indeed, adzuna],
            target_count=200,
            max_per_source=70,
            fetch_timeout_seconds=600,
        )
        async for raw_job in aggregator.fetch():
            # Unique, source-balanced jobs
            ...
    """

    def __init__(
        self,
        ingesters: list[BaseIngester],
        target_count: int = 200,
        dedup_by_fingerprint: bool = True,
        max_per_source: int | None = None,
        fetch_timeout_seconds: float | None = None,
    ):
        self.ingesters = ingesters
        self.target_count = target_count
        self.dedup_by_fingerprint = dedup_by_fingerprint
        self.max_per_source = max_per_source
        self.fetch_timeout_seconds = fetch_timeout_seconds

        # Dedup state
        self._seen_external_ids: set[str] = set()
        self._seen_fingerprints: set[str] = set()

        # Balance buffer: capped-out sources park extras here for the fill phase
        self._overflow: list[RawJob] = []

        # Metrics
        self.per_source_counts: dict[str, int] = defaultdict(int)
        self.per_source_dupes: dict[str, int] = defaultdict(int)
        self.total_yielded: int = 0
        self.total_duplicates: int = 0

    async def fetch(self) -> AsyncIterator[RawJob]:
        """
        Run all ingesters concurrently, collect jobs into a shared queue,
        and yield deduplicated results until target_count is reached.
        """
        queue: asyncio.Queue[RawJob | None] = asyncio.Queue()
        active_sources = len(self.ingesters)
        finished_count = 0

        async def _run_ingester(ingester: BaseIngester, source_name: str):
            """Worker: fetch from a single source and put jobs on the queue."""
            nonlocal finished_count
            try:
                count = 0
                async for raw_job in ingester.fetch():
                    await queue.put(raw_job)
                    count += 1
                    # Check if we've already hit target (approximate — dedup
                    # may reduce the final count)
                    if self.total_yielded >= self.target_count:
                        logger.info(
                            f"Aggregator: target count {self.target_count} "
                            f"reached, {source_name} stopping early"
                        )
                        break
                logger.info(
                    f"Aggregator: {source_name} finished — "
                    f"produced {count} raw jobs"
                )
            except Exception as e:
                logger.error(
                    f"Aggregator: {source_name} failed with error: {e}"
                )
            finally:
                finished_count += 1
                await queue.put(None)  # Sentinel: this source is done

        # Launch all ingesters as concurrent tasks
        tasks = []
        for ingester in self.ingesters:
            source_name = type(ingester).__name__
            task = asyncio.create_task(
                _run_ingester(ingester, source_name)
            )
            tasks.append(task)

        # Consume from the shared queue. Stops when all sources are done
        # OR when the fetch timeout elapses (stalled boards must not hold
        # parse+handoff hostage — we proceed with whatever arrived).
        import time
        deadline = (
            time.monotonic() + self.fetch_timeout_seconds
            if self.fetch_timeout_seconds
            else None
        )
        timed_out = False
        sentinels_received = 0
        while sentinels_received < active_sources:
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    timed_out = True
                    break
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=remaining)
                except asyncio.TimeoutError:
                    timed_out = True
                    break
            else:
                item = await queue.get()

            if item is None:
                sentinels_received += 1
                continue

            # Early termination check
            if self.total_yielded >= self.target_count:
                continue  # Drain remaining queue items

            # Dedup by external_id
            if item.external_id:
                if item.external_id in self._seen_external_ids:
                    source_key = item.source.value
                    self.per_source_dupes[source_key] += 1
                    self.total_duplicates += 1
                    continue
                self._seen_external_ids.add(item.external_id)

            # Dedup by content fingerprint (catches same job from different sources)
            if self.dedup_by_fingerprint:
                fp = self._fingerprint(item)
                if fp in self._seen_fingerprints:
                    source_key = item.source.value
                    self.per_source_dupes[source_key] += 1
                    self.total_duplicates += 1
                    continue
                self._seen_fingerprints.add(fp)

            # Balance cap: park extras from full sources for the fill phase
            source_key = item.source.value
            if (
                self.max_per_source
                and self.per_source_counts[source_key] >= self.max_per_source
            ):
                self._overflow.append(item)
                continue

            # Yield the deduplicated job
            self.per_source_counts[source_key] += 1
            self.total_yielded += 1
            yield item

        # Fill phase: all sources exhausted — drain buffered overflow FIFO
        # until the target is met (no caps here).
        for item in self._overflow:
            if self.total_yielded >= self.target_count:
                break
            source_key = item.source.value
            self.per_source_counts[source_key] += 1
            self.total_yielded += 1
            yield item

        if timed_out:
            logger.warning(
                f"Aggregator: fetch timeout ({self.fetch_timeout_seconds}s) hit with "
                f"{sentinels_received}/{active_sources} sources finished — "
                f"proceeding with {self.total_yielded} jobs + "
                f"{len(self._overflow)} buffered"
            )

        # Cancel any still-running tasks
        for task in tasks:
            if not task.done():
                task.cancel()

        logger.info(
            f"Aggregator complete: yielded {self.total_yielded} unique jobs, "
            f"filtered {self.total_duplicates} duplicates. "
            f"Per source: {dict(self.per_source_counts)}"
        )

    @staticmethod
    def _fingerprint(job: RawJob) -> str:
        """
        Generate a content fingerprint for cross-source deduplication.
        Normalizes title + company from raw text for fuzzy matching.
        """
        # Extract first two lines (usually Title: ... and Company: ...)
        lines = job.raw_text.strip().split("\n")[:2]
        normalized = " ".join(lines).lower().strip()

        # Remove common noise
        for noise in ["title:", "company:", "-", "|", "  "]:
            normalized = normalized.replace(noise, " ")
        normalized = " ".join(normalized.split())  # Collapse whitespace

        return hashlib.md5(normalized.encode()).hexdigest()

    async def health_check(self) -> bool:
        """At least one ingester must be healthy."""
        checks = await asyncio.gather(
            *(ing.health_check() for ing in self.ingesters),
            return_exceptions=True,
        )
        healthy = sum(1 for c in checks if c is True)
        total = len(self.ingesters)
        logger.info(
            f"Aggregator health check: {healthy}/{total} sources healthy"
        )
        return healthy > 0

    def get_stats(self) -> dict:
        """Return aggregation stats for metrics/logging."""
        return {
            "total_yielded": self.total_yielded,
            "total_duplicates": self.total_duplicates,
            "per_source": dict(self.per_source_counts),
            "per_source_dupes": dict(self.per_source_dupes),
            "target_count": self.target_count,
        }
