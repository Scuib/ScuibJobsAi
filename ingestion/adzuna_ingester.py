"""
ingestion/adzuna_ingester.py

Adzuna API ingester — third job source for volume.
Free tier: 250 calls/day. Returns 50 jobs/page, up to 10 pages.
Covers US, UK, CA, AU, DE job markets.

API docs: https://developer.adzuna.com/overview
"""

import asyncio
import logging
from typing import AsyncIterator
import httpx

from core.interfaces import BaseIngester
from core.models import RawJob, JobSource
from core.resilience import CircuitBreaker, AdaptiveRateLimiter, retry_with_backoff

logger = logging.getLogger(__name__)


class AdzunaIngester(BaseIngester):
    """
    Pulls jobs from the Adzuna API.

    Volume: 50 results/page × 10 pages = 500 jobs per query.
    Supports category filtering and multi-country search.
    """

    BASE_URL = "https://api.adzuna.com/v1/api/jobs"

    # Adzuna category slugs for tech jobs
    TECH_CATEGORIES = ["it-jobs", "engineering-jobs"]

    def __init__(
        self,
        app_id: str,
        app_key: str,
        query: str,
        location: str = "United States",
        country: str = "us",
        pages: int = 5,
        results_per_page: int = 50,
        category: str | None = None,
        date_posted: str = "week",
        seen_ids: set[str] | None = None,
        circuit_breaker: CircuitBreaker | None = None,
        rate_limiter: AdaptiveRateLimiter | None = None,
    ):
        self.app_id = app_id
        self.app_key = app_key
        self.query = query
        self.location = location
        self.country = country
        self.pages = pages
        self.results_per_page = results_per_page
        self.category = category
        self.date_posted = date_posted
        self._seen_ids: set[str] = seen_ids or set()
        self._circuit_breaker = circuit_breaker or CircuitBreaker(
            name="adzuna", failure_threshold=3, cooldown_seconds=60
        )
        self._rate_limiter = rate_limiter or AdaptiveRateLimiter(
            max_rpm=30, min_rpm=5
        )

    def _max_days_from_filter(self) -> int:
        """Convert date_posted filter to max_days_old parameter."""
        mapping = {
            "today": 1,
            "3days": 3,
            "week": 7,
            "month": 30,
        }
        return mapping.get(self.date_posted, 7)

    async def fetch(self) -> AsyncIterator[RawJob]:
        if not self._circuit_breaker.allow_request():
            logger.warning("AdzunaIngester: circuit breaker OPEN, skipping")
            return

        async with httpx.AsyncClient(timeout=20) as client:
            for page in range(1, self.pages + 1):
                try:
                    jobs = await self._fetch_page(client, page)
                    self._circuit_breaker.record_success()

                    for job in jobs:
                        ext_id = job.get("id", "")
                        ext_id_str = str(ext_id)
                        if ext_id_str in self._seen_ids:
                            continue
                        self._seen_ids.add(ext_id_str)

                        raw_text = self._flatten_job(job)
                        yield RawJob(
                            source=JobSource.ADZUNA_API,
                            external_id=f"adzuna_{ext_id_str}",
                            raw_text=raw_text,
                            source_url=job.get("redirect_url"),
                            metadata={
                                "employer": (job.get("company") or {}).get("display_name"),
                                "posted_at": job.get("created"),
                                "category": (job.get("category") or {}).get("label"),
                                "contract_type": job.get("contract_type"),
                            },
                        )

                    # Check if we got fewer results than expected (last page)
                    if len(jobs) < self.results_per_page:
                        logger.info(
                            f"AdzunaIngester: page {page} returned {len(jobs)} results "
                            f"(< {self.results_per_page}), stopping pagination"
                        )
                        break

                except Exception as e:
                    self._circuit_breaker.record_failure()
                    logger.error(f"AdzunaIngester: page {page} failed: {e}")
                    if not self._circuit_breaker.allow_request():
                        logger.warning("AdzunaIngester: circuit breaker tripped, aborting")
                        break
                    continue

                # Polite rate limiting between pages
                if page < self.pages:
                    await self._rate_limiter.acquire()

    async def _fetch_page(self, client: httpx.AsyncClient, page: int) -> list[dict]:
        """Fetch a single page from Adzuna with retry."""
        url = f"{self.BASE_URL}/{self.country}/search/{page}"

        params = {
            "app_id": self.app_id,
            "app_key": self.app_key,
            "what": self.query,
            "where": self.location,
            "results_per_page": str(self.results_per_page),
            "max_days_old": str(self._max_days_from_filter()),
            "sort_by": "date",
            "content-type": "application/json",
        }

        if self.category:
            params["category"] = self.category

        async def _do_fetch():
            await self._rate_limiter.acquire()
            response = await client.get(url, params=params)

            if response.status_code == 429:
                self._rate_limiter.record_throttle()
                raise httpx.HTTPStatusError(
                    "Rate limited", request=response.request, response=response
                )

            response.raise_for_status()
            self._rate_limiter.record_success()
            data = response.json()
            return data.get("results", [])

        return await retry_with_backoff(
            _do_fetch,
            max_retries=3,
            base_delay=2.0,
            retry_on=(httpx.HTTPError, httpx.HTTPStatusError),
            operation_name=f"adzuna_page_{page}",
        )

    def _flatten_job(self, job: dict) -> str:
        """Serialize Adzuna job dict to plain text for LLM parsing."""
        company = (job.get("company") or {}).get("display_name", "")
        location = job.get("location", {}).get("display_name", "")
        category = (job.get("category") or {}).get("label", "")

        parts = [
            f"Title: {job.get('title', '')}",
            f"Company: {company}",
            f"Location: {location}",
            f"Category: {category}",
            f"Contract Type: {job.get('contract_type', '')}",
            f"Description:\n{job.get('description', '')}",
        ]

        salary_min = job.get("salary_min")
        salary_max = job.get("salary_max")
        if salary_min or salary_max:
            parts.append(
                f"Salary: {salary_min or '?'} - {salary_max or '?'} GBP yearly"
            )

        return "\n".join(parts)

    async def health_check(self) -> bool:
        url = f"{self.BASE_URL}/{self.country}/search/1"
        params = {
            "app_id": self.app_id,
            "app_key": self.app_key,
            "what": "software engineer",
            "results_per_page": "1",
        }
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                r = await client.get(url, params=params)
                return r.status_code in (200, 429)
        except Exception:
            return False
