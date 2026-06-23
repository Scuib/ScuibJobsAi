"""
ingestion/ingesters.py

Concrete BaseIngester implementations — enterprise-grade.
- ManualIngester: paste raw text via API (unchanged)
- IndeedRSSIngester: multi-query, offset pagination (up to 125 jobs/query)
- JSearchIngester: deep pagination, date filtering, retries, circuit breaker
"""

import asyncio
import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import AsyncIterator
import httpx

from core.interfaces import BaseIngester
from core.models import RawJob, JobSource
from core.resilience import CircuitBreaker, AdaptiveRateLimiter, retry_with_backoff

logger = logging.getLogger(__name__)


# ─── Phase 1: Manual paste ────────────────────────────────────────────────────

class ManualIngester(BaseIngester):
    """
    Phase 1 ingester. Accepts a raw text block directly (no external source).
    Used by the /ingest/manual endpoint for POC validation.
    """

    def __init__(self, raw_text: str, source_url: str | None = None):
        self._raw_text = raw_text
        self._source_url = source_url

    async def fetch(self) -> AsyncIterator[RawJob]:
        yield RawJob(
            source=JobSource.MANUAL,
            raw_text=self._raw_text,
            source_url=self._source_url,
        )

    async def health_check(self) -> bool:
        return bool(self._raw_text.strip())


# ─── Indeed RSS (Enterprise) ──────────────────────────────────────────────────

class IndeedRSSIngester(BaseIngester):
    """
    Polls Indeed's RSS endpoint for new job listings.

    Enterprise upgrades:
    - Multi-query support: pass a list of queries, fetch all
    - Offset-based pagination: Indeed RSS supports &start=0,10,20...
    - Up to `max_pages` pages per query (25 results/page)
    - Circuit breaker and rate limiting

    RSS format: https://www.indeed.com/rss?q={query}&l={location}&sort=date&start={offset}
    """

    RSS_BASE = "https://www.indeed.com/rss"
    RESULTS_PER_PAGE = 25  # Indeed RSS returns 25 per page

    def __init__(
        self,
        query: str,
        location: str = "remote",
        queries: list[str] | None = None,
        max_pages: int = 5,
        seen_ids: set[str] | None = None,
        request_timeout: int = 15,
        circuit_breaker: CircuitBreaker | None = None,
        rate_limiter: AdaptiveRateLimiter | None = None,
    ):
        # Support both single query and multi-query
        self.queries = queries or [query]
        self.location = location
        self.max_pages = max_pages
        self._seen_ids: set[str] = seen_ids or set()
        self._timeout = request_timeout
        self._circuit_breaker = circuit_breaker or CircuitBreaker(
            name="indeed_rss", failure_threshold=3, cooldown_seconds=60
        )
        self._rate_limiter = rate_limiter or AdaptiveRateLimiter(
            max_rpm=20, min_rpm=5
        )

    async def fetch(self) -> AsyncIterator[RawJob]:
        if not self._circuit_breaker.allow_request():
            logger.warning("IndeedRSSIngester: circuit breaker OPEN, skipping")
            return

        async with httpx.AsyncClient(timeout=self._timeout) as client:
            for query in self.queries:
                for page in range(self.max_pages):
                    start = page * self.RESULTS_PER_PAGE
                    try:
                        items = await self._fetch_page(client, query, start)
                        self._circuit_breaker.record_success()

                        if not items:
                            logger.info(
                                f"IndeedRSS: no results for query='{query}' "
                                f"at start={start}, stopping pagination"
                            )
                            break

                        for item in items:
                            if item["guid"] not in self._seen_ids:
                                self._seen_ids.add(item["guid"])
                                yield RawJob(
                                    source=JobSource.INDEED_RSS,
                                    external_id=item["guid"],
                                    raw_text=item["description"],
                                    source_url=item.get("link"),
                                    metadata={
                                        "title": item.get("title"),
                                        "pub_date": item.get("pubDate"),
                                        "query": query,
                                    },
                                )

                        # If we got fewer than expected, no more pages
                        if len(items) < self.RESULTS_PER_PAGE:
                            break

                    except Exception as e:
                        self._circuit_breaker.record_failure()
                        logger.error(
                            f"IndeedRSS: fetch failed for query='{query}' "
                            f"page={page}: {e}"
                        )
                        if not self._circuit_breaker.allow_request():
                            logger.warning(
                                "IndeedRSS: circuit breaker tripped, aborting"
                            )
                            return
                        break  # Move to next query on failure

                    # Rate limiting between pages
                    if page < self.max_pages - 1:
                        await self._rate_limiter.acquire()

    async def _fetch_page(
        self, client: httpx.AsyncClient, query: str, start: int
    ) -> list[dict]:
        """Fetch a single page of RSS results with retry."""
        url = (
            f"{self.RSS_BASE}?q={query}&l={self.location}"
            f"&sort=date&start={start}"
        )

        async def _do_fetch():
            await self._rate_limiter.acquire()
            response = await client.get(
                url, headers={"User-Agent": "Mozilla/5.0"}
            )
            response.raise_for_status()
            self._rate_limiter.record_success()
            return self._parse_rss(response.text)

        return await retry_with_backoff(
            _do_fetch,
            max_retries=3,
            base_delay=2.0,
            retry_on=(httpx.HTTPError,),
            operation_name=f"indeed_rss_{query}_start{start}",
        )

    def _parse_rss(self, xml_text: str) -> list[dict]:
        items = []
        try:
            root = ET.fromstring(xml_text)
            channel = root.find("channel")
            if channel is None:
                return items
            for item in channel.findall("item"):
                items.append({
                    "title":       self._text(item, "title"),
                    "description": self._text(item, "description"),
                    "link":        self._text(item, "link"),
                    "guid":        self._text(item, "guid"),
                    "pubDate":     self._text(item, "pubDate"),
                })
        except ET.ParseError as e:
            logger.error(f"RSS XML parse error: {e}")
        return items

    @staticmethod
    def _text(el: ET.Element, tag: str) -> str:
        child = el.find(tag)
        return child.text.strip() if child is not None and child.text else ""

    async def health_check(self) -> bool:
        url = f"{self.RSS_BASE}?q=software+engineer&l=remote&sort=date"
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                r = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
                return r.status_code == 200
        except Exception:
            return False


# ─── JSearch API (Enterprise) ────────────────────────────────────────────────

class JSearchIngester(BaseIngester):
    """
    Pulls jobs from the JSearch API (RapidAPI).
    Covers Indeed, LinkedIn, Glassdoor, and others.
    https://rapidapi.com/letscrape-6bRBa3QguO5/api/jsearch

    Enterprise upgrades:
    - Deep pagination: default 10 pages (100+ jobs per query)
    - Date filtering: today, 3days, week, month
    - Employment type filtering: FULLTIME, CONTRACTOR, PARTTIME, INTERN
    - Per-page retry with exponential backoff
    - Circuit breaker: skip source after 3 consecutive failures
    - Adaptive rate limiting: backs off on 429s
    - Response metadata tracking

    Requires a RapidAPI key.
    """

    BASE_URL = "https://jsearch.p.rapidapi.com/search"

    def __init__(
        self,
        api_key: str,
        query: str,
        location: str = "United States",
        remote_only: bool = False,
        pages: int = 10,
        date_posted: str = "week",
        employment_types: str | None = None,
        seen_ids: set[str] | None = None,
        circuit_breaker: CircuitBreaker | None = None,
        rate_limiter: AdaptiveRateLimiter | None = None,
    ):
        self.headers = {
            "X-RapidAPI-Key": api_key,
            "X-RapidAPI-Host": "jsearch.p.rapidapi.com",
        }
        self.query = query
        self.location = location
        self.remote_only = remote_only
        self.pages = pages
        self.date_posted = date_posted
        self.employment_types = employment_types  # e.g., "FULLTIME,CONTRACTOR"
        self._seen_ids: set[str] = seen_ids or set()
        self._circuit_breaker = circuit_breaker or CircuitBreaker(
            name="jsearch", failure_threshold=3, cooldown_seconds=60
        )
        self._rate_limiter = rate_limiter or AdaptiveRateLimiter(
            max_rpm=30, min_rpm=5
        )

        # Tracking
        self._total_available: int | None = None  # From API response metadata

    async def fetch(self) -> AsyncIterator[RawJob]:
        if not self._circuit_breaker.allow_request():
            logger.warning("JSearchIngester: circuit breaker OPEN, skipping")
            return

        async with httpx.AsyncClient(timeout=20) as client:
            for page in range(1, self.pages + 1):
                try:
                    data = await self._fetch_page(client, page)
                    self._circuit_breaker.record_success()

                    jobs = data.get("data", [])
                    if not jobs:
                        logger.info(
                            f"JSearch: empty page {page}, stopping pagination"
                        )
                        break

                    # Track total available from API metadata
                    if page == 1 and "parameters" in data:
                        est_total = data.get("parameters", {}).get(
                            "estimated_total_results"
                        )
                        if est_total:
                            self._total_available = int(est_total)
                            logger.info(
                                f"JSearch: ~{self._total_available} total results "
                                f"available for query='{self.query}'"
                            )

                    for job in jobs:
                        ext_id = job.get("job_id", "")
                        if ext_id in self._seen_ids:
                            continue
                        self._seen_ids.add(ext_id)

                        raw_text = self._flatten_job(job)
                        yield RawJob(
                            source=JobSource.JSEARCH_API,
                            external_id=ext_id,
                            raw_text=raw_text,
                            source_url=job.get("job_apply_link"),
                            metadata={
                                "employer": job.get("employer_name"),
                                "posted_at": job.get("job_posted_at_datetime_utc"),
                                "query": self.query,
                            },
                        )

                except Exception as e:
                    self._circuit_breaker.record_failure()
                    logger.error(f"JSearch: page {page} failed: {e}")
                    if not self._circuit_breaker.allow_request():
                        logger.warning(
                            "JSearch: circuit breaker tripped, aborting"
                        )
                        break
                    continue

                # Polite rate limiting between pages (1.5s to avoid 429s)
                if page < self.pages:
                    await self._rate_limiter.acquire()

    async def _fetch_page(self, client: httpx.AsyncClient, page: int) -> dict:
        """Fetch a single page from JSearch with retry and rate limiting."""
        params = {
            "query": self.query,
            "page": str(page),
            "num_pages": "1",
            "remote_jobs_only": str(self.remote_only).lower(),
            "date_posted": self.date_posted,
        }
        if self.employment_types:
            params["employment_types"] = self.employment_types

        async def _do_fetch():
            await self._rate_limiter.acquire()
            response = await client.get(
                self.BASE_URL,
                headers=self.headers,
                params=params,
            )

            if response.status_code == 429:
                self._rate_limiter.record_throttle()
                raise httpx.HTTPStatusError(
                    "Rate limited",
                    request=response.request,
                    response=response,
                )

            response.raise_for_status()
            self._rate_limiter.record_success()
            return response.json()

        return await retry_with_backoff(
            _do_fetch,
            max_retries=3,
            base_delay=2.0,
            max_delay=15.0,
            retry_on=(httpx.HTTPError, httpx.HTTPStatusError, ValueError),
            operation_name=f"jsearch_page_{page}",
        )

    def _flatten_job(self, job: dict) -> str:
        """Serialize JSearch job dict to plain text for LLM parsing."""
        parts = [
            f"Title: {job.get('job_title', '')}",
            f"Company: {job.get('employer_name', '')}",
            f"Location: {job.get('job_city', '')} {job.get('job_state', '')} {job.get('job_country', '')}",
            f"Remote: {job.get('job_is_remote', False)}",
            f"Employment Type: {job.get('job_employment_type', '')}",
            f"Description:\n{job.get('job_description', '')}",
        ]
        if job.get("job_min_salary"):
            parts.append(f"Salary: {job['job_min_salary']} - {job.get('job_max_salary', '')} {job.get('job_salary_currency', 'USD')} {job.get('job_salary_period', '')}")

        # Include qualifications if available
        highlights = job.get("job_highlights") or {}
        qualifications = highlights.get("Qualifications", [])
        if qualifications:
            parts.append(f"Qualifications:\n" + "\n".join(f"- {q}" for q in qualifications))

        responsibilities = highlights.get("Responsibilities", [])
        if responsibilities:
            parts.append(f"Responsibilities:\n" + "\n".join(f"- {r}" for r in responsibilities))

        benefits = highlights.get("Benefits", [])
        if benefits:
            parts.append(f"Benefits:\n" + "\n".join(f"- {b}" for b in benefits))

        return "\n".join(parts)

    async def health_check(self) -> bool:
        params = {"query": "test", "page": "1", "num_pages": "1"}
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                r = await client.get(self.BASE_URL, headers=self.headers, params=params)
                return r.status_code in (200, 429)  # 429 = rate limited but alive
        except Exception:
            return False
