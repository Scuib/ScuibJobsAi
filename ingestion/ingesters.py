"""
ingestion/ingesters.py

Concrete BaseIngester implementations.
Phase 1: ManualIngester (paste raw text via API)
Phase 2: IndeedRSSIngester, JSearchIngester
"""

import asyncio
import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import AsyncIterator
import httpx

from core.interfaces import BaseIngester
from core.models import RawJob, JobSource

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


# ─── Phase 2: Indeed RSS ──────────────────────────────────────────────────────

class IndeedRSSIngester(BaseIngester):
    """
    Polls Indeed's RSS endpoint for new job listings.
    Deduplicates via a seen_ids set (swap for Redis in production).

    RSS format: https://www.indeed.com/rss?q={query}&l={location}&sort=date
    """

    RSS_BASE = "https://www.indeed.com/rss"

    def __init__(
        self,
        query: str,
        location: str = "remote",
        seen_ids: set[str] | None = None,
        request_timeout: int = 15,
    ):
        self.query = query
        self.location = location
        self._seen_ids: set[str] = seen_ids or set()
        self._timeout = request_timeout

    async def fetch(self) -> AsyncIterator[RawJob]:
        url = f"{self.RSS_BASE}?q={self.query}&l={self.location}&sort=date"
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            try:
                response = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
                response.raise_for_status()
            except httpx.HTTPError as e:
                logger.error(f"IndeedRSS fetch failed: {e}")
                return

        items = self._parse_rss(response.text)
        for item in items:
            if item["guid"] not in self._seen_ids:
                self._seen_ids.add(item["guid"])
                yield RawJob(
                    source=JobSource.INDEED_RSS,
                    external_id=item["guid"],
                    raw_text=item["description"],
                    source_url=item.get("link"),
                    metadata={"title": item.get("title"), "pub_date": item.get("pubDate")},
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


# ─── Phase 2: JSearch API (RapidAPI) ─────────────────────────────────────────

class JSearchIngester(BaseIngester):
    """
    Pulls jobs from the JSearch API (RapidAPI).
    Covers Indeed, LinkedIn, Glassdoor, and others.
    https://rapidapi.com/letscrape-6bRBa3QguO5/api/jsearch

    Requires a RapidAPI key.
    """

    BASE_URL = "https://jsearch.p.rapidapi.com/search"

    def __init__(
        self,
        api_key: str,
        query: str,
        location: str = "United States",
        remote_only: bool = False,
        pages: int = 1,
        seen_ids: set[str] | None = None,
    ):
        self.headers = {
            "X-RapidAPI-Key": api_key,
            "X-RapidAPI-Host": "jsearch.p.rapidapi.com",
        }
        self.query = query
        self.location = location
        self.remote_only = remote_only
        self.pages = pages
        self._seen_ids: set[str] = seen_ids or set()

    async def fetch(self) -> AsyncIterator[RawJob]:
        async with httpx.AsyncClient(timeout=20) as client:
            for page in range(1, self.pages + 1):
                params = {
                    "query": self.query,
                    "page": str(page),
                    "num_pages": "1",
                    "remote_jobs_only": str(self.remote_only).lower(),
                }
                try:
                    response = await client.get(
                        self.BASE_URL,
                        headers=self.headers,
                        params=params,
                    )
                    response.raise_for_status()
                    data = response.json()
                except (httpx.HTTPError, ValueError) as e:
                    logger.error(f"JSearch fetch error page {page}: {e}")
                    continue

                for job in data.get("data", []):
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
                        },
                    )

                # Polite rate limiting between pages
                if page < self.pages:
                    await asyncio.sleep(0.5)

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
        return "\n".join(parts)

    async def health_check(self) -> bool:
        params = {"query": "test", "page": "1", "num_pages": "1"}
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                r = await client.get(self.BASE_URL, headers=self.headers, params=params)
                return r.status_code in (200, 429)  # 429 = rate limited but alive
        except Exception:
            return False
