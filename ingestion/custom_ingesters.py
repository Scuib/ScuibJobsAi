"""
ingestion/custom_ingesters.py

Custom ingesters for specific job boards discovered through reconnaissance.
- WorkableIngester: clean JSON REST API at jobs.workable.com
- MyJobMagIngester: HTML scrape of myjobmag.com
- FuzuIngester: HTML scrape of fuzu.com
- JobGurusIngester: HTML scrape of jobgurus.com.ng
- JobbermanIngester: HTML scrape of jobberman.com
"""

import asyncio
import json
import logging
import re
from datetime import datetime
from typing import AsyncIterator
import httpx
from bs4 import BeautifulSoup

from core.interfaces import BaseIngester
from core.models import RawJob, JobSource
from core.resilience import CircuitBreaker, AdaptiveRateLimiter, retry_with_backoff

logger = logging.getLogger(__name__)


class WorkableIngester(BaseIngester):
    """
    Fetches jobs from Workable's public JSON API.
    https://jobs.workable.com/api/v1/jobs?query=...&location=...
    No API key required. Pagination via nextPageToken.
    """

    BASE_URL = "https://jobs.workable.com/api/v1/jobs"

    def __init__(
        self,
        query: str = "software engineer",
        location: str = "",
        max_pages: int = 5,
        seen_ids: set[str] | None = None,
        circuit_breaker: CircuitBreaker | None = None,
        rate_limiter: AdaptiveRateLimiter | None = None,
    ):
        self.query = query
        self.location = location
        self.max_pages = max_pages
        self._seen_ids: set[str] = seen_ids or set()
        self._circuit_breaker = circuit_breaker or CircuitBreaker(
            name="workable", failure_threshold=3, cooldown_seconds=60
        )
        self._rate_limiter = rate_limiter or AdaptiveRateLimiter(
            max_rpm=30, min_rpm=5
        )

    async def fetch(self) -> AsyncIterator[RawJob]:
        if not self._circuit_breaker.allow_request():
            logger.warning("WorkableIngester: circuit breaker OPEN, skipping")
            return

        page_token = None
        pages_fetched = 0

        async with httpx.AsyncClient(timeout=15) as client:
            while pages_fetched < self.max_pages:
                try:
                    data = await self._fetch_page(client, page_token)
                    self._circuit_breaker.record_success()
                    pages_fetched += 1

                    for job in data.get("jobs", []):
                        ext_id = job.get("id", "")
                        if ext_id in self._seen_ids:
                            continue
                        self._seen_ids.add(ext_id)

                        raw_text = self._job_to_text(job)
                        yield RawJob(
                            source=JobSource.WORKABLE,
                            external_id=ext_id,
                            raw_text=raw_text,
                            source_url=job.get("url"),
                            metadata={
                                "title": job.get("title"),
                                "company": job.get("company", {}).get("title"),
                                "location": job.get("location"),
                                "employment_type": job.get("employmentType"),
                                "workplace": job.get("workplace"),
                                "published_at": job.get("created"),
                                "company_info": job.get("company"),
                            },
                        )

                    page_token = data.get("nextPageToken")
                    if not page_token or not data.get("jobs"):
                        break

                except Exception as e:
                    self._circuit_breaker.record_failure()
                    logger.error(f"WorkableIngester: page failed: {e}")
                    if not self._circuit_breaker.allow_request():
                        break
                    break

    async def _fetch_page(self, client: httpx.AsyncClient, page_token: str | None) -> dict:
        params = {"query": self.query}
        if self.location:
            params["location"] = self.location
        if page_token:
            params["pageToken"] = page_token

        async def _do_fetch():
            await self._rate_limiter.acquire()
            response = await client.get(
                self.BASE_URL,
                params=params,
                headers={"User-Agent": "Mozilla/5.0"},
            )

            if response.status_code == 429:
                self._rate_limiter.record_throttle()
                raise httpx.HTTPStatusError("Rate limited", request=response.request, response=response)

            response.raise_for_status()
            self._rate_limiter.record_success()
            return response.json()

        return await retry_with_backoff(
            _do_fetch,
            max_retries=3,
            base_delay=2.0,
            retry_on=(httpx.HTTPError, httpx.HTTPStatusError),
            operation_name=f"workable_{self.query}_page_{page_token[:20] if page_token else 'first'}",
        )

    @staticmethod
    def _job_to_text(job: dict) -> str:
        """Serialize Workable job to plain text for LLM parsing."""
        company = job.get("company", {}) or {}
        loc = job.get("location", {}) or {}
        parts = [
            f"Title: {job.get('title', '')}",
            f"Company: {company.get('title', '')}",
            f"Location: {loc.get('city', '')}, {loc.get('subregion', '')}, {loc.get('countryName', '')}",
            f"Workplace: {job.get('workplace', '')}",
            f"Employment Type: {job.get('employmentType', '')}",
            f"Description:\n{job.get('description', '')}",
        ]
        return "\n".join(parts)

    async def health_check(self) -> bool:
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                r = await client.get(
                    self.BASE_URL,
                    params={"query": "test"},
                    headers={"User-Agent": "Mozilla/5.0"},
                )
                return r.status_code == 200
        except Exception:
            return False


class _BaseHTMLIngester(BaseIngester):
    """
    Base class for ingesters that scrape HTML job boards.
    Subclasses define search_url_pattern and job_link_pattern.
    """

    SEARCH_URL: str = ""
    JOB_LINK_SELECTOR: str = ""
    DETAIL_CONTENT_SELECTOR: str = ""

    def __init__(
        self,
        source: JobSource,
        query: str,
        location: str = "",
        max_pages: int = 3,
        seen_ids: set[str] | None = None,
        request_timeout: int = 30,
        circuit_breaker: CircuitBreaker | None = None,
        rate_limiter: AdaptiveRateLimiter | None = None,
    ):
        self.source = source
        self.query = query
        self.location = location
        self.max_pages = max_pages
        self._seen_ids: set[str] = seen_ids or set()
        self._timeout = request_timeout
        self._circuit_breaker = circuit_breaker or CircuitBreaker(
            name=source.value, failure_threshold=3, cooldown_seconds=60
        )
        self._rate_limiter = rate_limiter or AdaptiveRateLimiter(
            max_rpm=15, min_rpm=3
        )

    async def fetch(self) -> AsyncIterator[RawJob]:
        if not self._circuit_breaker.allow_request():
            logger.warning(f"{type(self).__name__}: circuit breaker OPEN, skipping")
            return

        async with httpx.AsyncClient(timeout=self._timeout, follow_redirects=True) as client:
            for page in range(1, self.max_pages + 1):
                try:
                    job_links = await self._fetch_job_links(client, page)
                    self._circuit_breaker.record_success()

                    if not job_links:
                        break

                    for title, url in job_links:
                        try:
                            raw_text = await self._fetch_detail(client, url)
                            ext_id = url if isinstance(url, str) else str(url)
                            if ext_id in self._seen_ids:
                                continue
                            self._seen_ids.add(ext_id)

                            yield RawJob(
                                source=self.source,
                                external_id=ext_id,
                                raw_text=raw_text,
                                source_url=url,
                                metadata={"title": title, "query": self.query},
                            )
                            await asyncio.sleep(0.5)
                        except Exception as e:
                            logger.warning(f"{type(self).__name__}: detail fetch failed for {url}: {e}")

                except Exception as e:
                    self._circuit_breaker.record_failure()
                    logger.error(f"{type(self).__name__}: page {page} failed: {e}")
                    if not self._circuit_breaker.allow_request():
                        break
                    break

    async def _fetch_job_links(self, client: httpx.AsyncClient, page: int) -> list[tuple[str, str]]:
        """Fetch search page and extract (title, url) tuples. Override in subclass."""
        raise NotImplementedError

    async def _fetch_detail(self, client: httpx.AsyncClient, url: str) -> str:
        """Fetch job detail page and extract raw text. Override in subclass."""
        raise NotImplementedError

    async def health_check(self) -> bool:
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                r = await client.get(
                    self.SEARCH_URL,
                    headers={"User-Agent": "Mozilla/5.0"},
                )
                return r.status_code == 200
        except Exception:
            return False


class MyJobMagIngester(_BaseHTMLIngester):
    """
    Scrapes job listings from www.myjobmag.com (Nigerian job board).
    Search: https://www.myjobmag.com/search/jobs?q={query}
    Detail: /job/{slug} page, content in .read-job-section-in or .job-details
    """

    SEARCH_URL = "https://www.myjobmag.com/search/jobs"

    def __init__(self, query: str = "software", max_pages: int = 3, **kwargs):
        super().__init__(
            source=JobSource.MYJOBMAG,
            query=query,
            max_pages=max_pages,
            **kwargs,
        )

    async def _fetch_job_links(self, client: httpx.AsyncClient, page: int) -> list[tuple[str, str]]:
        params = {"q": self.query, "page": page} if page > 1 else {"q": self.query}
        response = await client.get(
            self.SEARCH_URL,
            params=params,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        )
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.text.strip()
            if href.startswith("/job/") and len(text) > 5:
                full_url = f"https://www.myjobmag.com{href}"
                links.append((text, full_url))
        return links

    async def _fetch_detail(self, client: httpx.AsyncClient, url: str) -> str:
        response = await client.get(
            url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        )
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        content = ""
        for selector in ["read-job-section-in", "job-details", "mag-body"]:
            el = soup.find(class_=selector)
            if el:
                content = el.get_text(separator="\n", strip=True)
                break

        if not content:
            content = soup.get_text(separator="\n", strip=True)

        title = soup.title.text.strip() if soup.title else ""
        return f"Title: {title}\n\n{content}"


class FuzuIngester(_BaseHTMLIngester):
    """
    Scrapes job listings from www.fuzu.com.
    Uses the homepage job feed. Falls back to basic search patterns.
    """

    SEARCH_URL = "https://www.fuzu.com"

    def __init__(self, query: str = "software", location: str = "nigeria", max_pages: int = 2, **kwargs):
        super().__init__(
            source=JobSource.FUZU,
            query=query,
            location=location,
            max_pages=max_pages,
            **kwargs,
        )

    async def _fetch_job_links(self, client: httpx.AsyncClient, page: int) -> list[tuple[str, str]]:
        urls_to_try = [
            f"https://www.fuzu.com/{self.location}/search?q={self.query}&page={page}" if page > 1
            else f"https://www.fuzu.com/{self.location}/search?q={self.query}",
            f"https://www.fuzu.com/{self.location}/jobs?page={page}" if page > 1
            else f"https://www.fuzu.com/{self.location}/jobs",
            f"https://www.fuzu.com/search?q={self.query}&location={self.location}&page={page}" if page > 1
            else f"https://www.fuzu.com/search?q={self.query}&location={self.location}",
        ]

        for url in urls_to_try:
            try:
                response = await client.get(
                    url,
                    headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
                    timeout=10,
                )
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, "html.parser")
                    links = []
                    for a in soup.find_all("a", href=True):
                        href = a["href"]
                        text = a.text.strip()
                        if "/job/" in href and len(text) > 5:
                            full_url = href if href.startswith("http") else f"https://www.fuzu.com{href}"
                            links.append((text, full_url))
                    if links:
                        return links
            except Exception:
                continue
        return []

    async def _fetch_detail(self, client: httpx.AsyncClient, url: str) -> str:
        response = await client.get(
            url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        )
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        content = ""
        for selector in ["job-description", "job-detail", "description", "content", "main"]:
            el = soup.find(class_=selector) or soup.find(id=selector)
            if el:
                content = el.get_text(separator="\n", strip=True)
                break

        if not content:
            content = soup.get_text(separator="\n", strip=True)

        title = soup.title.text.strip() if soup.title else ""
        return f"Title: {title}\n\n{content}"


class JobGurusIngester(_BaseHTMLIngester):
    """
    Scrapes job listings from www.jobgurus.com.ng (Nigerian job board).
    Jobs page: https://www.jobgurus.com.ng/jobs
    """

    SEARCH_URL = "https://www.jobgurus.com.ng/jobs"

    def __init__(self, query: str = "software", max_pages: int = 2, **kwargs):
        super().__init__(
            source=JobSource.JOBGURUS,
            query=query,
            max_pages=max_pages,
            **kwargs,
        )

    async def _fetch_job_links(self, client: httpx.AsyncClient, page: int) -> list[tuple[str, str]]:
        params = {"q": self.query, "page": page} if page > 1 else {"q": self.query}
        response = await client.get(
            self.SEARCH_URL,
            params=params,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        )
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.text.strip()
            if any(p in href for p in ["/job/", "/vacancy/", ".html"]) and len(text) > 5:
                full_url = href if href.startswith("http") else f"https://www.jobgurus.com.ng{href}"
                links.append((text, full_url))
        return links

    async def _fetch_detail(self, client: httpx.AsyncClient, url: str) -> str:
        response = await client.get(
            url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        )
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        content = ""
        for selector in ["job-details", "job-description", "single-content", "content", "main", "entry-content"]:
            el = soup.find(class_=selector)
            if el:
                content = el.get_text(separator="\n", strip=True)
                break

        if not content:
            content = soup.get_text(separator="\n", strip=True)

        title = soup.title.text.strip() if soup.title else ""
        return f"Title: {title}\n\n{content}"


class JobbermanIngester(_BaseHTMLIngester):
    """
    Scrapes job listings from www.jobberman.com (Nigerian job board).
    Search: https://www.jobberman.com/jobs?q={query}
    """

    SEARCH_URL = "https://www.jobberman.com/jobs"

    def __init__(self, query: str = "software", max_pages: int = 2, **kwargs):
        super().__init__(
            source=JobSource.JOBBERMAN,
            query=query,
            max_pages=max_pages,
            **kwargs,
        )

    async def _fetch_job_links(self, client: httpx.AsyncClient, page: int) -> list[tuple[str, str]]:
        params = {"q": self.query}
        response = await client.get(
            self.SEARCH_URL,
            params=params,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        )
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.text.strip()
            if "/job/" in href and len(text) > 5:
                full_url = href if href.startswith("http") else f"https://www.jobberman.com{href}"
                links.append((text, full_url))
        return links

    async def _fetch_detail(self, client: httpx.AsyncClient, url: str) -> str:
        response = await client.get(
            url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        )
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        content = ""
        for selector in ["job-description", "job-detail", "description", "content", "main", "job-body"]:
            el = soup.find(class_=selector)
            if el:
                content = el.get_text(separator="\n", strip=True)
                break

        if not content:
            content = soup.get_text(separator="\n", strip=True)

        title = soup.title.text.strip() if soup.title else ""
        return f"Title: {title}\n\n{content}"
