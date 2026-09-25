"""
scripts/smoke_test.py

Offline CI smoke test — no network, no API keys, no Supabase.
Uses raw asserts (repo style). Fails fast with a clear message.

Covers: models, structured parsing, skill precision, aggregator
balancing + fetch timeout, pipeline safety gates, and the HTTP API
via FastAPI's TestClient (no server needed).
"""

import asyncio
import os
import sys
import tempfile

# Force fully-offline deterministic behavior: no LLM keys, file handoff to temp
os.environ.pop("GEMINI_API_KEY", None)
os.environ.pop("GROQ_API_KEY", None)
_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".jsonl")
os.environ["HANDOFF_FILE_PATH"] = _tmp.name

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PASS = []


def check(name, fn):
    fn()
    PASS.append(name)
    print(f"  PASS: {name}")


# ─── Models ───────────────────────────────────────────────────────────────────

def test_stable_ids():
    from core.models import stable_job_id
    a = stable_job_id("workable", "w-1")
    b = stable_job_id("workable", "w-1")
    c = stable_job_id("workable", "w-2")
    assert a == b, "same board job must map to same ID across runs"
    assert a != c, "different jobs must map to different IDs"
    assert stable_job_id("manual", None) != stable_job_id("manual", None)


def test_normalize_dates():
    from core.models import normalize_posted_date
    assert normalize_posted_date("2026-09-20T12:00:00.000Z") == "2026-09-20"
    assert normalize_posted_date("Sat, 20 Sep 2026 12:00:00 GMT") == "2026-09-20"
    assert normalize_posted_date("2026-09-20") == "2026-09-20"
    assert normalize_posted_date("not a date") is None
    assert normalize_posted_date(None) is None


# ─── Structured parsing ───────────────────────────────────────────────────────

def test_structured_workable_fields():
    from core.models import RawJob, JobSource
    from parsing.structured_parser import StructuredParser
    raw = RawJob(
        source=JobSource.WORKABLE,
        external_id="w-9",
        raw_text="Title: Python Developer\nCompany: TestCo\nPython and Django required.",
        source_url="https://jobs.workable.com/xyz",
        metadata={"title": "Python Developer", "company": "TestCo",
                  "posted_date": "2026-09-24"},
    )
    job = asyncio.run(StructuredParser().parse(raw))
    assert job.job_title == "Python Developer", job.job_title
    assert job.application_link == "https://jobs.workable.com/xyz"
    assert job.posted_date == "2026-09-24", job.posted_date
    assert "Python" in job.required_skills


def test_skill_precision():
    from parsing.structured_parser import _extract_skills
    skills = _extract_skills("Send your resume by email. We use Google Docs daily.")
    assert "AI" not in skills, f"'AI' false positive from 'email': {skills}"
    assert "Go" not in skills, f"'Go' false positive from 'Google': {skills}"
    assert "Google Workspace" not in skills  # 'Google Docs' is not Workspace


def test_title_nontech():
    from parsing.structured_parser import _extract_title
    assert _extract_title("Customer Service Representative needed") == \
        "Customer Service Representative"
    assert _extract_title("Virtual Assistant wanted ASAP") == "Virtual Assistant"


def test_posted_date_regex():
    from datetime import datetime
    from parsing.structured_parser import _extract_posted_date
    now = datetime(2026, 9, 24, 12, 0, 0)
    assert _extract_posted_date("Posted 3 days ago\nTitle: X", now) == "2026-09-21"
    assert _extract_posted_date("Posted today\nTitle: X", now) == "2026-09-24"
    assert _extract_posted_date("Posted on 12 September 2026", now) == "2026-09-12"
    assert _extract_posted_date("No date here at all", now) is None


# ─── Aggregator: balance + timeout ────────────────────────────────────────────

def test_balance_caps():
    from collections import Counter
    from core.interfaces import BaseIngester
    from core.models import RawJob, JobSource
    from ingestion.aggregator import MultiSourceAggregator

    class Fake(BaseIngester):
        def __init__(self, source, n, prefix):
            self._s, self._n, self._p = source, n, prefix

        async def fetch(self):
            for i in range(self._n):
                yield RawJob(source=self._s, external_id=f"{self._p}-{i}",
                             raw_text=f"Title: {self._p} {i}\nCompany: C",
                             source_url=f"https://x/{self._p}-{i}")

        async def health_check(self):
            return True

    async def run():
        agg = MultiSourceAggregator(
            ingesters=[Fake(JobSource.MYJOBMAG, 100, "fast"),
                       Fake(JobSource.WORKABLE, 8, "slow")],
            target_count=20, max_per_source=10)
        return [j async for j in agg.fetch()]

    got = asyncio.run(run())
    dist = Counter(j.source.value for j in got)
    assert len(got) == 20, f"expected 20, got {len(got)}"
    assert dist["workable"] == 8, f"slow source starved: {dict(dist)}"


def test_fetch_timeout():
    import time
    from core.interfaces import BaseIngester
    from core.models import RawJob, JobSource
    from ingestion.aggregator import MultiSourceAggregator

    class Quick(BaseIngester):
        async def fetch(self):
            for i in range(5):
                yield RawJob(source=JobSource.WORKABLE, external_id=f"q-{i}",
                             raw_text=f"Title: Q {i}\nCompany: C")

        async def health_check(self):
            return True

    class Hanging(BaseIngester):
        async def fetch(self):
            await asyncio.sleep(3600)
            yield RawJob(source=JobSource.FUZU, raw_text="never")

        async def health_check(self):
            return True

    async def run():
        agg = MultiSourceAggregator(
            ingesters=[Quick(), Hanging()], target_count=50,
            fetch_timeout_seconds=3)
        return [j async for j in agg.fetch()]

    start = time.monotonic()
    got = asyncio.run(run())
    assert len(got) == 5, f"expected 5, got {len(got)}"
    assert time.monotonic() - start < 20, "timeout did not fire"


# ─── Pipeline gates ───────────────────────────────────────────────────────────

def test_pipeline_gates():
    from core.models import RawJob, JobSource, JobStatus
    from core.pipeline import JobPipeline
    from parsing.structured_parser import StructuredParser
    from validation.validators import SchemaValidator
    from store.stores import InMemoryStore
    from handoff.handlers import MockHandoff

    os.environ["REQUIRE_APPLICATION_LINK"] = "true"
    os.environ["MAX_JOB_AGE_DAYS"] = "1"

    async def run():
        store, handoff = InMemoryStore(), MockHandoff()
        pipe = JobPipeline(ingester=None, parser=StructuredParser(),
                           validator=SchemaValidator(), handoff=handoff,
                           store=store)
        today = __import__("datetime").datetime.utcnow().date().isoformat()
        old = (__import__("datetime").datetime.utcnow().date()
               - __import__("datetime").timedelta(days=9)).isoformat()

        ok = await pipe.ingest_single(RawJob(
            source=JobSource.WORKABLE, external_id="g-1",
            raw_text="Title: Dev\nCompany: C", source_url="https://x/1",
            metadata={"posted_date": today}))
        assert ok.status == JobStatus.SENT, ok
        assert len(handoff.sent_jobs) == 1

        nold = await pipe.ingest_single(RawJob(
            source=JobSource.WORKABLE, external_id="g-2",
            raw_text="Title: Dev\nCompany: C", source_url="https://x/2",
            metadata={"posted_date": old}))
        assert nold.status == JobStatus.PARSED, nold  # skipped: too old

        nlink = await pipe.ingest_single(RawJob(
            source=JobSource.MANUAL, raw_text="Title: Dev\nCompany: C"))
        assert nlink.status == JobStatus.PARSED, nlink  # skipped: no URL
        assert len(handoff.sent_jobs) == 1, "gated jobs must not send"

    asyncio.run(run())


# ─── HTTP API ─────────────────────────────────────────────────────────────────

def test_http_api():
    from fastapi.testclient import TestClient
    import main
    client = TestClient(main.app)

    r = client.get("/health")
    assert r.status_code == 200 and r.json() == {"status": "ok"}, r.text

    spec = client.get("/openapi.json").json()
    for path in ("/ingest/bulk", "/jobs", "/jobs/stats", "/metrics",
                 "/ingest/runs/{run_id}/status", "/jobs/retry"):
        assert path in spec["paths"], f"route missing: {path}"

    page = client.get("/jobs", params={"page": 1, "page_size": 10}).json()
    assert set(("jobs", "page", "page_size", "total", "total_pages")) <= set(page), page

    res = client.post("/ingest/manual", json={
        "raw_text": "Title: QA Engineer\nCompany: TestCo\nPython, Selenium required.",
        "source_url": "https://example.com/jobs/qa-1",
    }).json()
    assert res["success"] is True and res["status"] == "sent", res


if __name__ == "__main__":
    print("smoke_test: offline checks")
    check("stable job IDs", test_stable_ids)
    check("date normalization", test_normalize_dates)
    check("structured workable fields", test_structured_workable_fields)
    check("skill precision (no substring false positives)", test_skill_precision)
    check("non-tech title extraction", test_title_nontech)
    check("posted-date regex", test_posted_date_regex)
    check("aggregator balance caps", test_balance_caps)
    check("aggregator fetch timeout", test_fetch_timeout)
    check("pipeline safety gates", test_pipeline_gates)
    check("HTTP API (health, spec, paging, manual ingest)", test_http_api)
    print(f"\nALL {len(PASS)} CHECKS PASSED")
