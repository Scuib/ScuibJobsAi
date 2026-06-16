"""
store/stores.py

Concrete BaseStore implementations.
InMemoryStore: Phase 1 / testing (no DB needed).
SupabaseStore: Phase 2+ production persistence.
"""

import logging
from datetime import datetime
from core.interfaces import BaseStore
from core.models import RawJob, ParsedJob, JobStatus

logger = logging.getLogger(__name__)


# ─── In-memory (Phase 1 / tests) ─────────────────────────────────────────────

class InMemoryStore(BaseStore):
    """
    No-DB store for Phase 1 and unit tests.
    Replace with SupabaseStore when moving to production.
    """

    def __init__(self):
        self._raw: dict[str, RawJob] = {}
        self._parsed: dict[str, ParsedJob] = {}

    async def save_raw(self, job: RawJob) -> str:
        self._raw[job.id] = job
        return job.id

    async def save_parsed(self, job: ParsedJob) -> str:
        self._parsed[job.id] = job
        return job.id

    async def update_status(self, job_id: str, status: JobStatus, notes: str = "") -> None:
        if job_id in self._parsed:
            self._parsed[job_id].status = status
            self._parsed[job_id].reviewer_notes = notes
            self._parsed[job_id].reviewed_at = datetime.utcnow()

    async def get_pending(self, limit: int = 50) -> list[ParsedJob]:
        return [
            j for j in self._parsed.values()
            if j.status == JobStatus.PARSED
        ][:limit]

    async def get_by_id(self, job_id: str) -> ParsedJob | None:
        return self._parsed.get(job_id)

    async def get_all(self) -> list[ParsedJob]:
        return list(self._parsed.values())


# ─── Supabase (Phase 2+) ──────────────────────────────────────────────────────

class SupabaseStore(BaseStore):
    """
    Persists jobs to Supabase using the supabase-py client.

    Required tables — run this SQL in your Supabase project:

    CREATE TABLE raw_jobs (
        id           UUID PRIMARY KEY,
        source       TEXT NOT NULL,
        external_id  TEXT,
        raw_text     TEXT NOT NULL,
        source_url   TEXT,
        fetched_at   TIMESTAMPTZ DEFAULT NOW(),
        metadata     JSONB DEFAULT '{}'
    );

    CREATE TABLE parsed_jobs (
        id                UUID PRIMARY KEY,
        raw_id            UUID REFERENCES raw_jobs(id),
        status            TEXT NOT NULL DEFAULT 'parsed',
        job_title         TEXT NOT NULL,
        company           TEXT,
        location          TEXT,
        remote            BOOLEAN DEFAULT FALSE,
        salary            JSONB,
        required_skills   TEXT[] DEFAULT '{}',
        preferred_skills  TEXT[] DEFAULT '{}',
        years_experience  INTEGER,
        education_level   TEXT,
        employment_type   TEXT,
        description_clean TEXT,
        model_used        TEXT,
        confidence        FLOAT DEFAULT 1.0,
        parse_warnings    TEXT[] DEFAULT '{}',
        validation_issues TEXT[] DEFAULT '{}',
        reviewer_notes    TEXT DEFAULT '',
        reviewed_at       TIMESTAMPTZ,
        reviewed_by       TEXT,
        parsed_at         TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE INDEX parsed_jobs_status_idx ON parsed_jobs(status);
    """

    def __init__(self, url: str, key: str):
        from supabase import create_client
        self.client = create_client(url, key)

    async def save_raw(self, job: RawJob) -> str:
        import asyncio
        data = job.model_dump(mode="json")
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: self.client.table("raw_jobs").insert(data).execute()
        )
        return job.id

    async def save_parsed(self, job: ParsedJob) -> str:
        import asyncio
        data = job.model_dump(mode="json")
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: self.client.table("parsed_jobs").insert(data).execute()
        )
        return job.id

    async def update_status(self, job_id: str, status: JobStatus, notes: str = "") -> None:
        import asyncio
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: self.client.table("parsed_jobs")
                .update({
                    "status": status.value,
                    "reviewer_notes": notes,
                    "reviewed_at": datetime.utcnow().isoformat(),
                })
                .eq("id", job_id)
                .execute()
        )

    async def get_pending(self, limit: int = 50) -> list[ParsedJob]:
        import asyncio
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            lambda: self.client.table("parsed_jobs")
                .select("*")
                .eq("status", "parsed")
                .limit(limit)
                .order("parsed_at", desc=False)
                .execute()
        )
        return [ParsedJob(**row) for row in (result.data or [])]

    async def get_by_id(self, job_id: str) -> ParsedJob | None:
        import asyncio
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            lambda: self.client.table("parsed_jobs")
                .select("*")
                .eq("id", job_id)
                .single()
                .execute()
        )
        if result.data:
            return ParsedJob(**result.data)
        return None
