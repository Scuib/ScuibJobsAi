"""
core/models.py

Immutable data contracts flowing through the pipeline.
RawJob → ParsedJob → HandoffPayload

Fully automatic: no human review gate. Every parsed job is handed off directly.
"""

from __future__ import annotations
from datetime import datetime
from enum import Enum
from typing import Any
from pydantic import BaseModel, Field, HttpUrl
import uuid


def new_id() -> str:
    return str(uuid.uuid4())


def stable_job_id(source: str, external_id: str | None) -> str:
    """
    Deterministic job ID so the same board posting maps to the same ID
    on every cron run. Lets the downstream backend dedup on job_id even
    if our staging store is wiped (e.g. Render restart with InMemoryStore).
    Falls back to a random UUID when there is no stable external_id.
    """
    if external_id:
        return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{source}:{external_id}"))
    return new_id()


# ─── Lifecycle ───────────────────────────────────────────────────────────────

class JobStatus(str, Enum):
    RAW       = "raw"        # Just ingested, not parsed yet
    PARSED    = "parsed"     # LLM extraction complete, ready for automatic handoff
    SENT      = "sent"       # Successfully delivered to downstream matching algorithm
    FAILED    = "failed"     # Handoff or parse failed


class JobSource(str, Enum):
    INDEED_RSS  = "indeed_rss"
    JSEARCH_API = "jsearch_api"
    ADZUNA_API  = "adzuna_api"
    MANUAL      = "manual"
    WORKABLE    = "workable"
    MYJOBMAG    = "myjobmag"
    FUZU        = "fuzu"
    JOBGURUS    = "jobgurus"
    JOBBERMAN   = "jobberman"


# ─── Stage 1: Raw ─────────────────────────────────────────────────────────────

class RawJob(BaseModel):
    id:          str       = Field(default_factory=new_id)
    source:      JobSource
    external_id: str | None = None          # Board's own ID for dedup
    raw_text:    str                        # Full job posting, messy HTML/text
    source_url:  str | None = None
    fetched_at:  datetime  = Field(default_factory=datetime.utcnow)
    metadata:    dict[str, Any] = Field(default_factory=dict)


# ─── Stage 2: Parsed (LLM output) ─────────────────────────────────────────────

class SalaryRange(BaseModel):
    min:      int | None = None
    max:      int | None = None
    currency: str        = "USD"
    period:   str        = "yearly"   # hourly | monthly | yearly


class ParsedJob(BaseModel):
    id:                 str        = Field(default_factory=new_id)
    raw_id:             str                            # FK to RawJob
    status:             JobStatus  = JobStatus.PARSED
    source:             str | None = None               # Originating source, forwarded from RawJob

    # Core extracted fields
    job_title:          str
    company:            str | None = None
    location:           str | None = None
    remote:             bool       = False
    salary:             SalaryRange | None = None
    required_skills:    list[str]  = Field(default_factory=list)
    preferred_skills:   list[str]  = Field(default_factory=list)
    years_experience:   int | None = None
    education_level:    str | None = None
    employment_type:  str | None = None   # full-time | contract | part-time
    description_clean:  str | None = None   # LLM-cleaned prose description

    # Apply link — forwarded from RawJob.source_url so users can click
    # through to the original board posting and apply.
    source_url:       str | None = None   # Job page on the source board
    application_link: str | None = None   # Direct apply URL (defaults to source_url)

    # Parsing metadata
    parsed_at:          datetime   = Field(default_factory=datetime.utcnow)
    model_used:         str        = ""
    confidence:         float      = 1.0    # 0–1, lower = more LLM uncertainty
    parse_warnings:     list[str]  = Field(default_factory=list)

    # Validation (automated checks)
    validation_issues:  list[str]  = Field(default_factory=list)


# ─── Stage 3: Handoff payload ─────────────────────────────────────────────────

class HandoffPayload(BaseModel):
    """
    Exact schema the downstream matching algorithm receives.
    Built directly from a ParsedJob — no human review step.
    """
    job_id:           str
    job_title:        str
    company:          str | None
    source:           str | None = None
    location:         str | None
    remote:           bool
    salary_min:       int | None
    salary_max:       int | None
    salary_currency:  str
    required_skills:  list[str]
    preferred_skills: list[str]
    years_experience: int | None
    employment_type:  str | None
    description:      str | None
    application_link: str | None = None   # Where the user clicks to apply
    source_url:       str | None = None   # Original board posting URL
    submitted_at:     datetime = Field(default_factory=datetime.utcnow)

    @classmethod
    def from_parsed(cls, p: ParsedJob) -> "HandoffPayload":
        """Build a handoff payload directly from a parsed job."""
        return cls(
            job_id=p.id,
            job_title=p.job_title,
            company=p.company,
            source=p.source,
            location=p.location,
            remote=p.remote,
            salary_min=p.salary.min if p.salary else None,
            salary_max=p.salary.max if p.salary else None,
            salary_currency=p.salary.currency if p.salary else "USD",
            required_skills=p.required_skills,
            preferred_skills=p.preferred_skills,
            years_experience=p.years_experience,
            employment_type=p.employment_type,
            description=p.description_clean,
            application_link=p.application_link or p.source_url,
            source_url=p.source_url,
        )


# ─── API response wrappers ────────────────────────────────────────────────────

class PipelineResult(BaseModel):
    success:  bool
    job_id:   str | None = None
    status:   JobStatus | None = None
    message:  str = ""
    errors:   list[str] = Field(default_factory=list)


# ─── Enterprise ingestion models ──────────────────────────────────────────────

class IngestionStats(BaseModel):
    """Detailed stats from a bulk ingestion run."""
    run_id:            str
    total_fetched:     int = 0
    total_parsed:      int = 0
    total_validated:   int = 0
    total_flagged:     int = 0
    total_errors:      int = 0
    total_duplicates:  int = 0
    total_skipped:     int = 0   # parsed but not handed off (e.g. no application link)
    per_source:        dict[str, int] = Field(default_factory=dict)
    duration_seconds:  float = 0.0
    jobs_per_second:   float = 0.0


class BulkIngestionRequest(BaseModel):
    """Request schema for enterprise bulk ingestion."""
    queries:       list[str] = Field(
        default=["software engineer"],
        description="Search queries to run across all sources",
    )
    locations:     list[str] = Field(
        default=["remote", "United States"],
        description="Locations to search in",
    )
    sources:       list[JobSource] = Field(
        default=[JobSource.JSEARCH_API, JobSource.INDEED_RSS, JobSource.ADZUNA_API],
        description="Which sources to pull from",
    )
    target_count:  int = Field(
        default=200,
        ge=1,
        le=2000,
        description="Stop fetching after this many unique jobs",
    )
    remote_only:   bool = False
    date_posted:   str = Field(
        default="week",
        description="Recency filter: today | 3days | week | month",
    )


class IngestionRunStatus(BaseModel):
    """Status of an active or completed ingestion run for polling."""
    run_id:     str
    state:      str = "running"   # running | completed | failed
    fetched:    int = 0
    parsed:     int = 0
    errors:     int = 0
    duplicates: int = 0
    skipped:    int = 0
    per_source: dict[str, int] = Field(default_factory=dict)
    message:    str = ""
