"""
core/interfaces.py

Abstract base classes defining the contracts for each pipeline stage.
Swap implementations without touching downstream code.
"""

from abc import ABC, abstractmethod
from typing import AsyncIterator
from core.models import RawJob, ParsedJob, ValidatedJob, HandoffPayload, JobStatus


class BaseIngester(ABC):
    """
    Pulls raw job postings from a source.
    Implement this for each board: IndeedRSSIngester, JSearchIngester, etc.
    """

    @abstractmethod
    async def fetch(self) -> AsyncIterator[RawJob]:
        """Yield raw jobs one at a time. Implementations handle pagination/polling."""
        ...

    @abstractmethod
    async def health_check(self) -> bool:
        """Verify source is reachable before ingestion run."""
        ...


class BaseParser(ABC):
    """
    Transforms raw job text into a structured ParsedJob via LLM.
    Swap Gemini for Claude/OpenAI without touching the pipeline.
    """

    @abstractmethod
    async def parse(self, raw: RawJob) -> ParsedJob:
        """Extract structured fields from raw job text."""
        ...

    @abstractmethod
    async def batch_parse(self, raws: list[RawJob]) -> list[ParsedJob]:
        """Parse multiple jobs — implementations may parallelize."""
        ...


class BaseValidator(ABC):
    """
    Applies rules to a ParsedJob before human review.
    Programmatic guards: required fields, salary sanity checks, etc.
    """

    @abstractmethod
    async def validate(self, parsed: ParsedJob) -> tuple[bool, list[str]]:
        """
        Returns (is_valid, list_of_issues).
        Invalid jobs are flagged, not silently dropped.
        """
        ...


class BaseHandoff(ABC):
    """
    Delivers a ValidatedJob to the downstream consumer (Dozie's algorithm).
    Could be HTTP POST, message queue, file drop, etc.
    """

    @abstractmethod
    async def send(self, job: ValidatedJob) -> HandoffPayload:
        """Deliver job to downstream. Returns a receipt/confirmation."""
        ...

    @abstractmethod
    async def health_check(self) -> bool:
        """Verify downstream is accepting jobs before sending."""
        ...


class BaseStore(ABC):
    """
    Persistence layer. Swap Supabase for Postgres/SQLite without touching pipeline.
    """

    @abstractmethod
    async def save_raw(self, job: RawJob) -> str:
        """Persist raw job, return assigned ID."""
        ...

    @abstractmethod
    async def save_parsed(self, job: ParsedJob) -> str:
        """Persist parsed job in pending state."""
        ...

    @abstractmethod
    async def update_status(self, job_id: str, status: JobStatus, notes: str = "") -> None:
        """Update a job's lifecycle status."""
        ...

    @abstractmethod
    async def get_pending(self, limit: int = 50) -> list[ParsedJob]:
        """Fetch jobs awaiting human review."""
        ...

    @abstractmethod
    async def get_by_id(self, job_id: str) -> ParsedJob | None:
        ...
