"""
core/pipeline.py

Orchestrates the full flow: ingest → parse → validate → stage → (human review) → handoff.
Constructed via dependency injection — no concrete implementations imported here.
"""

import asyncio
import logging
from core.interfaces import BaseIngester, BaseParser, BaseValidator, BaseHandoff, BaseStore
from core.models import RawJob, ParsedJob, ValidatedJob, JobStatus, PipelineResult

logger = logging.getLogger(__name__)


class JobPipeline:
    """
    Wires together the four pipeline stages.
    All dependencies are injected — swap any stage independently.

    Usage:
        pipeline = JobPipeline(
            ingester=IndeedRSSIngester(...),
            parser=GeminiParser(...),
            validator=SchemaValidator(...),
            handoff=HTTPHandoff(...),
            store=SupabaseStore(...),
        )
        await pipeline.run_ingestion_cycle()
    """

    def __init__(
        self,
        ingester: BaseIngester,
        parser: BaseParser,
        validator: BaseValidator,
        handoff: BaseHandoff,
        store: BaseStore,
        max_concurrent_parses: int = 5,
    ):
        self.ingester = ingester
        self.parser = parser
        self.validator = validator
        self.handoff = handoff
        self.store = store
        self._semaphore = asyncio.Semaphore(max_concurrent_parses)

    # ─── Phase 1: Ingest + Parse ───────────────────────────────────────────────

    async def run_ingestion_cycle(self) -> dict:
        """
        Full ingestion run: fetch all available jobs, parse, validate, stage.
        Returns a summary dict for logging/monitoring.
        """
        stats = {"fetched": 0, "parsed": 0, "flagged": 0, "errors": 0}

        async for raw in self.ingester.fetch():
            stats["fetched"] += 1
            try:
                raw_id = await self.store.save_raw(raw)
                result = await self._parse_and_stage(raw)
                if result.success:
                    stats["parsed"] += 1
                    if result.errors:
                        stats["flagged"] += 1
                else:
                    stats["errors"] += 1
            except Exception as e:
                logger.error(f"Pipeline error on job {raw.id}: {e}")
                stats["errors"] += 1

        logger.info(f"Ingestion cycle complete: {stats}")
        return stats

    async def ingest_single(self, raw: RawJob) -> PipelineResult:
        """
        Ingest a single raw job (Phase 1: manual paste endpoint).
        """
        await self.store.save_raw(raw)
        return await self._parse_and_stage(raw)

    async def _parse_and_stage(self, raw: RawJob) -> PipelineResult:
        async with self._semaphore:
            try:
                parsed = await self.parser.parse(raw)
                is_valid, issues = await self.validator.validate(parsed)
                parsed.validation_issues = issues

                if not is_valid:
                    parsed.status = JobStatus.PARSED  # Still staged, but flagged
                    logger.warning(f"Job {parsed.id} has validation issues: {issues}")

                await self.store.save_parsed(parsed)
                return PipelineResult(
                    success=True,
                    job_id=parsed.id,
                    status=parsed.status,
                    errors=issues,
                )
            except Exception as e:
                logger.error(f"Parse/stage failed for raw {raw.id}: {e}")
                return PipelineResult(success=False, message=str(e))

    # ─── Phase 2: Human Review → Handoff ──────────────────────────────────────

    async def approve_and_send(
        self,
        job_id: str,
        reviewer: str,
        notes: str = "",
    ) -> PipelineResult:
        """
        Human approves a pending job → update status → send to Dozie's algorithm.
        """
        parsed = await self.store.get_by_id(job_id)
        if not parsed:
            return PipelineResult(success=False, message=f"Job {job_id} not found")

        if parsed.status not in (JobStatus.PARSED, JobStatus.FAILED):
            return PipelineResult(
                success=False,
                message=f"Job {job_id} is in status {parsed.status}, cannot approve",
            )

        parsed.reviewer_notes = notes
        validated = ValidatedJob(parsed=parsed, approved_by=reviewer)

        try:
            receipt = await self.handoff.send(validated)
            await self.store.update_status(job_id, JobStatus.SENT, notes)
            return PipelineResult(success=True, job_id=job_id, status=JobStatus.SENT)
        except Exception as e:
            await self.store.update_status(job_id, JobStatus.FAILED, str(e))
            logger.error(f"Handoff failed for {job_id}: {e}")
            return PipelineResult(success=False, job_id=job_id, message=str(e))

    async def reject(self, job_id: str, reviewer: str, reason: str) -> PipelineResult:
        """Human rejects a pending job."""
        parsed = await self.store.get_by_id(job_id)
        if not parsed:
            return PipelineResult(success=False, message=f"Job {job_id} not found")

        await self.store.update_status(job_id, JobStatus.REJECTED, reason)
        return PipelineResult(success=True, job_id=job_id, status=JobStatus.REJECTED)

    # ─── Bulk handoff ─────────────────────────────────────────────────────────

    async def send_approved_batch(self, job_ids: list[str], reviewer: str) -> list[PipelineResult]:
        """Approve and send multiple jobs in parallel."""
        tasks = [self.approve_and_send(jid, reviewer) for jid in job_ids]
        return await asyncio.gather(*tasks, return_exceptions=False)
