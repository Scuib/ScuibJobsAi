"""
core/pipeline.py

Orchestrates the full flow: ingest → parse → validate → stage → (human review) → handoff.
Constructed via dependency injection — no concrete implementations imported here.
"""

import asyncio
import logging
from datetime import datetime
from core.interfaces import BaseIngester, BaseParser, BaseValidator, BaseHandoff, BaseStore
from core.models import RawJob, ParsedJob, ValidatedJob, JobStatus, PipelineResult, IngestionStats
from core.metrics import get_metrics_collector

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

    # ─── Enterprise Bulk Ingestion ─────────────────────────────────────────────

    async def run_bulk_ingestion(
        self,
        aggregator: BaseIngester,
        run_id: str,
        auto_approve_threshold: float | None = None,
        progress_callback: callable = None,
    ) -> IngestionStats:
        """
        Runs a high-throughput enterprise ingestion cycle.
        Fetches concurrently, deduplicates, batch parses, validates, and batch stores.
        """
        collector = get_metrics_collector()
        run = collector.start_run(run_id)

        try:
            # 1. Concurrently fetch all jobs from the aggregator
            logger.info(f"BulkIngestion[{run_id}]: starting fetch phase")
            if progress_callback:
                progress_callback("fetching", 0, aggregator.target_count)

            raw_jobs = []
            async for raw in aggregator.fetch():
                raw_jobs.append(raw)
                run.fetched += 1
                run.per_source[raw.source.value] += 1
                if progress_callback:
                    progress_callback("fetching", len(raw_jobs), aggregator.target_count)

            # 2. Check aggregator's own duplicate count if any
            if hasattr(aggregator, "total_duplicates"):
                run.duplicates += aggregator.total_duplicates

            # 3. Persistent deduplication check
            unique_raws = []
            for raw in raw_jobs:
                if raw.external_id and await self.store.exists_by_external_id(raw.external_id):
                    run.duplicates += 1
                    continue
                unique_raws.append(raw)

            if not unique_raws:
                logger.info(f"BulkIngestion[{run_id}]: no new unique jobs fetched")
                if progress_callback:
                    progress_callback("completed", 0, 0)
                collector.finish_run(run_id)
                return IngestionStats(
                    run_id=run_id,
                    total_fetched=run.fetched,
                    total_parsed=0,
                    total_validated=0,
                    total_flagged=0,
                    total_errors=run.errors,
                    total_duplicates=run.duplicates,
                    per_source=dict(run.per_source),
                    duration_seconds=run.duration_seconds,
                    jobs_per_second=0.0,
                )

            # 4. Save raw jobs in bulk
            logger.info(f"BulkIngestion[{run_id}]: saving {len(unique_raws)} raw jobs")
            if progress_callback:
                progress_callback("saving_raw", 0, len(unique_raws))
            await self.store.save_raw_batch(unique_raws)
            if progress_callback:
                progress_callback("saving_raw", len(unique_raws), len(unique_raws))

            # 5. Batch parse with adaptive concurrency
            logger.info(f"BulkIngestion[{run_id}]: batch parsing {len(unique_raws)} jobs")
            if progress_callback:
                progress_callback("parsing", 0, len(unique_raws))

            def _parse_progress(current: int, total: int):
                if progress_callback:
                    progress_callback("parsing", current, total)

            parsed_jobs = await self.parser.batch_parse(unique_raws, progress_callback=_parse_progress)
            run.parsed = len(parsed_jobs)

            # 6. Concurrently validate the batch
            logger.info(f"BulkIngestion[{run_id}]: validating parsed jobs")
            if progress_callback:
                progress_callback("validating", 0, len(parsed_jobs))

            validation_tasks = [self.validator.validate(job) for job in parsed_jobs]
            validation_results = await asyncio.gather(*validation_tasks)

            for i, (job, (is_valid, issues)) in enumerate(zip(parsed_jobs, validation_results)):
                job.validation_issues = issues
                
                # Check for parsing errors
                if "[PARSE FAILED]" in job.job_title:
                    run.errors += 1
                
                if is_valid:
                    run.validated += 1
                    # Auto-approve threshold check
                    if auto_approve_threshold is not None and job.confidence >= auto_approve_threshold:
                        job.status = JobStatus.APPROVED
                        job.reviewer_notes = "Auto-approved via confidence gate"
                        job.reviewed_at = datetime.utcnow()
                        job.reviewed_by = "system"
                else:
                    run.flagged += 1

                if progress_callback:
                    progress_callback("validating", i + 1, len(parsed_jobs))

            # 7. Batch save parsed/approved jobs
            logger.info(f"BulkIngestion[{run_id}]: saving {len(parsed_jobs)} parsed jobs")
            if progress_callback:
                progress_callback("saving_parsed", 0, len(parsed_jobs))
            await self.store.save_parsed_batch(parsed_jobs)
            if progress_callback:
                progress_callback("saving_parsed", len(parsed_jobs), len(parsed_jobs))

            logger.info(f"BulkIngestion[{run_id}]: completed successfully")
            if progress_callback:
                progress_callback("completed", len(parsed_jobs), len(parsed_jobs))

        except Exception as e:
            logger.error(f"BulkIngestion[{run_id}]: execution failed with error: {e}", exc_info=True)
            run.errors += 1
            if progress_callback:
                progress_callback("failed", 0, 0)
        finally:
            collector.finish_run(run_id)

        return IngestionStats(
            run_id=run_id,
            total_fetched=run.fetched,
            total_parsed=run.parsed,
            total_validated=run.validated,
            total_flagged=run.flagged,
            total_errors=run.errors,
            total_duplicates=run.duplicates,
            per_source=dict(run.per_source),
            duration_seconds=run.duration_seconds,
            jobs_per_second=run.jobs_per_second,
        )

