"""
core/pipeline.py

Orchestrates the full flow: ingest → parse → validate → handoff.
No human-in-the-loop — every parsed job is automatically handed off to the
downstream matching algorithm. Manual approve/reject endpoints still available
for retrying failed jobs or flagging unwanted ones.
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

    No human-in-the-loop: every job goes ingest → parse → validate → handoff.
    approve_and_send / reject are available for manual retry of failed jobs.
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

    # ─── Ingest → Parse → Handoff ─────────────────────────────────────────────

    async def run_ingestion_cycle(self) -> dict:
        stats = {"fetched": 0, "parsed": 0, "handoff_ok": 0, "errors": 0}

        async for raw in self.ingester.fetch():
            stats["fetched"] += 1
            try:
                await self.store.save_raw(raw)
                result = await self._parse_and_handoff(raw)
                if result.success:
                    stats["parsed"] += 1
                    if result.status == JobStatus.SENT:
                        stats["handoff_ok"] += 1
                    elif result.errors:
                        stats["errors"] += 1
                else:
                    stats["errors"] += 1
            except Exception as e:
                logger.error(f"Pipeline error on job {raw.id}: {e}")
                stats["errors"] += 1

        logger.info(f"Ingestion cycle complete: {stats}")
        return stats

    async def ingest_single(self, raw: RawJob) -> PipelineResult:
        await self.store.save_raw(raw)
        return await self._parse_and_handoff(raw)

    async def _parse_and_handoff(self, raw: RawJob) -> PipelineResult:
        async with self._semaphore:
            try:
                parsed = await self.parser.parse(raw)
                is_valid, issues = await self.validator.validate(parsed)
                parsed.validation_issues = issues
                parsed.reviewed_by = "system"
                parsed.reviewed_at = datetime.utcnow()

                await self.store.save_parsed(parsed)

                validated = ValidatedJob(parsed=parsed, approved_by="system")
                try:
                    await self.handoff.send(validated)
                    await self.store.update_status(parsed.id, JobStatus.SENT, "Auto-handoff")
                    return PipelineResult(
                        success=True,
                        job_id=parsed.id,
                        status=JobStatus.SENT,
                        errors=issues,
                    )
                except Exception as handoff_err:
                    await self.store.update_status(parsed.id, JobStatus.FAILED, str(handoff_err))
                    logger.error(f"Handoff failed for {parsed.id}: {handoff_err}")
                    return PipelineResult(
                        success=False,
                        job_id=parsed.id,
                        status=JobStatus.FAILED,
                        message=f"Parse OK, handoff failed: {handoff_err}",
                        errors=issues,
                    )

            except Exception as e:
                logger.error(f"Parse failed for raw {raw.id}: {e}")
                return PipelineResult(success=False, message=str(e))

    # ─── Manual override endpoints (retry failed jobs) ─────────────────────────

    async def approve_and_send(
        self,
        job_id: str,
        reviewer: str,
        notes: str = "",
    ) -> PipelineResult:
        """Retry handoff for a failed job."""
        parsed = await self.store.get_by_id(job_id)
        if not parsed:
            return PipelineResult(success=False, message=f"Job {job_id} not found")

        if parsed.status not in (JobStatus.PARSED, JobStatus.FAILED):
            return PipelineResult(
                success=False,
                message=f"Job {job_id} is in status {parsed.status}, cannot approve",
            )

        validated = ValidatedJob(parsed=parsed, approved_by=reviewer)
        try:
            await self.handoff.send(validated)
            await self.store.update_status(job_id, JobStatus.SENT, notes)
            return PipelineResult(success=True, job_id=job_id, status=JobStatus.SENT)
        except Exception as e:
            await self.store.update_status(job_id, JobStatus.FAILED, str(e))
            logger.error(f"Handoff retry failed for {job_id}: {e}")
            return PipelineResult(success=False, job_id=job_id, message=str(e))

    async def reject(self, job_id: str, reviewer: str, reason: str) -> PipelineResult:
        """Manually mark a job as rejected (won't be sent downstream)."""
        parsed = await self.store.get_by_id(job_id)
        if not parsed:
            return PipelineResult(success=False, message=f"Job {job_id} not found")

        await self.store.update_status(job_id, JobStatus.REJECTED, reason)
        return PipelineResult(success=True, job_id=job_id, status=JobStatus.REJECTED)

    async def send_approved_batch(self, job_ids: list[str], reviewer: str) -> list[PipelineResult]:
        tasks = [self.approve_and_send(jid, reviewer) for jid in job_ids]
        return await asyncio.gather(*tasks, return_exceptions=False)

    # ─── Bulk Ingestion ───────────────────────────────────────────────────────

    async def run_bulk_ingestion(
        self,
        aggregator: BaseIngester,
        run_id: str,
        progress_callback: callable = None,
    ) -> IngestionStats:
        """
        High-throughput bulk ingestion: fetch → dedup → parse → validate → handoff.
        All jobs are automatically sent to the downstream matching algorithm.
        """
        collector = get_metrics_collector()
        run = collector.start_run(run_id)

        try:
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

            if hasattr(aggregator, "total_duplicates"):
                run.duplicates += aggregator.total_duplicates

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
                    total_errors=run.errors,
                    total_duplicates=run.duplicates,
                    per_source=dict(run.per_source),
                    duration_seconds=run.duration_seconds,
                    jobs_per_second=0.0,
                )

            logger.info(f"BulkIngestion[{run_id}]: saving {len(unique_raws)} raw jobs")
            if progress_callback:
                progress_callback("saving_raw", 0, len(unique_raws))
            await self.store.save_raw_batch(unique_raws)
            if progress_callback:
                progress_callback("saving_raw", len(unique_raws), len(unique_raws))

            logger.info(f"BulkIngestion[{run_id}]: batch parsing {len(unique_raws)} jobs")
            if progress_callback:
                progress_callback("parsing", 0, len(unique_raws))

            def _parse_progress(current: int, total: int):
                if progress_callback:
                    progress_callback("parsing", current, total)

            parsed_jobs = await self.parser.batch_parse(unique_raws, progress_callback=_parse_progress)
            run.parsed = len(parsed_jobs)

            logger.info(f"BulkIngestion[{run_id}]: validating parsed jobs")
            if progress_callback:
                progress_callback("validating", 0, len(parsed_jobs))

            validation_tasks = [self.validator.validate(job) for job in parsed_jobs]
            validation_results = await asyncio.gather(*validation_tasks)

            sent_count = 0
            for i, (job, (is_valid, issues)) in enumerate(zip(parsed_jobs, validation_results)):
                job.validation_issues = issues
                job.reviewed_by = "system"
                job.reviewed_at = datetime.utcnow()

                if "[PARSE FAILED]" in job.job_title:
                    run.errors += 1
                elif is_valid:
                    run.validated += 1
                else:
                    run.flagged += 1

                if progress_callback:
                    progress_callback("validating", i + 1, len(parsed_jobs))

            logger.info(f"BulkIngestion[{run_id}]: saving and handing off {len(parsed_jobs)} jobs")
            if progress_callback:
                progress_callback("handoff", 0, len(parsed_jobs))

            await self.store.save_parsed_batch(parsed_jobs)

            for i, job in enumerate(parsed_jobs):
                try:
                    validated = ValidatedJob(parsed=job, approved_by="system")
                    await self.handoff.send(validated)
                    await self.store.update_status(job.id, JobStatus.SENT, "Bulk auto-handoff")
                    sent_count += 1
                except Exception as e:
                    await self.store.update_status(job.id, JobStatus.FAILED, str(e))
                    run.errors += 1
                    logger.error(f"BulkIngestion[{run_id}]: handoff failed for {job.id}: {e}")

                if progress_callback:
                    progress_callback("handoff", i + 1, len(parsed_jobs))

            logger.info(f"BulkIngestion[{run_id}]: completed — {sent_count}/{len(parsed_jobs)} handed off")
            if progress_callback:
                progress_callback("completed", sent_count, len(parsed_jobs))

        except Exception as e:
            logger.error(f"BulkIngestion[{run_id}]: execution failed: {e}", exc_info=True)
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

