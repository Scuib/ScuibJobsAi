import asyncio
import logging
import uuid
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from core.models import (
    ParsedJob,
    JobStatus,
    PipelineResult,
    RawJob,
    JobSource,
    BulkIngestionRequest,
    IngestionRunStatus,
    IngestionStats,
)
from core.pipeline import JobPipeline
from core.metrics import get_metrics_collector
from api.dependencies import get_pipeline, get_store, build_dynamic_aggregator

logger = logging.getLogger(__name__)


# ─── Lifespan ─────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: verify all pipeline components are reachable
    pipeline: JobPipeline = get_pipeline()
    ingester_ok = await pipeline.ingester.health_check()
    handoff_ok = await pipeline.handoff.health_check()
    if not ingester_ok:
        print("WARNING: Ingester health check failed — check source connectivity")
    if not handoff_ok:
        print("WARNING: Handoff health check failed — downstream may be unreachable")
    yield
    # Shutdown: nothing to clean up for now


app = FastAPI(
    title="Job Pipeline API",
    description="Automated job ingestion → LLM parsing → human validation → handoff",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # Tighten this in production
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Request/Response schemas ─────────────────────────────────────────────────

class ManualIngestRequest(BaseModel):
    raw_text: str
    source_url: str | None = None


class ReviewDecision(BaseModel):
    reviewer: str
    notes: str = ""


class BulkApproveRequest(BaseModel):
    job_ids: list[str]
    reviewer: str


class TriggerIngestionRequest(BaseModel):
    source: JobSource = JobSource.INDEED_RSS
    query: str = "software engineer"
    location: str = "remote"


class BulkActionRequest(BaseModel):
    action: str = Field(..., description="approve | reject")
    reviewer: str
    reason: str = ""
    # Filters
    confidence_min: float | None = None
    exclude_flagged: bool = False
    job_ids: list[str] | None = None


# ─── Phase 1: Ingestion endpoints ─────────────────────────────────────────────

@app.post("/ingest/manual", response_model=PipelineResult, tags=["Ingestion"])
async def ingest_manual(
    body: ManualIngestRequest,
    pipeline: JobPipeline = Depends(get_pipeline),
):
    """
    Phase 1: Paste raw job text directly. Triggers LLM parse + staging.
    No scraper needed — for POC validation.
    """
    raw = RawJob(
        source=JobSource.MANUAL,
        raw_text=body.raw_text,
        source_url=body.source_url,
    )
    return await pipeline.ingest_single(raw)


@app.post("/ingest/trigger", response_model=dict, tags=["Ingestion"])
async def trigger_ingestion(
    body: TriggerIngestionRequest,
    background_tasks: BackgroundTasks,
    pipeline: JobPipeline = Depends(get_pipeline),
):
    """
    Phase 2: Kick off a full ingestion cycle in the background.
    Returns immediately — check /jobs?status=parsed to see results.
    """
    background_tasks.add_task(pipeline.run_ingestion_cycle)
    return {"message": "Ingestion cycle started in background", "source": body.source}


async def run_bulk_ingestion_background(
    pipeline: JobPipeline,
    queries: list[str],
    locations: list[str],
    sources: list[JobSource],
    target_count: int,
    remote_only: bool,
    date_posted: str,
    run_id: str,
):
    try:
        aggregator = build_dynamic_aggregator(
            queries=queries,
            locations=locations,
            sources=sources,
            target_count=target_count,
            remote_only=remote_only,
            date_posted=date_posted,
        )

        def _progress_log(step: str, current: int, total: int):
            logger.info(f"BulkIngestion[{run_id}] - {step}: {current}/{total}")

        await pipeline.run_bulk_ingestion(
            aggregator=aggregator,
            run_id=run_id,
            progress_callback=_progress_log,
        )
    except Exception as e:
        logger.error(f"Bulk ingestion background task {run_id} failed: {e}", exc_info=True)


@app.post("/ingest/bulk", response_model=dict, tags=["Ingestion"])
async def trigger_bulk_ingestion(
    body: BulkIngestionRequest,
    background_tasks: BackgroundTasks,
    pipeline: JobPipeline = Depends(get_pipeline),
):
    """
    Kicks off a multi-source ingestion run in the background.
    All jobs are automatically parsed and handed off — no human review needed.
    Returns a run_id immediately to poll for progress.
    """
    run_id = str(uuid.uuid4())

    background_tasks.add_task(
        run_bulk_ingestion_background,
        pipeline=pipeline,
        queries=body.queries,
        locations=body.locations,
        sources=body.sources,
        target_count=body.target_count,
        remote_only=body.remote_only,
        date_posted=body.date_posted,
        run_id=run_id,
    )
    
    return {
        "message": "Bulk ingestion run scheduled in background.",
        "run_id": run_id,
    }


@app.get("/ingest/runs/{run_id}/status", response_model=IngestionRunStatus, tags=["Ingestion"])
async def get_run_status(run_id: str):
    """Poll progress and metrics for a specific bulk ingestion run."""
    collector = get_metrics_collector()
    run = collector.get_run(run_id)
    
    if run:
        return IngestionRunStatus(
            run_id=run_id,
            state="running",
            fetched=run.fetched,
            parsed=run.parsed,
            errors=run.errors,
            duplicates=run.duplicates,
            per_source=dict(run.per_source),
            message="Ingestion run is currently active and processing.",
        )
    
    # Check completed/archived runs
    snapshot = collector.get_snapshot()
    for r in snapshot["recent_runs"]:
        if r["run_id"] == run_id:
            return IngestionRunStatus(
                run_id=run_id,
                state="completed",
                fetched=r["fetched"],
                parsed=r["parsed"],
                errors=r["errors"],
                duplicates=r["duplicates"],
                per_source=r["per_source"],
                message="Ingestion run completed successfully.",
            )
            
    raise HTTPException(
        status_code=404, 
        detail=f"Run {run_id} not found or metrics have expired."
    )


# ─── Phase 2: Review queue endpoints ──────────────────────────────────────────

@app.get("/jobs/stats", tags=["Dashboard"])
async def get_jobs_stats(store=Depends(get_store)):
    """Get aggregate statistics across all jobs (parsed, pending, sent, etc.)."""
    return await store.get_stats()


@app.get("/jobs/pending", response_model=list[ParsedJob], tags=["Review"])
async def get_pending_jobs(
    limit: int = 50,
    store=Depends(get_store),
):
    """Fetch all jobs awaiting human review."""
    return await store.get_pending(limit=limit)


@app.get("/jobs/{job_id}", response_model=ParsedJob, tags=["Review"])
async def get_job(job_id: str, store=Depends(get_store)):
    """Fetch a specific job by ID."""
    job = await store.get_by_id(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return job


@app.post("/jobs/{job_id}/approve", response_model=PipelineResult, tags=["Review"])
async def approve_job(
    job_id: str,
    body: ReviewDecision,
    pipeline: JobPipeline = Depends(get_pipeline),
):
    """
    Human approves a parsed job → sends to Dozie's algorithm.
    This is the critical human-in-the-loop gate.
    """
    result = await pipeline.approve_and_send(
        job_id=job_id,
        reviewer=body.reviewer,
        notes=body.notes,
    )
    if not result.success:
        raise HTTPException(status_code=400, detail=result.message)
    return result


@app.post("/jobs/{job_id}/reject", response_model=PipelineResult, tags=["Review"])
async def reject_job(
    job_id: str,
    body: ReviewDecision,
    pipeline: JobPipeline = Depends(get_pipeline),
):
    """Human rejects a parsed job — will not be forwarded downstream."""
    result = await pipeline.reject(
        job_id=job_id,
        reviewer=body.reviewer,
        reason=body.notes,
    )
    if not result.success:
        raise HTTPException(status_code=400, detail=result.message)
    return result


@app.post("/jobs/bulk-approve", response_model=list[PipelineResult], tags=["Review"])
async def bulk_approve(
    body: BulkApproveRequest,
    pipeline: JobPipeline = Depends(get_pipeline),
):
    """Approve and send multiple jobs in one call."""
    return await pipeline.send_approved_batch(body.job_ids, body.reviewer)


@app.post("/jobs/bulk-action", response_model=list[PipelineResult], tags=["Review"])
async def bulk_action(
    body: BulkActionRequest,
    pipeline: JobPipeline = Depends(get_pipeline),
    store=Depends(get_store),
):
    """
    Bulk approve or reject jobs, with optional filters such as minimum confidence
    or specific job IDs.
    """
    if body.job_ids is not None:
        target_ids = body.job_ids
    else:
        pending = await store.get_pending(limit=1000)
        target_ids = []
        for job in pending:
            if body.confidence_min is not None and job.confidence < body.confidence_min:
                continue
            if body.exclude_flagged and job.validation_issues:
                continue
            target_ids.append(job.id)

    if not target_ids:
        return []

    if body.action == "approve":
        return await pipeline.send_approved_batch(target_ids, body.reviewer)
    elif body.action == "reject":
        tasks = [pipeline.reject(jid, body.reviewer, body.reason) for jid in target_ids]
        return await asyncio.gather(*tasks)
    else:
        raise HTTPException(status_code=400, detail=f"Invalid bulk action: {body.action}")


@app.get("/metrics", tags=["System"])
async def get_metrics():
    """Returns in-memory snapshot metrics for monitoring."""
    return get_metrics_collector().get_snapshot()


# ─── Health ────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["System"])
async def health():
    return {"status": "ok"}
