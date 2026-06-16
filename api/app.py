"""
api/app.py

FastAPI application. All pipeline dependencies are injected via
FastAPI's DI system — no concrete implementations hardcoded here.
Swap store/parser/handoff at startup in config.py.
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from core.models import ParsedJob, JobStatus, PipelineResult, RawJob, JobSource
from core.pipeline import JobPipeline
from api.dependencies import get_pipeline, get_store


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


# ─── Phase 2: Review queue endpoints ──────────────────────────────────────────

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


# ─── Health ────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["System"])
async def health():
    return {"status": "ok"}
