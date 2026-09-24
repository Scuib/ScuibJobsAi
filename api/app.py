import asyncio
import logging
import uuid
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks, Path, Query, status
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
    PagedJobs,
)
from core.pipeline import JobPipeline
from core.metrics import get_metrics_collector
from api.dependencies import get_pipeline, get_store, build_dynamic_aggregator

logger = logging.getLogger(__name__)


# ─── Swagger tag metadata ────────────────────────────────────────────────────

tags_metadata = [
    {
        "name": "Ingestion",
        "description": (
            "Start job ingestion runs. These endpoints fetch raw job postings from "
            "external sources (Workable, MyJobMag, Fuzu, JSearch, Adzuna, etc.), "
            "parse them with AI, validate them, and automatically send them to the "
            "downstream matching algorithm. No human approval is needed."
        ),
    },
    {
        "name": "Jobs",
        "description": (
            "Read-only endpoints for browsing and monitoring jobs that have been "
            "processed by the pipeline. Use these to build dashboards and "
            "monitoring views in the frontend panel."
        ),
    },
    {
        "name": "System",
        "description": (
            "Health checks and pipeline performance metrics. Use `/health` for "
            "liveness probes and `/metrics` for observability dashboards."
        ),
    },
]


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
    title="ScuibJobsAi Pipeline API",
    description=(
        "**Fully automated** job data pipeline that ingests job listings from multiple "
        "sources (Workable, MyJobMag, Fuzu, JobGurus, Jobberman, JSearch, Indeed RSS, "
        "Adzuna), parses them using Google Gemini AI (with regex fallback), validates "
        "the extracted data, and automatically hands off every job to the downstream "
        "matching algorithm.\n\n"
        "### How it works\n"
        "1. **Ingest** — Fetches raw job postings from configured sources\n"
        "2. **Parse** — Extracts structured fields (title, company, skills, salary, etc.) using Gemini LLM\n"
        "3. **Validate** — Runs automated quality checks (missing fields, salary sanity, etc.)\n"
        "4. **Handoff** — Sends the structured job data to the matching algorithm automatically\n\n"
        "### Key points\n"
        "- **No human approval needed** — the entire pipeline runs end-to-end automatically\n"
        "- **Background processing** — bulk ingestion runs asynchronously; poll for progress\n"
        "- **Multi-source** — fetches from 8+ job boards in a single run\n"
        "- **Resilient** — circuit breakers, rate limiters, and automatic retries built-in"
    ),
    version="2.0.0",
    lifespan=lifespan,
    openapi_tags=tags_metadata,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # Tighten this in production
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Request schemas ──────────────────────────────────────────────────────────

class ManualIngestRequest(BaseModel):
    """Payload for manually submitting a single raw job posting for processing."""
    raw_text: str = Field(
        ...,
        description="The full raw text of the job posting (can include HTML or plain text).",
        json_schema_extra={"example": "Senior Backend Engineer - Remote\nTechCorp Inc | $140k-$180k\nPython, Go, PostgreSQL, 5+ years experience required."},
    )
    source_url: str | None = Field(
        default=None,
        description="Optional URL where the job was originally posted. Used for reference only.",
        json_schema_extra={"example": "https://example.com/jobs/senior-backend-eng"},
    )


class TriggerIngestionRequest(BaseModel):
    """Payload for triggering a simple single-source ingestion run."""
    source: JobSource = Field(
        default=JobSource.INDEED_RSS,
        description="Which job source to fetch from. Defaults to Indeed RSS.",
    )
    query: str = Field(
        default="software engineer",
        description="Search query to use when fetching jobs.",
    )
    location: str = Field(
        default="remote",
        description="Location filter for the job search.",
    )


class RetryHandoffRequest(BaseModel):
    """Payload for retrying the handoff of a failed job."""
    job_ids: list[str] = Field(
        ...,
        description="List of job IDs (UUIDs) to retry handoff for. Only jobs with 'failed' status can be retried.",
        json_schema_extra={"example": ["550e8400-e29b-41d4-a716-446655440000"]},
    )


# ─── Phase 1: Ingestion endpoints ─────────────────────────────────────────────

@app.post(
    "/ingest/manual",
    response_model=PipelineResult,
    tags=["Ingestion"],
    summary="Manually submit a single job for processing",
    response_description="The result of the pipeline run, including the new job's ID and its final status (usually 'sent').",
)
async def ingest_manual(
    body: ManualIngestRequest,
    pipeline: JobPipeline = Depends(get_pipeline),
):
    """
    Submit a raw job posting directly as text. The pipeline will:

    1. **Parse** the text using AI (Gemini LLM) to extract structured fields like
       job title, company, location, salary, required skills, etc.
    2. **Validate** the extracted data against quality rules.
    3. **Hand off** the structured job to the downstream matching algorithm.

    This endpoint is useful for testing, one-off jobs, or when you have job text
    that didn't come from a supported source.

    **No human approval is needed** — the job is processed end-to-end automatically.
    """
    raw = RawJob(
        source=JobSource.MANUAL,
        raw_text=body.raw_text,
        source_url=body.source_url,
    )
    return await pipeline.ingest_single(raw)


@app.post(
    "/ingest/trigger",
    response_model=dict,
    tags=["Ingestion"],
    summary="Start a simple single-source ingestion run",
    response_description="Confirmation that the ingestion run has started in the background.",
)
async def trigger_ingestion(
    body: TriggerIngestionRequest,
    background_tasks: BackgroundTasks,
    pipeline: JobPipeline = Depends(get_pipeline),
):
    """
    Kicks off an ingestion cycle from a single source in the background.
    The server responds immediately — jobs will be fetched, parsed, validated,
    and handed off automatically.

    Use `GET /jobs/stats` to see updated counts after the run completes.
    For multi-source ingestion with more control, use `POST /ingest/bulk` instead.
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


@app.post(
    "/ingest/bulk",
    response_model=dict,
    tags=["Ingestion"],
    summary="Start a multi-source bulk ingestion run",
    response_description="A `run_id` you can use to poll for progress via `GET /ingest/runs/{run_id}/status`.",
)
async def trigger_bulk_ingestion(
    body: BulkIngestionRequest,
    background_tasks: BackgroundTasks,
    pipeline: JobPipeline = Depends(get_pipeline),
):
    """
    **The main way to fetch jobs.** Kicks off a multi-source ingestion run that:

    1. Fetches raw job postings from the specified sources (Workable, MyJobMag, Fuzu, etc.)
    2. Deduplicates against previously seen jobs
    3. Parses all new jobs using AI (Gemini LLM with regex fallback)
    4. Validates extracted data quality
    5. Hands off every job to the downstream matching algorithm

    **This runs entirely in the background.** The endpoint returns immediately with a
    `run_id` that you can poll with `GET /ingest/runs/{run_id}/status` to track progress.

    All jobs are processed automatically — no human approval step.

    **Available sources:** `workable`, `myjobmag`, `fuzu`, `jobgurus`, `jobberman`,
    `jsearch_api`, `indeed_rss`, `adzuna_api`
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


@app.get(
    "/ingest/runs/{run_id}/status",
    response_model=IngestionRunStatus,
    tags=["Ingestion"],
    summary="Check the progress of a bulk ingestion run",
    response_description="Current status and counters for the specified ingestion run.",
)
async def get_run_status(
    run_id: str = Path(..., description="The UUID returned by `POST /ingest/bulk`"),
):
    """
    Poll progress of a running or recently completed bulk ingestion run.

    **Recommended polling interval:** every 3–5 seconds.

    The response includes:
    - `state` — either `running`, `completed`, or `failed`
    - `fetched` — how many raw jobs have been fetched so far
    - `parsed` — how many have been successfully parsed by AI
    - `errors` — how many jobs failed during parsing or handoff
    - `duplicates` — how many were skipped as duplicates
    - `per_source` — breakdown of job counts by source

    Returns `404` if the run ID is not found (expired from memory or never existed).
    """
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
            skipped=run.skipped,
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
                skipped=r.get("skipped", 0),
                per_source=r["per_source"],
                message="Ingestion run completed successfully.",
            )
            
    raise HTTPException(
        status_code=404, 
        detail=f"Run {run_id} not found or metrics have expired."
    )


# ─── Jobs: Read-only monitoring endpoints ─────────────────────────────────────

@app.get(
    "/jobs/stats",
    tags=["Jobs"],
    summary="Get aggregate dashboard statistics",
    response_description="Aggregate counts broken down by status, source, and average confidence.",
)
async def get_jobs_stats(store=Depends(get_store)):
    """
    Returns aggregate statistics across all jobs in the system. Useful for building
    dashboard summary cards.

    **Response includes:**
    - `total_raw` — total number of raw (unprocessed) job postings fetched
    - `total_parsed` — total jobs that have been through the AI parser
    - `avg_confidence` — average AI confidence score (0.0–1.0) across all parsed jobs
    - `by_status` — count of jobs in each status: `raw`, `parsed`, `sent`, `failed`
    - `by_source` — count of jobs per source (e.g., `workable: 150`, `myjobmag: 80`)
    """
    return await store.get_stats()


@app.get(
    "/jobs/{job_id}",
    response_model=ParsedJob,
    tags=["Jobs"],
    summary="Get a single job by ID",
    response_description="The full parsed job object with all extracted fields.",
)
async def get_job(
    job_id: str = Path(..., description="The UUID of the job to fetch"),
    store=Depends(get_store),
):
    """
    Fetch a specific parsed job by its UUID. Returns the full job object including:

    - Extracted fields (title, company, location, salary, skills, etc.)
    - AI parsing metadata (model used, confidence score, parse warnings)
    - Validation issues (if any automated checks flagged problems)
    - Current status (`parsed`, `sent`, or `failed`)

    Returns `404` if the job ID doesn't exist.
    """
    job = await store.get_by_id(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return job


@app.get(
    "/jobs",
    response_model=PagedJobs,
    tags=["Jobs"],
    summary="List processed jobs (paginated)",
    response_description="One page of parsed job objects plus pagination metadata.",
)
async def list_jobs(
    page: int = Query(default=1, ge=1, description="Page number, starting at 1."),
    page_size: int = Query(default=50, ge=1, le=500, description="Jobs per page (1–500)."),
    status: str | None = Query(default=None, description="Filter by job status. Valid values: `parsed`, `sent`, `failed`. Leave empty for all jobs."),
    store=Depends(get_store),
):
    """
    Fetch jobs page by page — never the whole table at once.

    **Status values:**
    - `parsed` — AI extraction complete, waiting for handoff
    - `sent` — successfully delivered to the matching algorithm
    - `failed` — handoff or parsing failed (can be retried with `POST /jobs/retry`)

    Jobs are returned newest-first. Walk pages with `page=1,2,3…`
    until `page > total_pages`.
    """
    offset = (page - 1) * page_size
    jobs = await store.get_all_jobs(limit=page_size, offset=offset, status=status)
    total = await store.get_jobs_count(status=status)
    total_pages = (total + page_size - 1) // page_size if total else 0
    return PagedJobs(
        jobs=jobs, page=page, page_size=page_size, total=total, total_pages=total_pages
    )


@app.post(
    "/jobs/retry",
    response_model=list[PipelineResult],
    tags=["Jobs"],
    summary="Retry handoff for failed jobs",
    response_description="A list of results — one per job ID — showing whether the retry succeeded.",
)
async def retry_failed_jobs(
    body: RetryHandoffRequest,
    pipeline: JobPipeline = Depends(get_pipeline),
):
    """
    Retry the handoff step for one or more jobs that previously failed.
    Only jobs with `status: failed` can be retried.

    This is the only "manual action" endpoint — it exists because network errors
    or downstream outages can cause handoff failures that are transient and worth retrying.

    **This is NOT a human approval step** — it simply re-sends the already-parsed data.
    """
    tasks = [pipeline.retry_handoff(jid) for jid in body.job_ids]
    return await asyncio.gather(*tasks)


# ─── System ────────────────────────────────────────────────────────────────────

@app.get(
    "/metrics",
    tags=["System"],
    summary="Get pipeline performance metrics",
    response_description="In-memory snapshot of pipeline performance counters and recent run history.",
)
async def get_metrics():
    """
    Returns a full snapshot of pipeline performance metrics. Useful for
    monitoring dashboards and debugging.

    **Response includes:**
    - `lifetime` — total runs, total jobs fetched/parsed/errors across all time
    - `per_source` — lifetime job counts and error counts broken down by source
    - `active_runs` — details of any currently running ingestion jobs
    - `recent_runs` — history of the last 10 completed runs with timing and counts
    """
    return get_metrics_collector().get_snapshot()


@app.get(
    "/health",
    tags=["System"],
    summary="Health check",
    response_description="Simple liveness check response.",
)
async def health():
    """
    Basic liveness check. Returns `{"status": "ok"}` when the server is running.
    Use this for load balancer health probes or uptime monitoring.
    """
    return {"status": "ok"}
