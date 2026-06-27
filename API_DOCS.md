# API Documentation — ScuibJobsAi Pipeline

Base URL: `http://localhost:8000`
Interactive docs: `http://localhost:8000/docs` (Swagger UI)

## Overview

The pipeline ingests jobs from multiple sources, parses them (LLM or structured fallback), stores them for human review, then hands them off to the matching algorithm once approved.

**Data flow:** `Ingest → Parse → Validate → Store → [Human Review] → Handoff`

---

## Ingestion Endpoints

### `POST /ingest/manual`
Paste a raw job description for one-off parsing.

**Request body:**
```json
{
  "raw_text": "Senior Backend Engineer - Remote\nTechCorp Inc | $140k-$180k\n\nPython, Go, 5+ yrs exp",
  "source_url": "https://example.com/job/123"
}
```
`source_url` is optional.

**Response:**
```json
{
  "success": true,
  "job_id": "uuid-here",
  "status": "parsed",
  "message": "",
  "errors": []
}
```
On success `status` is `parsed`. The job is now in the review queue.

---

### `POST /ingest/bulk`
Kick off a multi-source ingestion run in the background. Returns immediately — poll progress with the returned `run_id`.

**Request body:**
```json
{
  "queries": ["software engineer", "backend developer"],
  "locations": ["remote", "Nigeria"],
  "sources": ["workable", "myjobmag", "fuzu"],
  "target_count": 100,
  "remote_only": false,
  "date_posted": "week"
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `queries` | `list[str]` | `["software engineer"]` | Search queries to run across sources |
| `locations` | `list[str]` | `["remote", "United States"]` | Locations to search |
| `sources` | `list[str]` | `["jsearch_api", "indeed_rss", "adzuna_api"]` | Sources to use. Valid values: `workable`, `myjobmag`, `fuzu`, `jobgurus`, `jobberman`, `jsearch_api`, `indeed_rss`, `adzuna_api` |
| `target_count` | `int` | `200` | Stop after this many unique jobs (max 2000) |
| `remote_only` | `bool` | `false` | Only fetch remote jobs |
| `date_posted` | `str` | `"week"` | `"today"`, `"3days"`, `"week"`, `"month"` |

**Response:**
```json
{
  "message": "Bulk ingestion run scheduled in background.",
  "run_id": "uuid-here"
}
```

---

### `GET /ingest/runs/{run_id}/status`
Poll the progress of a bulk ingestion run.

**Response (running):**
```json
{
  "run_id": "uuid-here",
  "state": "running",
  "fetched": 45,
  "parsed": 30,
  "errors": 2,
  "duplicates": 5,
  "per_source": {
    "workable": 20,
    "myjobmag": 25
  },
  "message": "Ingestion run is currently active and processing."
}
```

**Response (completed):**
```json
{
  "run_id": "uuid-here",
  "state": "completed",
  "fetched": 100,
  "parsed": 98,
  "errors": 2,
  "duplicates": 15,
  "per_source": {
    "workable": 50,
    "myjobmag": 50
  },
  "message": "Ingestion run completed successfully."
}
```

---

## Review Queue Endpoints

### `GET /jobs/pending?limit=50`
Fetch jobs awaiting human review. Returns up to `limit` jobs ordered by parse time (oldest first).

**Response:**
```json
[
  {
    "id": "uuid",
    "raw_id": "uuid",
    "status": "parsed",
    "job_title": "Senior Backend Engineer",
    "company": "TechCorp",
    "location": "Remote, US",
    "remote": true,
    "salary": {
      "min": 140000,
      "max": 180000,
      "currency": "USD",
      "period": "yearly"
    },
    "required_skills": ["Python", "Go", "PostgreSQL"],
    "employment_type": "full-time",
    "description_clean": "...",
    "confidence": 0.7,
    "validation_issues": [],
    "parse_warnings": ["Used structured fallback (no LLM)"],
    "model_used": "structured_parser",
    "source": "workable"
  }
]
```

### `GET /jobs/{job_id}`
Fetch a single job by ID.

### `GET /jobs/stats`
Aggregate statistics across all jobs.

**Response:**
```json
{
  "total_raw": 150,
  "total_parsed": 145,
  "avg_confidence": 0.72,
  "by_status": {
    "parsed": 120,
    "sent": 20,
    "rejected": 3,
    "failed": 2
  },
  "by_source": {
    "workable": 80,
    "myjobmag": 65
  }
}
```

---

## Review Action Endpoints

### `POST /jobs/{job_id}/approve`
Human approves a pending job. Sends it to the handoff (matching algorithm).

**Request body:**
```json
{
  "reviewer": "silas",
  "notes": "Looks good, all fields present"
}
```

**Response:**
```json
{
  "success": true,
  "job_id": "uuid",
  "status": "sent",
  "message": ""
}
```

### `POST /jobs/{job_id}/reject`
Human rejects a job. It will not be forwarded downstream.

**Request body:**
```json
{
  "reviewer": "silas",
  "notes": "Not a real job posting"
}
```

### `POST /jobs/bulk-action`
Approve or reject multiple jobs at once with optional filters.

**Request body:**
```json
{
  "action": "approve",
  "reviewer": "silas",
  "reason": "Auto: good quality",
  "confidence_min": 0.8,
  "exclude_flagged": true,
  "job_ids": null
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `action` | Yes | `"approve"` or `"reject"` |
| `reviewer` | Yes | Who is performing the action |
| `reason` | No | Notes for the action |
| `confidence_min` | No | Only act on jobs with confidence >= this |
| `exclude_flagged` | No | Skip jobs with validation issues |
| `job_ids` | No | List of specific job IDs to act on. If null, applies to all pending jobs matching other filters |

---

## System Endpoints

### `GET /health`
Liveness check. Always returns `{"status": "ok"}` when the server is up.

### `GET /metrics`
In-memory pipeline metrics snapshot.

**Response:**
```json
{
  "lifetime": {
    "total_runs": 5,
    "total_fetched": 500,
    "total_parsed": 480,
    "total_errors": 20
  },
  "per_source": {
    "totals": {
      "workable": 300,
      "myjobmag": 200
    },
    "errors": {
      "myjobmag": 5
    }
  },
  "active_runs": {},
  "recent_runs": [...]
}
```

---

## Data Sources — Ingester Reference

| Source | Type | Class | URL Pattern | Config |
|--------|------|-------|-------------|--------|
| Workable | JSON API | `WorkableIngester` | `jobs.workable.com/api/v1/jobs` | No auth needed |
| MyJobMag | HTML | `MyJobMagIngester` | `www.myjobmag.com/search/jobs?q=` | No auth needed |
| Fuzu | HTML | `FuzuIngester` | `www.fuzu.com/{location}/search?q=` | No auth needed |
| JobGurus | HTML | `JobGurusIngester` | `www.jobgurus.com.ng/jobs` | No auth needed |
| Jobberman | HTML | `JobbermanIngester` | `www.jobberman.com/jobs?q=` | No auth needed |
| JSearch | REST | `JSearchIngester` | `jsearch.p.rapidapi.com` | RapidAPI key |
| Indeed RSS | RSS | `IndeedRSSIngester` | `www.indeed.com/rss` | No auth needed |
| Adzuna | REST | `AdzunaIngester` | `api.adzuna.com` | App ID + Key |
| Manual | N/A | `ManualIngester` | N/A | No auth needed |

---

## Frontend Integration Guide

### Typical user flow

1. **Start a bulk ingestion run** → `POST /ingest/bulk`
2. **Poll for progress** → `GET /ingest/runs/{run_id}/status` (poll every 3-5s)
3. **Display pending jobs** → `GET /jobs/pending`
4. **Show job details** → `GET /jobs/{job_id}`
5. **Admin approves/rejects** → `POST /jobs/{job_id}/approve` or `/reject`
6. **Bulk action** → `POST /jobs/bulk-action` with filters
7. **Monitor dashboard** → `GET /jobs/stats` and `GET /metrics`

### Important behaviors

- **Parsing can take 2-10s per job** with LLM (slower) or ~0.1s per job with structured parser (faster, lower quality)
- **Bulk ingestion runs in background** — the `POST /ingest/bulk` endpoint returns immediately with a `run_id`. Poll the status endpoint to track progress.
- **Jobs are returned confidence-sorted** from the pending queue. Higher confidence = more reliable extraction.
- **Validation issues are informational** — they flag potential problems (missing title, suspicious salary) but don't block the job. The reviewer decides.
- **Auto-approve** — set `AUTO_APPROVE_CONFIDENCE_THRESHOLD=0.9` in `.env` to automatically approve and handoff high-confidence jobs, bypassing the review queue.
- **Handoff destination** — when `HANDOFF_ENDPOINT_URL` is set, approved jobs are POSTed there. When unset, they're written to `handoff_output.jsonl`.

### Error handling

- Non-existent job IDs return `404`
- Wrong job status for approve/reject returns `400`
- Network errors during ingestion are logged but don't crash the pipeline
- Circuit breakers temporarily skip failing sources

---

## Backend Architecture Notes

### Adding a new job source

1. Add the source to `JobSource` enum in [`core/models.py`](core/models.py)
2. Create a new ingester class extending `BaseIngester` in [`ingestion/custom_ingesters.py`](ingestion/custom_ingesters.py) (or `ingestion/ingesters.py` for API-based sources)
3. Register it in `build_dynamic_aggregator()` in [`api/dependencies.py`](api/dependencies.py)
4. Add any new env vars to `.env.example` and update the env table above

### Parser fallback chain

1. `HybridParser` tries `GeminiParser` (LLM)
2. If Gemini fails (all retries exhausted) or confidence < 0.3 → falls to `StructuredParser` (regex)
3. StructuredParser extracts title, company, location, salary, skills from raw text
4. For Workable sources, StructuredParser uses API metadata fields directly (more accurate)

### Adding new build/test commands

```bash
# Install all dependencies
pip install -r requirements.txt

# Run all tests
PYTHONPATH=. python scripts/bulk_test.py

# Run single manual ingest test
PYTHONPATH=. python scripts/phase1_test.py
```
