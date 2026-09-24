# API Documentation — ScuibJobsAi Pipeline

**Base URL:** `http://localhost:8000`  
**Interactive docs:** `http://localhost:8000/docs` (Swagger UI)

---

## Overview

This API powers a **fully automatic** job data pipeline. It fetches job listings from multiple sources, parses them with AI (Google Gemini), validates the data quality, and sends every job to the downstream matching algorithm — all without any human approval step.

**Data flow:**

```
Fetch from sources → Deduplicate → AI Parse → Validate → Auto Handoff
```

**Job statuses:**

| Status | Meaning |
|--------|---------|
| `raw` | Just fetched from a source, not yet parsed |
| `parsed` | AI extraction complete, about to be handed off |
| `sent` | Successfully delivered to the matching algorithm |
| `failed` | Handoff failed — can be retried with `POST /jobs/retry` |

---

## Ingestion Endpoints

These endpoints start the pipeline. Jobs are fetched, parsed, and handed off automatically.

### `POST /ingest/manual`

**What it does:** Submit a single raw job posting as text. The pipeline will parse it with AI, validate it, and hand it off to the matching algorithm automatically.

**When to use:** Testing, one-off jobs, or pasting job text that didn't come from a supported source.

**Request body:**
```json
{
  "raw_text": "Senior Backend Engineer - Remote\nTechCorp Inc | $140k-$180k\n\nWe're looking for a Python/Go developer with 5+ years experience.\nRequired: Python, Go, PostgreSQL, Docker\nNice to have: Kubernetes, AWS",
  "source_url": "https://example.com/jobs/senior-backend-eng"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `raw_text` | `string` | Yes | The full text of the job posting. Can be messy HTML or plain text — the AI handles cleanup. |
| `source_url` | `string` | No | URL where the job was originally posted. For reference only. |

**Response (success):**
```json
{
  "success": true,
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "sent",
  "message": "",
  "errors": []
}
```

**Response (parse succeeded but handoff failed):**
```json
{
  "success": false,
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "failed",
  "message": "Parse OK, handoff failed: Connection refused",
  "errors": ["No required skills extracted — verify manually"]
}
```

The `errors` field contains validation warnings (informational — they don't block the job).

---

### `POST /ingest/trigger`

**What it does:** Starts a simple single-source ingestion run in the background.

**When to use:** Quick ingestion from one source. For multi-source runs, use `/ingest/bulk` instead.

**Request body:**
```json
{
  "source": "indeed_rss",
  "query": "software engineer",
  "location": "remote"
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `source` | `string` | `"indeed_rss"` | Job source to fetch from. Options: `indeed_rss`, `jsearch_api`, `adzuna_api`, `workable`, `myjobmag`, `fuzu`, `jobgurus`, `jobberman`, `manual` |
| `query` | `string` | `"software engineer"` | Search query |
| `location` | `string` | `"remote"` | Location filter |

**Response:**
```json
{
  "message": "Ingestion cycle started in background",
  "source": "indeed_rss"
}
```

---

### `POST /ingest/bulk` ⭐ Main Endpoint

**What it does:** Starts a multi-source bulk ingestion run in the background. This is the **primary way to fetch jobs**. It searches across multiple sources, deduplicates, parses with AI, validates, and hands off everything automatically.

**When to use:** Whenever you want to pull a batch of fresh jobs from multiple sources at once.

**Request body:**
```json
{
  "queries": ["software engineer", "backend developer", "python developer"],
  "locations": ["remote", "Nigeria", "United States"],
  "sources": ["workable", "myjobmag", "fuzu", "jobgurus", "jobberman"],
  "target_count": 200,
  "remote_only": false,
  "date_posted": "week"
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `queries` | `list[string]` | `["software engineer"]` | Search queries to run across all sources. Each query is run against every source+location combo. |
| `locations` | `list[string]` | `["remote", "United States"]` | Locations to search in. |
| `sources` | `list[string]` | `["jsearch_api", "indeed_rss", "adzuna_api"]` | Which sources to pull from. Valid: `workable`, `myjobmag`, `fuzu`, `jobgurus`, `jobberman`, `jsearch_api`, `indeed_rss`, `adzuna_api` |
| `target_count` | `integer` | `200` | Stop after fetching this many unique jobs. Range: 1–2000. |
| `remote_only` | `boolean` | `false` | If `true`, only fetch remote/work-from-home jobs. |
| `date_posted` | `string` | `"week"` | How recent the job posts should be. Options: `today`, `3days`, `week`, `month`. |

**Response:**
```json
{
  "message": "Bulk ingestion run scheduled in background.",
  "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

Save the `run_id` — you'll need it to poll for progress.

---

### `GET /ingest/runs/{run_id}/status`

**What it does:** Check the progress of a running or recently completed bulk ingestion run.

**When to use:** Poll this every 3–5 seconds after calling `POST /ingest/bulk` to show progress in the UI.

**URL parameter:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `run_id` | `string` | The UUID returned by `POST /ingest/bulk` |

**Response (while running):**
```json
{
  "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "state": "running",
  "fetched": 85,
  "parsed": 60,
  "errors": 3,
  "duplicates": 12,
  "per_source": {
    "workable": 40,
    "myjobmag": 30,
    "fuzu": 15
  },
  "message": "Ingestion run is currently active and processing."
}
```

**Response (completed):**
```json
{
  "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "state": "completed",
  "fetched": 200,
  "parsed": 185,
  "errors": 5,
  "duplicates": 30,
  "per_source": {
    "workable": 80,
    "myjobmag": 60,
    "fuzu": 30,
    "jobgurus": 15,
    "jobberman": 15
  },
  "message": "Ingestion run completed successfully."
}
```

| Field | Description |
|-------|-------------|
| `state` | `"running"`, `"completed"`, or `"failed"` |
| `fetched` | Total raw job postings pulled from sources |
| `parsed` | Jobs successfully parsed by AI |
| `errors` | Jobs that failed during parsing or handoff |
| `duplicates` | Jobs skipped because they were already in the system |
| `per_source` | Breakdown of job counts by source |

**Error:** Returns `404` if the run ID is not found.

---

## Job Endpoints

Read-only endpoints for browsing and monitoring processed jobs. Use these to build your dashboard.

### `GET /jobs`

**What it does:** Lists processed jobs **one page at a time**, with optional filtering by status. Never returns the whole table at once.

**When to use:** Building the main jobs table/list view in the dashboard. Walk `page=1,2,3…` until `page > total_pages`.

**Query parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `page` | `integer` | `1` | Page number, starting at 1 |
| `page_size` | `integer` | `50` | Jobs per page (1–500) |
| `status` | `string` | (all) | Filter by status: `parsed`, `sent`, `failed` |

**Example requests:**
```
GET /jobs                                  → Page 1, 50 newest jobs
GET /jobs?page=2&page_size=50              → Next 50 jobs
GET /jobs?status=failed                    → Page 1 of failed jobs
GET /jobs?status=sent&page_size=200        → Page 1, 200 sent jobs
```

**Response:**
```json
{
  "jobs": [
    {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "raw_id": "660f9500-f30c-52e5-b827-557766551111",
    "status": "sent",
    "source": "workable",
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
    "required_skills": ["Python", "Go", "PostgreSQL", "Docker"],
    "preferred_skills": ["Kubernetes", "AWS"],
    "years_experience": 5,
    "education_level": "Bachelor's",
    "employment_type": "full-time",
    "description_clean": "We are looking for a senior backend engineer...",
    "parsed_at": "2026-07-13T15:30:00Z",
    "model_used": "gemini-2.0-flash",
    "confidence": 0.92,
    "parse_warnings": [],
    "validation_issues": []
    }
  ],
  "page": 1,
  "page_size": 50,
  "total": 863,
  "total_pages": 18
}
```

---

### `GET /jobs/{job_id}`

**What it does:** Fetch a single job by its UUID.

**When to use:** Showing a detailed job view when the user clicks on a job in the list.

**Response:** Same schema as individual items in `GET /jobs`. Returns `404` if not found.

---

### `GET /jobs/stats`

**What it does:** Returns aggregate statistics across all jobs. Perfect for dashboard summary cards.

**When to use:** Building the top-level dashboard with counters and charts.

**Response:**
```json
{
  "total_raw": 500,
  "total_parsed": 480,
  "avg_confidence": 0.78,
  "by_status": {
    "parsed": 10,
    "sent": 460,
    "failed": 10
  },
  "by_source": {
    "workable": 200,
    "myjobmag": 120,
    "fuzu": 80,
    "jobgurus": 50,
    "jobberman": 50
  }
}
```

| Field | Description |
|-------|-------------|
| `total_raw` | Total job postings fetched from all sources |
| `total_parsed` | Jobs that have been through the AI parser |
| `avg_confidence` | Average AI confidence score (0.0–1.0). Higher = more reliable extraction. |
| `by_status` | Count of jobs in each status |
| `by_source` | Count of raw jobs per source |

---

### `POST /jobs/retry`

**What it does:** Retries the handoff step for one or more jobs that previously failed. Only jobs with `status: failed` can be retried.

**When to use:** When some jobs failed to hand off (usually due to network errors or downstream outages) and you want to try again.

**This is NOT a human approval step** — it simply re-sends the already-parsed data.

**Request body:**
```json
{
  "job_ids": [
    "550e8400-e29b-41d4-a716-446655440000",
    "661f9500-f30c-52e5-b827-557766551111"
  ]
}
```

**Response:**
```json
[
  {
    "success": true,
    "job_id": "550e8400-e29b-41d4-a716-446655440000",
    "status": "sent",
    "message": "",
    "errors": []
  },
  {
    "success": false,
    "job_id": "661f9500-f30c-52e5-b827-557766551111",
    "status": "failed",
    "message": "Connection refused",
    "errors": []
  }
]
```

---

## System Endpoints

### `GET /health`

**Response:** `{"status": "ok"}` — returns this whenever the server is running. Use for uptime monitoring.

### `GET /metrics`

**What it does:** Returns pipeline performance metrics. Useful for monitoring dashboards.

**Response:**
```json
{
  "lifetime": {
    "total_runs": 12,
    "total_fetched": 2400,
    "total_parsed": 2300,
    "total_errors": 100
  },
  "per_source": {
    "totals": {
      "workable": 800,
      "myjobmag": 600,
      "fuzu": 400,
      "jobgurus": 300,
      "jobberman": 300
    },
    "errors": {
      "fuzu": 20,
      "jobgurus": 15
    }
  },
  "active_runs": {},
  "recent_runs": [
    {
      "run_id": "a1b2c3d4-...",
      "duration_seconds": 45.2,
      "jobs_per_second": 4.2,
      "fetched": 200,
      "parsed": 190,
      "validated": 180,
      "flagged": 10,
      "errors": 5,
      "duplicates": 30,
      "per_source": {"workable": 100, "myjobmag": 100}
    }
  ]
}
```

---

## Data Models Reference

### ParsedJob (main job object)

This is the primary data object returned by job endpoints.

| Field | Type | Description |
|-------|------|-------------|
| `id` | `string (UUID)` | Unique identifier for this parsed job |
| `raw_id` | `string (UUID)` | Reference to the original raw job posting |
| `status` | `string` | Current status: `raw`, `parsed`, `sent`, `failed` |
| `source` | `string` | Which source it came from: `workable`, `myjobmag`, etc. |
| `job_title` | `string` | Extracted job title |
| `company` | `string \| null` | Company name (may be null if not found) |
| `location` | `string \| null` | Job location |
| `remote` | `boolean` | Whether the job is remote/work-from-home |
| `salary` | `object \| null` | Salary range (see below) |
| `required_skills` | `list[string]` | Skills marked as required |
| `preferred_skills` | `list[string]` | Skills marked as nice-to-have |
| `years_experience` | `integer \| null` | Required years of experience |
| `education_level` | `string \| null` | Required education (e.g., "Bachelor's") |
| `employment_type` | `string \| null` | `full-time`, `contract`, `part-time` |
| `description_clean` | `string \| null` | AI-cleaned job description text |
| `parsed_at` | `string (ISO 8601)` | When the AI parsing was completed |
| `model_used` | `string` | Which AI model was used (e.g., `gemini-2.0-flash` or `structured_parser`) |
| `confidence` | `float (0–1)` | How confident the AI is in the extraction. Higher = better. LLM: 0.8–0.95, regex: 0.5–0.7 |
| `parse_warnings` | `list[string]` | Warnings from the parser (e.g., "Used structured fallback") |
| `validation_issues` | `list[string]` | Issues found by automated validation (e.g., "Missing company name") |

### Salary object

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `min` | `integer \| null` | — | Minimum salary |
| `max` | `integer \| null` | — | Maximum salary |
| `currency` | `string` | `"USD"` | Currency code |
| `period` | `string` | `"yearly"` | `hourly`, `monthly`, or `yearly` |

### PipelineResult

Returned by ingestion and retry endpoints.

| Field | Type | Description |
|-------|------|-------------|
| `success` | `boolean` | Whether the operation succeeded |
| `job_id` | `string \| null` | UUID of the affected job |
| `status` | `string \| null` | Final status of the job |
| `message` | `string` | Error message if failed |
| `errors` | `list[string]` | Validation warnings (informational) |

---

## Frontend Integration Guide

### Typical flow for the admin panel

1. **Start ingestion** → `POST /ingest/bulk` with queries/sources/locations
2. **Show progress** → Poll `GET /ingest/runs/{run_id}/status` every 3–5 seconds until `state` is `completed`
3. **Dashboard view** → `GET /jobs/stats` for summary cards, `GET /metrics` for performance charts
4. **Jobs table** → `GET /jobs` for the full list, with status filter tabs
5. **Job detail** → `GET /jobs/{id}` when user clicks a row
6. **Retry failures** → `POST /jobs/retry` with the IDs of failed jobs

### Tips

- **Parsing speed:** 2–10 seconds per job with Gemini, ~0.1s with regex fallback
- **Bulk runs are async** — the `POST /ingest/bulk` returns immediately. Always poll for status.
- **Confidence score** is the best quality indicator. Jobs parsed with Gemini have 0.8–0.95 confidence; regex gives 0.5–0.7
- **Validation issues are informational** — they flag potential problems but don't block the pipeline
- **All timestamps are ISO 8601 / UTC**

### Error handling

| HTTP Code | When |
|-----------|------|
| `200` | Success |
| `404` | Job ID or run ID not found |
| `422` | Invalid request body (Pydantic validation error) |
| `500` | Server error |

---

## Data Sources — Ingester Reference

| Source | Enum Value | Type | URL Pattern | Auth |
|--------|-----------|------|-------------|------|
| Workable | `workable` | JSON API | `jobs.workable.com/api/v1/jobs` | None |
| MyJobMag | `myjobmag` | HTML scrape | `www.myjobmag.com/search/jobs?q=` | None |
| Fuzu | `fuzu` | HTML scrape | `www.fuzu.com/{location}/search?q=` | None |
| JobGurus | `jobgurus` | HTML scrape | `www.jobgurus.com.ng/jobs` | None |
| Jobberman | `jobberman` | HTML scrape | `www.jobberman.com/jobs?q=` | None |
| JSearch | `jsearch_api` | REST API | `jsearch.p.rapidapi.com` | RapidAPI key |
| Indeed RSS | `indeed_rss` | RSS feed | `www.indeed.com/rss` | None |
| Adzuna | `adzuna_api` | REST API | `api.adzuna.com` | App ID + Key |
