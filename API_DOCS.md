# API Documentation — ScuibJobsAi Pipeline

Welcome! This document is the complete guide to the ScuibJobsAi pipeline API. It is written for everyone who touches the system: frontend developers building dashboards, backend developers (Anthony) integrating the matching algorithm, and anyone operating the hourly cron job.

**Base URL (local):** `http://localhost:8000`
**Base URL (production):** `https://scuibjobsai.onrender.com`
**Interactive docs:** Open `/docs` on either base URL in your browser (Swagger UI — you can try every endpoint there).

> **One-sentence summary:** You tell the pipeline *what* jobs to fetch, it fetches them from 8 job boards, extracts structured data with AI, and delivers every job to the matching algorithm automatically. There is no human approval step anywhere in the flow.

---

## Table of Contents

1. [How the Pipeline Works](#how-the-pipeline-works)
2. [Job Lifecycle & Statuses](#job-lifecycle--statuses)
3. [Starting a Run (Ingestion Endpoints)](#starting-a-run-ingestion-endpoints)
   - [`POST /ingest/manual`](#post-ingestmanual)
   - [`POST /ingest/trigger`](#post-ingesttrigger)
   - [`POST /ingest/bulk` — the main endpoint](#post-ingestbulk--main-endpoint)
   - [`GET /ingest/runs/{run_id}/status`](#get-ingestrunsrun_idstatus)
4. [Reading Jobs (Job Endpoints)](#reading-jobs-job-endpoints)
   - [`GET /jobs` (paginated)](#get-jobs)
   - [`GET /jobs/{job_id}`](#get-jobsjob_id)
   - [`GET /jobs/stats`](#get-jobsstats)
   - [`POST /jobs/retry`](#post-jobsretry)
5. [Health & Monitoring (System Endpoints)](#health--monitoring-system-endpoints)
6. [What the Matching Algorithm Receives (Handoff Contract)](#what-the-matching-algorithm-receives-handoff-contract)
7. [Data Models Reference](#data-models-reference)
8. [How Duplicates Are Prevented](#how-duplicates-are-prevented)
9. [Posted Dates & UI Filtering](#posted-dates--ui-filtering)
10. [Hourly Cron Cookbook (Cloudflare)](#hourly-cron-cookbook-cloudflare)
11. [Configuration (Environment Variables)](#configuration-environment-variables)
12. [Frontend Integration Guide](#frontend-integration-guide)
13. [Troubleshooting](#troubleshooting)
14. [Data Sources — Ingester Reference](#data-sources--ingester-reference)

---

## How the Pipeline Works

Think of the pipeline as a conveyor belt with five stations. A job posting enters on the left as messy text and comes out on the right as clean, structured data delivered to the matching algorithm:

```
Fetch from boards → Deduplicate → AI Parse → Validate → Auto Handoff
     (ingestion)      (dedup)      (parsing)   (checks)   (delivery)
```

Here is what happens at each station, in plain language:

1. **Fetch.** The pipeline contacts the configured job boards (Workable, MyJobMag, Fuzu, JobGurus, Jobberman, JSearch, Indeed RSS, Adzuna) and pulls raw job postings. Each posting keeps its original board URL (`source_url`) so users can always click through to apply.
2. **Deduplicate.** The same job often appears on multiple boards — or is still listed when the next hourly run starts. The pipeline filters out repeats using the board's own job ID plus a content fingerprint (title + company). See [How Duplicates Are Prevented](#how-duplicates-are-prevented).
3. **AI Parse.** A language model reads the messy posting and extracts structured fields: title, company, location, salary, skills, experience, employment type, a clean summary, the apply link, and the posted date. The parser chain is **Gemini → Groq → Structured (regex)**: if Gemini is rate-limited or fails, Groq takes over; if both are unavailable, a regex parser still extracts the basics. Every result carries a `confidence` score (0–1) so you know how much to trust it.
4. **Validate.** Automated rules check the extracted data (is there a title? any skills? sane salary? an apply link? a posted date?). Problems are recorded as `validation_issues` on the job — they are informational annotations, they never block the pipeline by themselves.
5. **Auto Handoff.** Every parsed job is immediately POSTed to the downstream matching algorithm (`HANDOFF_ENDPOINT_URL`, i.e. Anthony's `POST /api/jobs/ingest/`). Two safety gates can hold a job back (see below); everything else flows straight through. No human clicks anything.

**The two safety gates** (both configured with environment variables):

- **Apply-link gate** (`REQUIRE_APPLICATION_LINK`, default `true`). Jobs with no apply URL are parsed and stored but **never handed off**. A job users can't apply to never reaches the site — this protects the brand.
- **Freshness gate** (`MAX_JOB_AGE_DAYS`, default `0` = off). When set to `N`, jobs whose board-posted date is older than N days are parsed and stored but **never handed off**. Jobs with an unknown posted date are always kept (the pipeline never punishes a job for missing data).

Jobs held back by a gate stay in status `parsed` (visible via `GET /jobs?status=parsed`) and are counted as `skipped` in run statistics — they are not errors. Jobs whose parsing completely failed (`[PARSE FAILED]`) are likewise never handed off.

---

## Job Lifecycle & Statuses

Every job in the system has a `status` that tells you exactly where it is on the conveyor belt:

| Status | Meaning | What to do |
|--------|---------|------------|
| `parsed` | AI extraction is done. The job is either waiting for handoff, or was held back by a safety gate (no apply link / too old). | Inspect with `GET /jobs?status=parsed` if you're curious why jobs aren't flowing. |
| `sent` | Successfully delivered to the matching algorithm. This is the happy end state. | Nothing — the job is now Anthony's backend's responsibility. |
| `failed` | Something broke: parsing crashed or the handoff POST failed (network error, downstream down). | Retry with `POST /jobs/retry`. If it keeps failing, check [Troubleshooting](#troubleshooting). |

> **Note:** You may see `raw` in the status enum — it is a transient internal state for freshly fetched postings. The job-listing endpoints only ever show `parsed`, `sent`, or `failed`.

**Job IDs are stable across runs.** A parsed job's `id` is deterministically derived from `source + board job ID`, so the same board posting gets the same ID every hour, every day. Downstream systems should dedup on `job_id` (Anthony's backend stores it as `source_job_id`).

---

## Starting a Run (Ingestion Endpoints)

These endpoints put jobs onto the conveyor belt. Bulk runs execute **in the background**: the endpoint returns immediately with a `run_id`, and you poll for progress separately.

### `POST /ingest/manual`

**What it does:** Submit one single raw job posting as text. The pipeline parses it with AI, validates it, and hands it off — all within this one request (it waits for the full flow, unlike bulk runs).

**When to use it:** Testing the parser, processing a one-off posting, or handling job text that didn't come from any supported board.

**Request body:**

```json
{
  "raw_text": "Senior Backend Engineer - Remote\nTechCorp Inc | $140k-$180k\n\nWe're looking for a Python/Go developer with 5+ years experience.\nRequired: Python, Go, PostgreSQL, Docker\nNice to have: Kubernetes, AWS",
  "source_url": "https://example.com/jobs/senior-backend-eng"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `raw_text` | `string` | Yes | The full text of the job posting. It can be messy HTML or plain text — the AI handles cleanup. Longer is better; the parser reads up to ~8,000 characters. |
| `source_url` | `string` | No | URL where the job was originally posted. Strongly recommended: without it, the job has no apply link and (with the default safety gate on) will be parsed but never handed off. |

**Response — end-to-end success:**

```json
{
  "success": true,
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "sent",
  "message": "",
  "errors": []
}
```

**Response — parsed fine, but handoff failed:**

```json
{
  "success": false,
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "failed",
  "message": "Parse OK, handoff failed: Connection refused",
  "errors": ["No required skills extracted — verify manually"]
}
```

**Response — held back by a safety gate** (no apply link, or posted date too old):

```json
{
  "success": true,
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "parsed",
  "message": "Parsed and stored, handoff skipped: no application URL",
  "errors": []
}
```

The `errors` array holds validation warnings. They are informational — a job can have warnings and still be `sent`.

---

### `POST /ingest/trigger`

**What it does:** Starts a simple ingestion run from a single source, using the server's configured defaults. Runs in the background; the endpoint returns immediately.

**When to use it:** Quick top-ups from one board. For anything serious — multiple boards, custom queries, progress tracking — use [`POST /ingest/bulk`](#post-ingestbulk--main-endpoint) instead.

**Request body:**

```json
{
  "source": "indeed_rss",
  "query": "software engineer",
  "location": "Nigeria"
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `source` | `string` | `"indeed_rss"` | Which board to fetch from. Valid values: `indeed_rss`, `jsearch_api`, `adzuna_api`, `workable`, `myjobmag`, `fuzu`, `jobgurus`, `jobberman`. |
| `query` | `string` | `"software engineer"` | Search keywords, e.g. `"backend developer"`, `"data analyst"`. |
| `location` | `string` | `"remote"` | Location filter, e.g. `"Nigeria"`. (Note: the Nigerian boards are inherently Nigerian and largely ignore this field.) |

**Response:**

```json
{
  "message": "Ingestion cycle started in background",
  "source": "indeed_rss"
}
```

There is no `run_id` for this endpoint — watch `GET /jobs/stats` or `GET /metrics` to see the results land.

---

### `POST /ingest/bulk` ⭐ Main Endpoint

**What it does:** Starts a multi-source bulk ingestion run in the background. **This is the primary way to fetch jobs** — it is what the hourly cron calls. In one run it searches across all configured boards, removes duplicates, parses everything with AI, validates, and hands off every qualifying job automatically.

**When to use it:** Whenever you want a fresh batch of jobs — on a schedule (cron) or on demand.

**Recommended request body (Nigeria-only, cron-friendly):**

```json
{
  "queries": ["software engineer", "backend developer", "frontend developer", "data analyst"],
  "locations": ["Nigeria"],
  "sources": ["workable", "myjobmag", "fuzu", "jobgurus", "jobberman"],
  "target_count": 60,
  "remote_only": false,
  "date_posted": "month"
}
```

**All fields:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `queries` | `list[string]` | `["software engineer"]` | Search keywords. Each query runs against every source × location combination, so keep this list short (3–5 items) — 10 queries × 8 sources means up to 80 fetchers and a very long run. |
| `locations` | `list[string]` | `["Nigeria"]` | Locations to search in. Use `["Nigeria"]` for a strictly Nigerian feed. (Adding `"remote"` pulls in mostly foreign remote listings — not recommended.) |
| `sources` | `list[string]` | `["jsearch_api", "indeed_rss", "adzuna_api"]` | Which boards to pull from. Valid values: `workable`, `myjobmag`, `fuzu`, `jobgurus`, `jobberman`, `jsearch_api`, `indeed_rss`, `adzuna_api`. Start with the five Nigerian/global boards; add the API sources once jobs are flowing. |
| `target_count` | `integer` | `200` | Stop fetching after this many unique jobs (range 1–2000). **Keep this small on free hosting** (50–80): a 200-target run with HTML scraping can take longer than the platform's idle timeout and get killed mid-run. The target is split evenly per board (target ÷ distinct sources), so fast boards can't starve slow ones — capped boards buffer extras and drain them after all boards finish. |
| `remote_only` | `boolean` | `false` | If `true`, only fetch remote/work-from-home jobs. Only meaningful for JSearch. |
| `date_posted` | `string` | `"week"` | Recency window, enforced server-side by JSearch and Adzuna. Options: `today`, `3days`, `week`, `month`. Use `"month"` (widest) when you want everything with dates recorded — the UI-side 24h/1wk/1mo toggles filter on the stored `posted_date` afterwards. |

**Response (returns immediately — the run continues in the background):**

```json
{
  "message": "Bulk ingestion run scheduled in background.",
  "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
}
```

> **Important:** A `200 OK` here only means the run was *scheduled*. It says nothing about whether jobs flowed. Always follow up with [`GET /ingest/runs/{run_id}/status`](#get-ingestrunsrun_idstatus) — that is the true success signal. Save the `run_id`.

---

### `GET /ingest/runs/{run_id}/status`

**What it does:** Reports the progress of a bulk run — running or recently finished.

**When to use it:** Poll this every 20–60 seconds after `POST /ingest/bulk` (or from the cron worker) until `state` becomes `completed`. For an hourly cron, this polling response is far more meaningful than the cron platform's own "success" checkmark.

**URL parameter:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `run_id` | `string` | The UUID returned by `POST /ingest/bulk`. |

**Response — while running:**

```json
{
  "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "state": "running",
  "fetched": 85,
  "parsed": 0,
  "errors": 0,
  "duplicates": 12,
  "skipped": 0,
  "per_source": {
    "workable": 40,
    "myjobmag": 30,
    "fuzu": 15
  },
  "message": "Ingestion run is currently active and processing."
}
```

> Don't be alarmed if `parsed` stays `0` for a long time while `fetched` climbs: fetching (especially HTML detail pages, one by one) is the slow phase. Parsing and handoff happen after fetching finishes.

**Response — completed:**

```json
{
  "run_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "state": "completed",
  "fetched": 200,
  "parsed": 185,
  "errors": 5,
  "duplicates": 30,
  "skipped": 12,
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

**How to read the counters:**

| Field | Meaning | Healthy looks like |
|-------|---------|--------------------|
| `state` | `"running"` or `"completed"`. Note: even a run that hit an exception archives as `"completed"` — always check `errors` alongside it. Unknown IDs return `404`. | `completed` |
| `fetched` | Raw postings pulled from boards | Grows steadily during the run |
| `parsed` | Jobs the AI (or fallback) processed | Close to `fetched` at the end |
| `errors` | Parse crashes + failed handoffs | `0`, or small with a clear cause in logs |
| `duplicates` | Already-seen jobs filtered out | Non-zero is *good* — dedup working |
| `skipped` | Parsed but deliberately not handed off (no apply link / too old / parse failed) | `0`–moderate; high values mean check the safety gates |
| `per_source` | Yielded jobs per board | Matches the boards you asked for; a missing board likely timed out — check server logs |

**Error:** Returns `404` if the run ID is unknown (never existed, or expired from memory after a restart).

---

## Reading Jobs (Job Endpoints)

Read-only endpoints for browsing and monitoring processed jobs. Use these to build dashboards — and note that Anthony's *user-facing* job list lives on his backend (`GET /api/jobs/ingested/` etc.); these endpoints expose *our pipeline's* staging store.

### `GET /jobs`

**What it does:** Lists processed jobs **one page at a time** — it never dumps the whole table at once, no matter how many jobs have accumulated.

**When to use it:** Building a jobs table, an admin review list, or debugging ("show me everything stuck in `parsed`"). Walk `page=1,2,3…` until `page > total_pages`.

**Query parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `page` | `integer` | `1` | Page number, starting at 1. |
| `page_size` | `integer` | `50` | Jobs per page (1–500). |
| `status` | `string` | (all) | Filter by status: `parsed`, `sent`, `failed`. Leave empty for everything. |

**Example requests:**

```
GET /jobs                                  → Page 1, 50 newest jobs
GET /jobs?page=2&page_size=50              → The next 50 jobs
GET /jobs?status=failed                    → Page 1 of failed jobs (for retry triage)
GET /jobs?status=sent&page_size=200        → Page 1, 200 sent jobs
GET /jobs?status=parsed                    → Jobs held back by safety gates — start debugging here
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
      "location": "Lagos, Nigeria",
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
      "source_url": "https://jobs.workable.com/abc123",
      "application_link": "https://jobs.workable.com/abc123",
      "posted_date": "2026-09-23",
      "parsed_at": "2026-09-23T15:30:00Z",
      "model_used": "gemini-2.5-flash",
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

**How to read the pagination envelope:**

| Field | Meaning |
|-------|---------|
| `jobs` | This page's jobs, newest first. Empty array = you've walked past the last page. |
| `page` / `page_size` | Echo of what you asked for. |
| `total` | Total jobs matching the filter, across all pages. |
| `total_pages` | How many pages exist. Stop fetching when `page > total_pages`. |

---

### `GET /jobs/{job_id}`

**What it does:** Fetch one single job by its UUID, with every extracted field.

**When to use it:** Job detail views (user clicks a row in the table), or inspecting exactly what the AI extracted for a suspicious job.

**Response:** One job object, same schema as the items inside `GET /jobs`. Returns `404` if the ID doesn't exist.

---

### `GET /jobs/stats`

**What it does:** Aggregate counts across the whole store — perfect for dashboard summary cards ("240 sent, 12 failed, 30 held for review").

**When to use it:** Top-level dashboard numbers, and as a first health check ("is anything flowing at all?").

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

| Field | Meaning |
|-------|---------|
| `total_raw` | Raw postings fetched and staged. |
| `total_parsed` | Jobs that went through the AI parser. |
| `avg_confidence` | Average AI confidence (0.0–1.0). Persistently dropping values suggest boards changed their layouts. |
| `by_status` | Jobs per status. Lots of `parsed` = safety gates holding jobs back; lots of `failed` = handoff trouble. |
| `by_source` | Raw jobs per board. A board at `0` across runs probably can't be reached — check logs. |

> **Free-hosting caveat:** on Render's free tier the store above is in-memory unless Supabase is configured, so these counters reset to zero on every sleep/restart. Zeros here don't always mean "nothing ever ran" — check `GET /metrics` run history and the server logs too.

---

### `POST /jobs/retry`

**What it does:** Re-sends the handoff for one or more jobs stuck in `failed` status. Only `failed` jobs can be retried.

**When to use it:** After a transient outage — e.g. Anthony's backend was down during a run and a batch of jobs failed to deliver. Fix the downstream, then retry instead of re-scraping.

**This is NOT a human approval step** — it simply re-sends already-parsed data. There is no approval anywhere in this system.

**Request body:**

```json
{
  "job_ids": [
    "550e8400-e29b-41d4-a716-446655440000",
    "661f9500-f30c-52e5-b827-557766551111"
  ]
}
```

**Response** (one result per job ID):

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

## Health & Monitoring (System Endpoints)

### `GET /health`

**Response:** `{"status": "ok"}` — returned whenever the server process is alive. Use it for uptime monitors and load-balancer probes. (On free hosting, expect the *first* request after idle to be slow — that's a cold start, not an error.)

### `GET /metrics`

**What it does:** Returns pipeline performance counters: lifetime totals, per-board totals and errors, currently active runs, and the last 10 completed runs with timing.

**When to use it:** Operations dashboards, and answering "what happened overnight?" — `recent_runs` shows each run's fetched/parsed/errors/duplicates/skipped breakdown with `duration_seconds` and `jobs_per_second`.

**Response (abridged):**

```json
{
  "lifetime": {
    "total_runs": 12,
    "total_fetched": 2400,
    "total_parsed": 2300,
    "total_errors": 100
  },
  "per_source": {
    "totals": { "workable": 800, "myjobmag": 600, "fuzu": 400 },
    "errors": { "fuzu": 20, "jobgurus": 15 }
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
      "skipped": 12,
      "per_source": {"workable": 100, "myjobmag": 100}
    }
  ]
}
```

> Same free-hosting caveat as `/jobs/stats`: this history lives in memory and resets on sleep/restart.

---

## What the Matching Algorithm Receives (Handoff Contract)

This section is for Anthony. Every qualifying job is POSTed as JSON to `HANDOFF_ENDPOINT_URL` (production: `https://backend-api-4p3k.onrender.com/api/jobs/ingest/`), one request per job, with up to 3 retries on server errors. The payload schema:

| Field | Type | Description |
|-------|------|-------------|
| `job_id` | `string` | **Stable ID** (uuid5 of `source:board-job-id`). The same board posting always produces the same `job_id` — dedup on this (stored as `source_job_id`). |
| `job_title` | `string` | Extracted title. Never `[PARSE FAILED]` — failed parses are never handed off. |
| `company` | `string \| null` | Company name, when found. |
| `location` | `string \| null` | Job location text. |
| `remote` | `boolean` | Remote/work-from-home flag — drive the Remote toggle in the UI from this. |
| `salary_min` / `salary_max` | `integer \| null` | Salary range numbers. |
| `salary_currency` | `string` | Currency code (default `"USD"`). |
| `required_skills` / `preferred_skills` | `list[string]` | Skill lists for matching. |
| `years_experience` | `integer \| null` | Required years of experience. |
| `employment_type` | `string \| null` | `full-time`, `contract`, `part-time`, … |
| `description` | `string \| null` | AI-cleaned 2–3 sentence summary. |
| `application_link` | `string \| null` | **Where the user clicks to apply** (direct apply URL, falling back to the board posting URL). With the default safety gate on, this is always present. **Needs a matching column on the ingested-job model** so it can be stored and returned to the frontend. |
| `source_url` | `string \| null` | Original board posting URL. |
| `posted_date` | `string \| null` | Board-posted date as `YYYY-MM-DD`. **Needs a matching column** — this powers the 24h / 1 week / 1 month filter toggles in the UI. |
| `submitted_at` | `string (ISO 8601)` | When our pipeline handed the job off. |

> **Board sources are internal-only.** The payload carries `application_link` + `source_url` (so users can click through and apply) but never the board's name (`workable`, `myjobmag`, …). Users see *where to apply*, never *where we scraped it from*.

**Three integration asks** (the user-facing features depend on these):

1. **Store `application_link`** — without it, users see jobs they cannot apply for.
2. **Store `posted_date`** and expose recency filtering (24h / 1wk / 1mo) on the job-listing endpoints.
3. **Dedup on `source_job_id`** (unique constraint or upsert) — our `job_id` is stable across hourly runs, so this single check makes redelivery harmless.

---

## Data Models Reference

### ParsedJob (the main job object)

This is what `GET /jobs` and `GET /jobs/{job_id}` return.

| Field | Type | Description |
|-------|------|-------------|
| `id` | `string (UUID)` | Stable ID (uuid5 of `source:board-job-id`) when the board gave us an ID; random otherwise. |
| `raw_id` | `string (UUID)` | Reference to the original raw posting in the staging store. |
| `status` | `string` | `parsed`, `sent`, or `failed`. |
| `source` | `string` | Board it came from: `workable`, `myjobmag`, etc. |
| `job_title` | `string` | Extracted job title. |
| `company` | `string \| null` | Company name (null when not found). |
| `location` | `string \| null` | Job location text. |
| `remote` | `boolean` | Remote/work-from-home flag. |
| `salary` | `object \| null` | Salary range (see below). |
| `required_skills` | `list[string]` | Must-have skills. |
| `preferred_skills` | `list[string]` | Nice-to-have skills. |
| `years_experience` | `integer \| null` | Required years of experience. |
| `education_level` | `string \| null` | Required education (e.g. `"Bachelor's"`). |
| `employment_type` | `string \| null` | `full-time`, `contract`, `part-time`, … |
| `description_clean` | `string \| null` | AI-cleaned summary of the role. |
| `source_url` | `string \| null` | Job page on the source board. |
| `application_link` | `string \| null` | Direct apply URL (falls back to `source_url`). |
| `posted_date` | `string \| null` | Board-posted date (`YYYY-MM-DD`), when known. |
| `parsed_at` | `string (ISO 8601)` | When parsing completed. |
| `model_used` | `string` | Which parser produced this: a Gemini/Groq model name, or `structured_parser` for regex. |
| `confidence` | `float (0–1)` | Extraction confidence. Rough guide: LLM 0.8–0.95, regex 0.5–0.7, below 0.3 falls through to the next parser. |
| `parse_warnings` | `list[string]` | Parser notes (e.g. which fallback was used, ambiguous fields). |
| `validation_issues` | `list[string]` | Automated quality flags (e.g. missing skills, no apply link, no posted date). Informational only. |

### Salary object

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `min` | `integer \| null` | — | Minimum salary. |
| `max` | `integer \| null` | — | Maximum salary. |
| `currency` | `string` | `"USD"` | Currency code. |
| `period` | `string` | `"yearly"` | `hourly`, `monthly`, or `yearly`. |

### PipelineResult

Returned by ingestion (`POST /ingest/manual`) and retry (`POST /jobs/retry`) endpoints — one per job.

| Field | Type | Description |
|-------|------|-------------|
| `success` | `boolean` | Whether the operation reached its goal. |
| `job_id` | `string \| null` | UUID of the affected job. |
| `status` | `string \| null` | The job's status after the operation (`sent`, `failed`, or `parsed` when held by a safety gate). |
| `message` | `string` | Human-readable outcome, especially useful for gate skips and failures. |
| `errors` | `list[string]` | Validation warnings (informational — they don't block anything). |

---

## How Duplicates Are Prevented

The same job appears on multiple boards, and boards keep listings up for weeks — so without dedup, an hourly cron would spam the database with copies. Three layers prevent that:

1. **Within a run** — the aggregator drops repeats by board job ID (`external_id`) and by a title+company content fingerprint. These show up as `duplicates` in the run status. A non-zero number here is *good news*.
2. **Across runs (our side)** — before parsing, each fetched job is checked against the staging store's `external_id` history. **This only survives restarts with Supabase configured** (`SUPABASE_URL` + `SUPABASE_KEY`, tables created). With the in-memory store, a sleep/restart wipes the history and the next run refetches everything.
3. **Across runs (Anthony's side — the backstop)** — our `job_id` is stable per board posting, so his ingest endpoint can dedup on the stored `source_job_id` (unique constraint or upsert). This protects the user-facing database even when our staging store was wiped.

> **Bottom line for operators:** set Supabase credentials on Render *and* ask Anthony for the `source_job_id` unique constraint. With both in place, re-fetching the same listing is harmless at every layer.

---

## Posted Dates & UI Filtering

Every job carries `posted_date` (`YYYY-MM-DD`) from the board when it can be determined:

- **API sources** (Workable, JSearch, Adzuna, Indeed RSS) provide real timestamps, normalized at ingest.
- **HTML boards** (MyJobMag, Fuzu, JobGurus, Jobberman) get best-effort extraction from the page text (`2026-09-23`, `12 September 2026`, `Posted 3 days ago`, `Posted today`, …). When nothing reliable is found, `posted_date` is `null` — and the job is kept anyway (the pipeline never discards a job just for missing a date).
- The LLM parsers may also read a date written in the posting; the ingester-provided date always wins on conflict.

Two knobs control date behavior:

- **`date_posted`** (per bulk request: `today` / `3days` / `week` / `month`) — enforced *server-side by JSearch and Adzuna only*. Use a wide window (`month`) when you want everything with dates recorded.
- **`MAX_JOB_AGE_DAYS`** (env, default `0` = off) — when set to `N`, dated jobs older than N days are held back at handoff (counted as `skipped`). Dateless jobs are always kept.

For the user-facing **24h / 1 week / 1 month toggles**: filter on the stored `posted_date` (falling back to ingestion time when null). That filtering lives on Anthony's backend once the `posted_date` column exists there.

---

## Hourly Cron Cookbook (Cloudflare)

The production rhythm is one Cloudflare Worker, fired hourly, that POSTs to the bulk endpoint. A minimal-but-robust worker:

```js
export default {
  async fetch(request, env, ctx) {
    return new Response("SCUIB Jobs Cron Worker is running.");
  },

  async scheduled(event, env, ctx) {
    ctx.waitUntil((async () => {
      try {
        // 1. Schedule the run (returns immediately with a run_id)
        const resp = await fetch("https://scuibjobsai.onrender.com/ingest/bulk", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            queries: ["customer service", "graphics designer", "motion designer", "web designer", "data entry", "sales representative", "marketing", "accounting", "software engineer"],
            locations: ["Nigeria"],
            sources: ["workable", "myjobmag", "fuzu", "jobgurus", "jobberman"],
            target_count: 60,
            remote_only: false,
            date_posted: "month"
          })
        });
        const data = await resp.json().catch(() => ({}));
        console.log("bulk status:", resp.status, "run_id:", data.run_id);
        if (!resp.ok || !data.run_id) {
          console.error("scheduling failed:", resp.status, JSON.stringify(data));
          return;
        }
        // 2. Poll for the TRUE outcome (scheduling ≠ jobs flowed)
        const deadline = Date.now() + 4 * 60 * 1000;
        while (Date.now() < deadline) {
          await new Promise(r => setTimeout(r, 20000));
          const s = await fetch(`https://scuibjobsai.onrender.com/ingest/runs/${data.run_id}/status`);
          const st = await s.json().catch(() => ({}));
          console.log("run status:", JSON.stringify(st));
          if (st.state === "completed" || st.state === "failed") break;
        }
      } catch (e) {
        console.error("cron failed:", (e && e.message) || e);
      }
    })());
  }
};
```

**Why the body looks like this:**

- **Few queries + small target (60):** every query × source combo spawns fetchers, and HTML boards scrape detail pages one by one. A 200-target run can outlast free-tier idle timeouts and die mid-run. Small runs finish in minutes.
- **`locations: ["Nigeria"]`:** strictly Nigerian feed. (Global `"remote"` listings are overwhelmingly foreign; remote *Nigerian* jobs still arrive via the Nigerian boards with `remote: true`.)
- **`date_posted: "month"`:** widest window — fetch everything *with* dates recorded, and let the UI toggles do the filtering.

**How to verify a cron run actually delivered jobs** (in order of reliability):

1. The worker logs show `state: "completed"` with nonzero handoff counts (`fetched − errors − duplicates − skipped` ≈ handed off).
2. The newest `created_at` on Anthony's `GET /api/jobs/ingested/` moves to that hour.
3. Render's log stream shows `BulkIngestion[<run_id>]: completed — X/Y handed off, S skipped`.

The Cloudflare dashboard's own "success" checkmark only proves step 1's POST was accepted — it is the weakest signal. Trust the three checks above instead.

---

## Configuration (Environment Variables)

Copy `.env.example` to `.env` locally; set the same keys in the Render dashboard for production (Render redeploys when env changes).

| Variable | Required | Default | What it does |
|----------|----------|---------|--------------|
| `GEMINI_API_KEY` | Yes (or `GROQ_API_KEY`) | — | Google Gemini key — first parser in the chain. |
| `GROQ_API_KEY` | No | — | Groq key — second parser when Gemini fails or is rate-limited. Get one at `console.groq.com`. Without either LLM key, the regex parser runs alone. |
| `GROQ_MODEL` | No | `llama-3.3-70b-versatile` | Groq model name. |
| `GEMINI_MODEL` / `GEMINI_FALLBACK_MODEL` | No | `gemini-2.0-flash` / `gemini-2.5-pro` | Primary and fallback Gemini models. |
| `HANDOFF_ENDPOINT_URL` | Yes (prod) | — | Where parsed jobs are POSTed. Production: `https://backend-api-4p3k.onrender.com/api/jobs/ingest/`. **If unset, jobs are written to a local file and never reach Anthony's database** — the #1 cause of "cron succeeds but DB is empty". |
| `HANDOFF_API_KEY` | No | — | Bearer token sent with handoff POSTs, if the downstream requires auth. |
| `SUPABASE_URL` / `SUPABASE_KEY` | Strongly recommended (prod) | — | Persistent staging store. Without these, everything lives in memory and **wipes on every sleep/restart** (stats, dedup history, everything). Tables must be created first — DDL is in `store/stores.py`. |
| `INGEST_QUERIES` / `INGEST_LOCATIONS` | No | role list / `Nigeria` | Server-side defaults used by `POST /ingest/trigger`. The bulk endpoint takes these per-request instead. |
| `INGEST_SOURCES` | No | all 8 boards | Server-side default source list for `/ingest/trigger`. |
| `TARGET_JOB_COUNT` | No | `500` | Server-side default target for `/ingest/trigger`. |
| `JSEARCH_API_KEY` / `ADZUNA_APP_ID` / `ADZUNA_APP_KEY` | No | — | Enable the JSearch/Adzuna API sources. Without keys those sources are skipped with a warning. |
| `REQUIRE_APPLICATION_LINK` | No | `true` | `true` = hold back jobs with no apply URL (recommended — brand safety). Set `false` to hand off everything. |
| `MAX_JOB_AGE_DAYS` | No | `0` (off) | `0` = hand off all dated jobs. Set `N` to hold back jobs posted more than N days ago. |
| `MAX_CONCURRENT_PARSES` | No | `15` | Parallel AI parses. Lower it if you hit LLM rate limits. |

---

## Frontend Integration Guide

### Typical admin-panel flow

1. **Start ingestion** → `POST /ingest/bulk` with queries, Nigeria locations, and sources. Save the `run_id`.
2. **Show progress** → Poll `GET /ingest/runs/{run_id}/status` every 20–60 seconds. Render a progress bar from `fetched / target`, plus counters for `parsed`, `duplicates`, `skipped`, `errors`, and a per-board breakdown from `per_source`. Stop at `state: "completed"`.
3. **Dashboard cards** → `GET /jobs/stats` for totals, status split, per-board split, and average AI confidence.
4. **Jobs table** → `GET /jobs?page=N&page_size=50&status=…` with status tabs (`all` / `parsed` / `sent` / `failed`). Paginate with `total_pages`.
5. **Job detail** → `GET /jobs/{id}` when the user clicks a row. Show `confidence`, `model_used`, `parse_warnings`, and `validation_issues` as quality badges.
6. **Retry failures** → Collect failed IDs from the table and `POST /jobs/retry`.
7. **Performance view** → `GET /metrics` for lifetime totals and per-run history (duration, jobs/sec, per-board counts).

### Practical tips

- **Bulk runs are async.** `POST /ingest/bulk` returns in under a second with a `run_id`. Never treat that response as "jobs delivered" — poll the run status.
- **Parsing speed:** roughly 2–10s per job on Gemini/Groq, ~0.1s on the regex fallback. A 60-target run is minutes, not seconds — mostly HTML fetching.
- **Confidence is your quality signal.** Expect 0.8–0.95 from LLMs, 0.5–0.7 from regex. Below 0.3 the pipeline automatically tries the next parser.
- **Validation issues are advisory.** They annotate; they don't block. Use them for warning icons, not for hiding jobs.
- **A nonzero `duplicates` count is healthy** — it means dedup is catching re-listings.
- **`skipped` jobs are visible** under `GET /jobs?status=parsed` — that's where held-back (link-less / stale) jobs sit for inspection.
- **All timestamps are ISO 8601 UTC.** `posted_date` is a plain `YYYY-MM-DD` string (or null when the board gave nothing usable).
- **Cold starts are normal on free hosting.** The first request after idle can take 30–90s while the server wakes and health-checks boards. Allow generous timeouts in the cron worker and don't retry aggressively.

### Error handling

| HTTP code | Meaning | What to do |
|-----------|---------|------------|
| `200` | Success (for bulk: *scheduled*, not *finished*). | Poll the run status. |
| `404` | Job ID or run ID not found (or expired from memory after a restart). | Check the ID; re-list to get fresh ones. |
| `422` | Request body failed validation (e.g. `target_count` out of 1–2000). | Fix the request fields. |
| `500` | Server error. | Check server logs; retry. |

---

## Troubleshooting

**"The cron succeeds but Anthony's database gets nothing."** Work through this list in order — one of them is almost always the cause:

1. **`HANDOFF_ENDPOINT_URL` unset on Render?** The server log prints `HANDOFF_ENDPOINT_URL not set — using FileHandoff` at startup. With file handoff, jobs go to `handoff_output.jsonl` on ephemeral disk and vanish on restart. Set it to `https://backend-api-4p3k.onrender.com/api/jobs/ingest/`.
2. **Did the run actually finish?** Check `GET /ingest/runs/{run_id}/status`. `fetched: 0` with errors means every board failed (see 4). A run stuck in `running` forever likely died with a sleep — the free tier kills long background runs.
3. **Did anything get held by safety gates?** High `skipped` with jobs sitting in `GET /jobs?status=parsed` = link-less or stale jobs. Check `validation_issues` on a few to see which gate held them.
4. **Can Render reach the boards?** Nigerian boards in particular can time out from US/EU networks. `per_source` missing a board across runs + `ERROR` lines in Render logs = connectivity, not a code bug.
5. **LLM keys exhausted?** Gemini 429s with no `GROQ_API_KEY` set means everything falls back to regex (lower quality, but jobs still flow). Check logs for `429` / `ResourceExhausted`.
6. **Stats read zero but runs happened?** In-memory store + metrics wipe on every sleep. Zeros prove nothing by themselves — check Supabase config and the run-status endpoint instead.
7. **Wrong cron target?** The worker must `POST` (not GET) to `/ingest/bulk` with a JSON body. Hitting `/docs` or `/health` on a schedule only keeps the instance warm — it fetches zero jobs.

**"Users see jobs they can't apply to."** Check two places: our handoff payloads always include `application_link` when the gate is on — verify Anthony's backend has the `application_link` column and returns it. If the column doesn't exist yet, links are sent but dropped on arrival.

**"Old jobs keep reappearing."** Confirm Supabase credentials + tables on Render (cross-run dedup), and ask Anthony for a unique constraint / upsert on `source_job_id` (backstop dedup on stable IDs).

---

## Data Sources — Ingester Reference

| Source | Value for `sources` | Type | What it hits | Auth | Date support |
|--------|-------------------|------|--------------|------|--------------|
| Workable | `workable` | JSON API | `jobs.workable.com/api/v1/jobs` | None | Real `created` timestamp per job |
| MyJobMag | `myjobmag` | HTML scrape | `www.myjobmag.com/search/jobs?q=` | None | Best-effort text extraction |
| Fuzu | `fuzu` | HTML scrape | `www.fuzu.com/{location}/search?q=` | None | Best-effort text extraction |
| JobGurus | `jobgurus` | HTML scrape | `www.jobgurus.com.ng/jobs` | None | Best-effort text extraction |
| Jobberman | `jobberman` | HTML scrape | `www.jobberman.com/jobs?q=` | None | Best-effort text extraction |
| JSearch | `jsearch_api` | REST API | `jsearch.p.rapidapi.com` | RapidAPI key | Server-side `date_posted` filter + per-job timestamp |
| Indeed RSS | `indeed_rss` | RSS feed | `www.indeed.com/rss` (sorted by date) | None | Real `pubDate` per item |
| Adzuna | `adzuna_api` | REST API | `api.adzuna.com` (free: 250 calls/day) | App ID + Key | Server-side `max_days_old` + per-job `created` |

The four Nigerian boards (`myjobmag`, `fuzu`, `jobgurus`, `jobberman`) are inherently Nigerian — including their remote listings — and largely ignore the `locations` parameter. Global sources (`workable`, `jsearch_api`, `indeed_rss`, `adzuna_api`) *do* honor it, so keep `locations: ["Nigeria"]` to keep foreign jobs out.
