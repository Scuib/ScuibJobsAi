# Automated Job Data Pipeline (ScuibJobsAi Backend)

High-throughput backend for ingesting, parsing, validating, and handing off job listings to downstream matching algorithms. Supports multiple sources, Gemini LLM extraction, human-in-the-loop review, and enterprise resilience primitives.

---

## Architecture

```
ingestion/ → parsing/ → validation/ → store/ → handoff/
                  ↕ (human-in-the-loop approval via API)
```

### Pipeline stages

| Stage | Component | Responsibility |
|-------|-----------|----------------|
| Ingestion | `BaseIngester` | Pull raw jobs from JSearch API, Indeed RSS, Adzuna API, or manual paste |
| Parsing | `BaseParser` (`GeminiParser`) | Extract structured JSON via Gemini LLM with chunked concurrency |
| Validation | `BaseValidator` (`SchemaValidator`) | Rule checks on LLM output (required fields, salary sanity, parse failure) |
| Store | `BaseStore` | Persistence — auto-chooses `SupabaseStore` or falls back to `InMemoryStore` |
| Handoff | `BaseHandoff` | Delivery to downstream — auto-chooses `HTTPHandoff` or falls back to `FileHandoff` |

Interfaces defined in [`core/interfaces.py`](core/interfaces.py). Wiring via DI in [`api/dependencies.py`](api/dependencies.py). Orchestration in [`core/pipeline.py`](core/pipeline.py).

### Resilience primitives ([`core/resilience.py`](core/resilience.py), [`core/metrics.py`](core/metrics.py))
- **`CircuitBreaker`** — per-source: skip after N consecutive failures, probe on cooldown expiry
- **`AdaptiveRateLimiter`** — token bucket that halves on 429s, gradually recovers
- **`retry_with_backoff`** — exponential backoff + jitter for page-level API calls
- **`MetricsCollector`** — singleton tracking p50/p95/p99 parse latencies, per-source counts, active runs

---

## Tech Stack
- **Python 3.11+**
- **FastAPI** & **Uvicorn**
- **Pydantic v2**
- **google-generativeai** (Gemini 1.5 Flash/Pro)
- **HTTPX** (async HTTP)
- **Supabase** (optional Postgres persistence)

---

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env   # then fill in credentials
```

### Required env vars

| Variable | Purpose |
|----------|---------|
| `GEMINI_API_KEY` | Google AI Studio API key — mandatory |

### Optional but commonly used

| Variable | Default | Purpose |
|----------|---------|---------|
| `SUPABASE_URL`, `SUPABASE_KEY` | — | When set, uses `SupabaseStore`; otherwise `InMemoryStore` |
| `HANDOFF_ENDPOINT_URL` | — | When set, posts to downstream via `HTTPHandoff`; otherwise writes `handoff_output.jsonl` |
| `HANDOFF_API_KEY` | — | Bearer token for the handoff endpoint |
| `HANDOFF_FILE_PATH` | `handoff_output.jsonl` | Output file when endpoint is unset |
| `JSEARCH_API_KEY` | — | RapidAPI key for JSearch |
| `JSEARCH_PAGES` | `10` | Pages per query (10 results/page) |
| `ADZUNA_APP_ID`, `ADZUNA_APP_KEY` | — | Adzuna API credentials (free tier: 250 calls/day) |
| `ADZUNA_PAGES` | `5` | Pages per query (50 results/page) |
| `INGEST_QUERIES` | `software engineer,backend developer,python developer` | Comma-separated query list |
| `INGEST_LOCATIONS` | `remote,United States` | Comma-separated location list |
| `TARGET_JOB_COUNT` | `200` | Stop after this many unique jobs |
| `DATE_POSTED_FILTER` | `week` | `today`, `3days`, `week`, or `month` |
| `GEMINI_MODEL` | `gemini-1.5-flash` | Falls back to `gemini-1.5-pro` after 3 retries |
| `AUTO_APPROVE_CONFIDENCE_THRESHOLD` | — | Auto-approve jobs above this confidence (e.g. `0.9`) |
| `MAX_CONCURRENT_PARSES` | `15` | Semaphore bound for Gemini calls |
| `CIRCUIT_BREAKER_THRESHOLD` | `3` | Failures before circuit opens |
| `CIRCUIT_BREAKER_COOLDOWN` | `60` | Seconds before half-open probe |
| `RATE_LIMIT_REQUESTS_PER_MINUTE` | `60` | Max LLM API requests/min |

---

## Database Schema (Supabase)

DDL in [`store/stores.py:105-141`](store/stores.py). Required when `SUPABASE_URL` and `SUPABASE_KEY` are set:

```sql
CREATE TABLE raw_jobs (
    id           UUID PRIMARY KEY,
    source       TEXT NOT NULL,
    external_id  TEXT,
    raw_text     TEXT NOT NULL,
    source_url   TEXT,
    fetched_at   TIMESTAMPTZ DEFAULT NOW(),
    metadata     JSONB DEFAULT '{}'
);

CREATE TABLE parsed_jobs (
    id                UUID PRIMARY KEY,
    raw_id            UUID REFERENCES raw_jobs(id),
    status            TEXT NOT NULL DEFAULT 'parsed',
    job_title         TEXT NOT NULL,
    company           TEXT,
    location          TEXT,
    remote            BOOLEAN DEFAULT FALSE,
    salary            JSONB,
    required_skills   TEXT[] DEFAULT '{}',
    preferred_skills  TEXT[] DEFAULT '{}',
    years_experience  INTEGER,
    education_level   TEXT,
    employment_type   TEXT,
    description_clean TEXT,
    model_used        TEXT,
    confidence        FLOAT DEFAULT 1.0,
    parse_warnings    TEXT[] DEFAULT '{}',
    validation_issues TEXT[] DEFAULT '{}',
    reviewer_notes    TEXT DEFAULT '',
    reviewed_at       TIMESTAMPTZ,
    reviewed_by       TEXT,
    parsed_at         TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX parsed_jobs_status_idx ON parsed_jobs(status);
```

---

## Running

```bash
# Dev server
uvicorn main:app --reload        # → http://127.0.0.1:8000/docs

# Phase 1 manual test (single job → LLM parse → handoff)
PYTHONPATH=. python scripts/phase1_test.py

# Bulk pipeline test (hermetic dedup, circuit breaker, full flow)
PYTHONPATH=. python scripts/bulk_test.py
```

### Key API endpoints

| Endpoint | Purpose |
|----------|---------|
| `POST /ingest/manual` | Paste raw text for LLM parse + stage |
| `POST /ingest/bulk` | Multi-source background ingestion (returns `run_id`) |
| `GET /ingest/runs/{run_id}/status` | Poll progress of a bulk run |
| `GET /jobs/pending` | Jobs awaiting human review |
| `GET /jobs/stats` | Aggregate dashboard stats |
| `POST /jobs/{id}/approve` | Approve and handoff a single job |
| `POST /jobs/{id}/reject` | Reject a single job |
| `POST /jobs/bulk-action` | Approve/reject filtered by ID, confidence, or flagged status |
| `GET /metrics` | In-memory metrics snapshot |
| `GET /health` | Liveness check |

---

## Rollout

All three phases are complete:

| Phase | What | Status |
|-------|------|--------|
| 1 | Manual paste → LLM parse → mock handoff (POC validation) | Done |
| 2 | Supabase persistence, Indeed RSS + Adzuna, HTTP handoff, human review UI endpoints | Done |
| 3 | Multi-source concurrent ingestion, chunked parallel LLM parsing, circuit breakers, rate limiters, metrics, bulk API | Done |
