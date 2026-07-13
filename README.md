# ScuibJobsAi — Automated Job Data Pipeline

Fully automatic backend that ingests job listings from multiple sources, parses them with AI (Google Gemini), validates the extracted data, and hands off every job to the downstream matching algorithm — **no human approval needed**.

---

## How It Works

```
Source A ─┐
Source B ─┤                                                    
Source C ─┼─→  Fetch  →  Deduplicate  →  AI Parse  →  Validate  →  Handoff
Source D ─┤                                                    
Source E ─┘                                                    
```

Every job flows through this pipeline automatically:

| Step | What Happens | Component |
|------|-------------|-----------|
| **Fetch** | Pulls raw job postings from 8+ sources (APIs, RSS, scrapers) | `ingestion/` |
| **Deduplicate** | Skips jobs already seen (by external ID) | `store/` |
| **AI Parse** | Gemini LLM extracts structured fields (title, company, skills, salary, etc.) — falls back to regex if Gemini is unavailable | `parsing/` |
| **Validate** | Automated checks: missing fields, salary sanity, confidence thresholds | `validation/` |
| **Handoff** | POSTs structured data to the matching algorithm (or writes to file in dev) | `handoff/` |

### Job Lifecycle

```
RAW → PARSED → SENT
                 ↓ (if handoff fails)
              FAILED → (retry) → SENT
```

Jobs have 4 possible statuses:
- **`raw`** — Just fetched, not parsed yet
- **`parsed`** — AI extraction complete, about to be handed off
- **`sent`** — Successfully delivered to the matching algorithm
- **`failed`** — Handoff failed (can be retried via `POST /jobs/retry`)

---

## Tech Stack

- **Python 3.11+**
- **FastAPI** & **Uvicorn** — async web framework
- **Pydantic v2** — data validation and serialization
- **Google Gemini** (2.0 Flash / 2.5 Pro) — AI parsing (optional, falls back to regex)
- **HTTPX** — async HTTP client
- **BeautifulSoup4** — HTML scraping for African job boards
- **Supabase** — Postgres persistence (optional, falls back to in-memory)

---

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Copy env template and fill in your credentials
cp .env.example .env

# 3. Start the dev server
uvicorn main:app --reload
```

The server starts at `http://127.0.0.1:8000`. Open `http://127.0.0.1:8000/docs` for the interactive Swagger UI.

### Without any API keys

The pipeline works without Gemini. If `GEMINI_API_KEY` is unset, `StructuredParser` extracts fields via regex. Confidence is lower (0.5–0.7 vs 0.9+ with LLM), but the pipeline keeps running.

---

## Environment Variables

### Required

| Variable | Purpose |
|----------|---------|
| `GEMINI_API_KEY` | Google AI Studio API key — **optional** but strongly recommended; without it, only regex parsing is used |

### Commonly Used

| Variable | Default | Purpose |
|----------|---------|---------|
| `SUPABASE_URL` | — | When set (with `SUPABASE_KEY`), uses Supabase for persistence |
| `SUPABASE_KEY` | — | Supabase service role key |
| `HANDOFF_ENDPOINT_URL` | — | When set, POSTs jobs to this URL; otherwise writes to `handoff_output.jsonl` |
| `HANDOFF_API_KEY` | — | Bearer token for the handoff endpoint |
| `HANDOFF_FILE_PATH` | `handoff_output.jsonl` | Output file when endpoint is unset |
| `JSEARCH_API_KEY` | — | RapidAPI key for JSearch (covers Indeed, LinkedIn, Glassdoor) |
| `JSEARCH_PAGES` | `10` | Pages per query (10 results/page) |
| `ADZUNA_APP_ID` | — | Adzuna API app ID |
| `ADZUNA_APP_KEY` | — | Adzuna API key (free tier: 250 calls/day) |
| `ADZUNA_PAGES` | `5` | Pages per query (50 results/page) |

### Ingestion Settings

| Variable | Default | Purpose |
|----------|---------|---------|
| `INGEST_QUERIES` | `software engineer,backend developer,python developer` | Comma-separated search queries |
| `INGEST_LOCATIONS` | `remote,United States,Nigeria` | Comma-separated locations |
| `INGEST_SOURCES` | `workable,myjobmag,fuzu,jobgurus,jobberman` | Comma-separated source list |
| `TARGET_JOB_COUNT` | `200` | Stop after this many unique jobs |

### Advanced

| Variable | Default | Purpose |
|----------|---------|---------|
| `GEMINI_MODEL` | `gemini-2.0-flash` | Primary LLM model |
| `GEMINI_FALLBACK_MODEL` | `gemini-2.5-pro` | Fallback after retries fail |
| `MAX_CONCURRENT_PARSES` | `15` | Semaphore bound for parallel Gemini calls |
| `CIRCUIT_BREAKER_THRESHOLD` | `3` | Failures before circuit opens for a source |
| `CIRCUIT_BREAKER_COOLDOWN` | `60` | Seconds before half-open probe |
| `RATE_LIMIT_REQUESTS_PER_MINUTE` | `60` | Max LLM API requests/min |

---

## Supported Job Sources

| Source | Type | Auth | Notes |
|--------|------|------|-------|
| **Workable** | JSON API | None | `jobs.workable.com/api/v1/jobs` — clean, rich API |
| **MyJobMag** | HTML scrape | None | Nigerian job board |
| **Fuzu** | HTML scrape | None | African job board |
| **JobGurus** | HTML scrape | None | Nigerian job board |
| **Jobberman** | HTML scrape | None | Nigerian job board |
| **JSearch API** | REST API | RapidAPI key | Covers Indeed, LinkedIn, Glassdoor |
| **Indeed RSS** | RSS feed | None | Public RSS, up to 125 jobs/query |
| **Adzuna API** | REST API | App ID + Key | Free tier: 250 calls/day |

---

## API Endpoints

Full details with request/response examples: see [API_DOCS.md](API_DOCS.md)

| Method | Endpoint | Purpose |
|--------|----------|---------|
| `POST` | `/ingest/manual` | Submit a single raw job text for processing |
| `POST` | `/ingest/trigger` | Start a simple single-source ingestion run |
| `POST` | `/ingest/bulk` | Start a multi-source bulk ingestion (returns `run_id`) |
| `GET` | `/ingest/runs/{run_id}/status` | Poll progress of a bulk run |
| `GET` | `/jobs` | List processed jobs (with optional status filter) |
| `GET` | `/jobs/stats` | Aggregate dashboard statistics |
| `GET` | `/jobs/{job_id}` | Get a single job by ID |
| `POST` | `/jobs/retry` | Retry handoff for failed jobs |
| `GET` | `/metrics` | Pipeline performance metrics |
| `GET` | `/health` | Liveness check |

---

## Database Schema (Supabase)

Only needed when `SUPABASE_URL` and `SUPABASE_KEY` are set. Run this SQL in your Supabase project:

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
    parsed_at         TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX parsed_jobs_status_idx ON parsed_jobs(status);
```

---

## Architecture

```
ScuibJobsAi/
├── main.py                  # Entry point (uvicorn main:app --reload)
├── api/
│   ├── app.py               # FastAPI routes and Swagger configuration
│   └── dependencies.py      # Dependency injection — swap implementations here
├── core/
│   ├── interfaces.py        # Abstract base classes for all pipeline stages
│   ├── models.py            # Pydantic data models (RawJob, ParsedJob, etc.)
│   ├── pipeline.py          # Pipeline orchestrator (ingest → parse → validate → handoff)
│   ├── metrics.py           # In-memory metrics collector
│   └── resilience.py        # Circuit breaker, rate limiter, retry utilities
├── ingestion/
│   ├── ingesters.py         # IndeedRSS, JSearch, Manual ingesters
│   ├── adzuna_ingester.py   # Adzuna API ingester
│   ├── custom_ingesters.py  # Workable, MyJobMag, Fuzu, JobGurus, Jobberman
│   └── aggregator.py        # Multi-source aggregator
├── parsing/                 # Gemini parser, structured parser, hybrid parser
├── validation/
│   └── validators.py        # Automated validation rules
├── handoff/
│   └── handlers.py          # HTTP, File, and Mock handoff implementations
└── store/
    └── stores.py            # InMemory and Supabase persistence
```

### Adding a new job source

1. Add the source to `JobSource` enum in `core/models.py`
2. Create a new ingester class extending `BaseIngester` in `ingestion/custom_ingesters.py`
3. Register it in `build_dynamic_aggregator()` in `api/dependencies.py`
4. Add any new env vars to `.env.example`

### Parser fallback chain

1. `HybridParser` tries `GeminiParser` (LLM) first
2. If Gemini fails → falls back to `StructuredParser` (regex)
3. For Workable sources, `StructuredParser` uses API metadata fields directly (more accurate)

---

## Known Limitations

- **Gemini API** needs billing enabled for production volume (free tier quota exhausts quickly)
- **Wellfound** (angel.co) is behind Cloudflare — cannot scrape with plain HTTP
- **Nigerian sites** (JobGurus, Jobberman, Fuzu) — HTML scraping may need browser-like headers or proxies
- **Workable API** rate limits unknown — conservative 30 RPM configured
