# AGENTS.md — ScuibJobsAi

## Run

```bash
pip install -r requirements.txt
# .env required with at least GEMINI_API_KEY (copy from .env.example)
uvicorn main:app --reload
```

`main.py` must load `dotenv` **before** importing `api.app` — keep this import ordering (`main.py:6-10`). `/health` at root.

## Test

No test framework configured. Standalone test scripts (raw `assert`):

```bash
python scripts/smoke_test.py  # offline CI gate: models, parsers, aggregator, gates, HTTP API (no network/keys/DB)
python scripts/phase1_test.py # single job: manual paste → LLM parse → handoff
python scripts/bulk_test.py   # hermetic: dedup, circuit breaker, full pipeline + optional real Gemini
```

`bulk_test.py` test 4 (real Gemini) is skipped when `GEMINI_API_KEY` is unset — not a failure. GitHub Actions (`.github/workflows/ci.yml`) runs compileall + `smoke_test.py` on push/PR. `render.yaml` is a Blueprint for NEW services only — never apply to the existing dashboard service (would duplicate it).

## Architecture

```
ingestion/ → parsing/ → validation/ → store/ → handoff/
                  ↕ (human-in-the-loop approval via API)
```

- `core/interfaces.py`: ABC contracts for each stage (`BaseIngester`, `BaseParser`, `BaseValidator`, `BaseHandoff`, `BaseStore`).
- `core/pipeline.py`: `JobPipeline` wires stages via DI. Created in `api/dependencies.py`.
- `api/dependencies.py`: DI container. Auto-falls back: `SupabaseStore` → `InMemoryStore`, `HTTPHandoff` → `FileHandoff`.
- `core/models.py`: Data flow `RawJob → ParsedJob → HandoffPayload`. `ParsedJob.id` is a stable uuid5 of `source:external_id` (when external_id exists) so hourly cron reruns map to the same ID — downstream can dedup on `job_id`. `source_url`/`application_link` flow `RawJob → ParsedJob → HandoffPayload.application_link`.
- `REQUIRE_APPLICATION_LINK=true` (default): jobs with no apply URL are parsed+stored but skipped at handoff (counted as `skipped`, stay `PARSED`). `[PARSE FAILED]` jobs are never handed off. Link-less jobs stay out of Anthony's DB (brand safety).
- `MAX_JOB_AGE_DAYS=1` (default = today only): dated jobs must be **strictly younger** than N days at handoff (`1` = today only, `2` = today + yesterday) — counted in `skipped_stale` (total in `skipped`; dateless jobs always kept). Set 0 for no limit. `posted_date` (YYYY-MM-DD) flows `ingester metadata → ParsedJob → HandoffPayload.posted_date` so users filter 24h/1wk/1mo in UI. API sources normalize dates at ingest (`normalize_posted_date`), HTML boards regex-extract them. `date_posted` request param (`today|3days|week|month`) is enforced server-side by JSearch/Adzuna/DelonJobs — use `today` in cron for same-day-only runs. Gates can be pinned per run via `require_application_link` / `max_age_days` request fields on `POST /ingest/bulk` (env-independent).
- `GET /jobs` is paginated (`page`, `page_size` ≤ 500) and returns `PagedJobs{jobs, page, page_size, total, total_pages}`; store layer pages via `get_all_jobs(limit, offset, status)` + `get_jobs_count(status)`.
- Cross-run dedup needs `SupabaseStore` (persistent `raw_jobs.external_id`); `InMemoryStore` wipes on Render sleep/restart. Downstream should also dedup on stable `job_id`.
- `core/resilience.py`: `CircuitBreaker`, `AdaptiveRateLimiter`, `retry_with_backoff` — used per-ingester, not centrally.
- `core/metrics.py`: `MetricsCollector` singleton consumed by `GET /metrics`.

## Inference

- `GeminiParser` in `parsing/gemini_parser.py` uses `run_in_executor` to call the synchronous Gemini SDK.
- Truncates raw text to 8k chars (`gemini_parser.py:196`).
- Falls back to gemini-1.5-pro after 3 retries on flash.
- `batch_parse` processes in chunks (default 10), bounded by `max_concurrent` semaphore.

## Data sources

| Source | Env vars needed | Notes |
|--------|----------------|-------|
| JSearch (RapidAPI) | `JSEARCH_API_KEY` | Deep pagination, circuit breaker, rate limiter |
| Indeed RSS | None (public) | Multi-query, offset pagination, up to 125 jobs/query |
| Adzuna API | `ADZUNA_APP_ID`, `ADZUNA_APP_KEY` | Free tier: 250 calls/day, 50/page |
| Manual | None | Paste raw text via API |

`MultiSourceAggregator` (`ingestion/aggregator.py`) runs all sources concurrently, deduplicates by external_id + content fingerprint, stops at `target_count`. Per-source balance caps (`max_per_source` = target ÷ distinct sources, set in `build_dynamic_aggregator`): capped-out boards buffer overflow and drain FIFO after all sources finish, so fast boards can't starve slow ones. Fetch phase ends when all sources finish OR `FETCH_TIMEOUT_SECONDS` (default 600) elapses — hanging boards are abandoned and the run proceeds with what arrived.

## Notes

- No linter, formatter, type checker, or CI configured.
- No pytest — test scripts use raw `assert`.
- `handoff_output.jsonl` written when `HANDOFF_ENDPOINT_URL` is unset.
- Supabase tables must be created manually: run `scripts/supabase_init.sql` once in the Supabase SQL Editor (idempotent; source of truth for schema, incl. `ALTER TABLE ... IF NOT EXISTS` migration no-ops). If they are missing, runs still succeed (store errors are swallowed) but `GET /jobs` is always empty, cross-run dedup is dead, and `GET /metrics.store_errors` climbs (`store_error_detail` has the root-cause message) — check it when the store looks empty.
- `AUTO_APPROVE_CONFIDENCE_THRESHOLD` env var allows bypassing human review for high-confidence jobs.
- Cloudflare cron worker source is versioned at `scripts/cloudflare_cron_worker.js` (classic `addEventListener` format — module format is rejected by the script-upload API). It pins `require_application_link: true` + `max_age_days: 1` in the request body so gates hold regardless of Render env. Schedule `0 */6 * * *` is set via `PUT /accounts/{id}/workers/scripts/scuibjobs-cron/schedules` (bare JSON array body — `metadata.crons` on upload does not stick).
