# AGENTS.md — ScuibJobsAi

## Run

```bash
pip install -r requirements.txt
# .env required with at least GEMINI_API_KEY (copy from .env.example)
uvicorn main:app --reload
```

`main.py` must load `dotenv` **before** importing `api.app` — keep this import ordering (`main.py:6-10`). `/health` at root.

## Test

No test framework configured. Two standalone test scripts:

```bash
python scripts/phase1_test.py     # single job: manual paste → LLM parse → handoff
python scripts/bulk_test.py       # hermetic: dedup, circuit breaker, full pipeline + optional real Gemini
```

`bulk_test.py` test 4 (real Gemini) is skipped when `GEMINI_API_KEY` is unset — not a failure.

## Architecture

```
ingestion/ → parsing/ → validation/ → store/ → handoff/
                  ↕ (human-in-the-loop approval via API)
```

- `core/interfaces.py`: ABC contracts for each stage (`BaseIngester`, `BaseParser`, `BaseValidator`, `BaseHandoff`, `BaseStore`).
- `core/pipeline.py`: `JobPipeline` wires stages via DI. Created in `api/dependencies.py`.
- `api/dependencies.py`: DI container. Auto-falls back: `SupabaseStore` → `InMemoryStore`, `HTTPHandoff` → `FileHandoff`.
- `core/models.py`: Data flow `RawJob → ParsedJob → ValidatedJob → HandoffPayload`.
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

`MultiSourceAggregator` (`ingestion/aggregator.py`) runs all sources concurrently, deduplicates by external_id + content fingerprint, stops at `target_count`.

## Notes

- No linter, formatter, type checker, or CI configured.
- No pytest — test scripts use raw `assert`.
- `handoff_output.jsonl` written when `HANDOFF_ENDPOINT_URL` is unset.
- Supabase tables must be created manually (DDL in `store/stores.py:105-141`).
- `AUTO_APPROVE_CONFIDENCE_THRESHOLD` env var allows bypassing human review for high-confidence jobs.
