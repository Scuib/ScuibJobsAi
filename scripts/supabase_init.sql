-- ─────────────────────────────────────────────────────────────────────────────
-- Supabase schema for ScuibJobsAi — run this ONCE in the Supabase dashboard:
--   Supabase project → SQL Editor → New query → paste this file → Run.
-- Idempotent: safe to re-run. Nothing else in this repo creates the tables.
--
-- Without these tables runs still "succeed" (store errors are swallowed) but:
--   GET /jobs and /jobs/stats are always empty, cross-run dedup is dead, and
--   GET /metrics.store_errors climbs. Verify afterwards: /jobs/stats should
--   show counts after the next cron run and store_errors should stop growing.
--
-- Key note: use the service_role key in the backend (server-side only, never
-- in a browser). It bypasses RLS, so no row policies are required below.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS raw_jobs (
    id           UUID PRIMARY KEY,
    source       TEXT NOT NULL,
    external_id  TEXT,
    raw_text     TEXT NOT NULL,
    source_url   TEXT,
    fetched_at   TIMESTAMPTZ DEFAULT NOW(),
    metadata     JSONB DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS parsed_jobs (
    id                UUID PRIMARY KEY,
    raw_id            UUID REFERENCES raw_jobs(id) ON DELETE CASCADE,
    status            TEXT NOT NULL DEFAULT 'parsed',
    source            TEXT,
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
    source_url        TEXT,
    application_link  TEXT,
    -- TEXT (not DATE): batch inserts are atomic in PostgREST, so one malformed
    -- date string would fail an entire batch. ISO 'YYYY-MM-DD' sorts/filters
    -- correctly as text; the age gate parses it defensively anyway.
    posted_date       TEXT,
    model_used        TEXT,
    confidence        DOUBLE PRECISION DEFAULT 1.0,
    parse_warnings    TEXT[] DEFAULT '{}',
    validation_issues TEXT[] DEFAULT '{}',
    parsed_at         TIMESTAMPTZ DEFAULT NOW()
);

-- Read paths: status filter + recency ordering (GET /jobs), dedup probe
CREATE INDEX IF NOT EXISTS parsed_jobs_status_idx   ON parsed_jobs(status);
CREATE INDEX IF NOT EXISTS parsed_jobs_parsed_at_idx ON parsed_jobs(parsed_at DESC);
CREATE INDEX IF NOT EXISTS raw_jobs_external_id_idx ON raw_jobs(external_id);

-- Migration no-ops for tables created by older versions of this DDL:
ALTER TABLE parsed_jobs ADD COLUMN IF NOT EXISTS source           TEXT;
ALTER TABLE parsed_jobs ADD COLUMN IF NOT EXISTS source_url       TEXT;
ALTER TABLE parsed_jobs ADD COLUMN IF NOT EXISTS application_link TEXT;
ALTER TABLE parsed_jobs ADD COLUMN IF NOT EXISTS posted_date      TEXT;
