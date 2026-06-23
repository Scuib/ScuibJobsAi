"""
api/dependencies.py

Dependency injection container.
Change implementations here — nothing else needs to change.
Phase 1: InMemoryStore + MockHandoff + ManualIngester
Phase 2: SupabaseStore + HTTPHandoff + IndeedRSSIngester
"""

import os
from functools import lru_cache

from core.pipeline import JobPipeline
from core.interfaces import BaseStore

from ingestion.ingesters import IndeedRSSIngester, ManualIngester, JSearchIngester
from ingestion.aggregator import MultiSourceAggregator
from ingestion.adzuna_ingester import AdzunaIngester
from core.models import JobSource
from parsing.gemini_parser import GeminiParser
from validation.validators import SchemaValidator
from handoff.handlers import HTTPHandoff, MockHandoff, FileHandoff
from store.stores import InMemoryStore, SupabaseStore


def _build_store() -> BaseStore:
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")

    if supabase_url and supabase_key:
        return SupabaseStore(url=supabase_url, key=supabase_key)

    print("SUPABASE_URL/KEY not set — using InMemoryStore (Phase 1 mode)")
    return InMemoryStore()


def _build_handoff():
    endpoint = os.getenv("HANDOFF_ENDPOINT_URL")
    handoff_key = os.getenv("HANDOFF_API_KEY")

    if endpoint:
        return HTTPHandoff(endpoint_url=endpoint, api_key=handoff_key)

    output_file = os.getenv("HANDOFF_FILE_PATH", "handoff_output.jsonl")
    print(f"HANDOFF_ENDPOINT_URL not set — using FileHandoff -> {output_file}")
    return FileHandoff(output_path=output_file)


def build_dynamic_aggregator(
    queries: list[str],
    locations: list[str],
    sources: list[JobSource],
    target_count: int = 200,
    remote_only: bool = False,
    date_posted: str = "week",
) -> MultiSourceAggregator:
    """
    Dynamically constructs a MultiSourceAggregator with one or more concrete
    ingesters configured per query/location combination based on user specifications.
    """
    ingesters = []

    # 1. JSearch API
    if JobSource.JSEARCH_API in sources:
        jsearch_key = os.getenv("JSEARCH_API_KEY")
        if jsearch_key:
            pages = int(os.getenv("JSEARCH_PAGES", "10"))
            for query in queries:
                for location in locations:
                    ingesters.append(JSearchIngester(
                        api_key=jsearch_key,
                        query=query,
                        location=location,
                        remote_only=remote_only,
                        pages=pages,
                        date_posted=date_posted,
                    ))
        else:
            print("WARNING: JSearch source requested but JSEARCH_API_KEY not configured. Skipping.")

    # 2. Indeed RSS
    if JobSource.INDEED_RSS in sources:
        max_pages = int(os.getenv("INDEED_PAGES", "5"))
        for location in locations:
            ingesters.append(IndeedRSSIngester(
                query="",  # Handled by queries list
                queries=queries,
                location=location,
                max_pages=max_pages,
            ))

    # 3. Adzuna API
    if JobSource.ADZUNA_API in sources:
        adzuna_id = os.getenv("ADZUNA_APP_ID")
        adzuna_key = os.getenv("ADZUNA_APP_KEY")
        if adzuna_id and adzuna_key:
            pages = int(os.getenv("ADZUNA_PAGES", "5"))
            for query in queries:
                for location in locations:
                    # Determine country from location if possible, default to us
                    country = "us"
                    loc_lower = location.lower()
                    if "united kingdom" in loc_lower or "uk" in loc_lower:
                        country = "gb"
                    elif "canada" in loc_lower or "ca" in loc_lower:
                        country = "ca"
                    elif "australia" in loc_lower or "au" in loc_lower:
                        country = "au"
                    elif "germany" in loc_lower or "de" in loc_lower:
                        country = "de"

                    ingesters.append(AdzunaIngester(
                        app_id=adzuna_id,
                        app_key=adzuna_key,
                        query=query,
                        location=location,
                        country=country,
                        pages=pages,
                        date_posted=date_posted,
                    ))
        else:
            print("WARNING: Adzuna source requested but ADZUNA_APP_ID/KEY not configured. Skipping.")

    return MultiSourceAggregator(
        ingesters=ingesters,
        target_count=target_count,
    )


def _build_ingester():
    queries_raw = os.getenv("INGEST_QUERIES", "software engineer,backend developer,python developer")
    queries = [q.strip() for q in queries_raw.split(",") if q.strip()]

    locations_raw = os.getenv("INGEST_LOCATIONS", "remote,United States")
    locations = [l.strip() for l in locations_raw.split(",") if l.strip()]

    sources = [JobSource.JSEARCH_API, JobSource.INDEED_RSS, JobSource.ADZUNA_API]
    target_count = int(os.getenv("TARGET_JOB_COUNT", "200"))
    date_posted = os.getenv("DATE_POSTED_FILTER", "week")

    return build_dynamic_aggregator(
        queries=queries,
        locations=locations,
        sources=sources,
        target_count=target_count,
        date_posted=date_posted,
    )


# Singleton pipeline — built once at startup
@lru_cache(maxsize=1)
def _build_pipeline() -> JobPipeline:
    gemini_key = os.getenv("GEMINI_API_KEY")
    if not gemini_key:
        raise RuntimeError("GEMINI_API_KEY environment variable is required")

    store = _build_store()

    return JobPipeline(
        ingester=_build_ingester(),
        parser=GeminiParser(
            api_key=gemini_key,
            model_name=os.getenv("GEMINI_MODEL", "gemini-1.5-flash"),
        ),
        validator=SchemaValidator(),
        handoff=_build_handoff(),
        store=store,
        max_concurrent_parses=int(os.getenv("MAX_CONCURRENT_PARSES", "15")),
    )


# FastAPI Depends callables
def get_pipeline() -> JobPipeline:
    return _build_pipeline()


def get_store() -> BaseStore:
    return _build_pipeline().store
