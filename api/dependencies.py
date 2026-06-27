"""
api/dependencies.py

Dependency injection container.
Change implementations here — nothing else needs to change.
Phase 1: InMemoryStore + MockHandoff + ManualIngester
Phase 2: SupabaseStore + HTTPHandoff + IndeedRSSIngester
Phase 3+: Custom ingesters (Workable, MyJobMag, Fuzu, JobGurus, Jobberman)
"""

import os
from functools import lru_cache

from core.pipeline import JobPipeline
from core.interfaces import BaseStore, BaseParser

from ingestion.ingesters import IndeedRSSIngester, ManualIngester, JSearchIngester
from ingestion.aggregator import MultiSourceAggregator
from ingestion.adzuna_ingester import AdzunaIngester
from ingestion.custom_ingesters import (
    WorkableIngester,
    MyJobMagIngester,
    FuzuIngester,
    JobGurusIngester,
    JobbermanIngester,
)
from core.models import JobSource
from parsing.gemini_parser import GeminiParser
from parsing.structured_parser import StructuredParser
from parsing.hybrid_parser import HybridParser
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

    # 4. Workable API
    if JobSource.WORKABLE in sources:
        for query in queries:
            ingesters.append(WorkableIngester(
                query=query,
                location=locations[0] if locations else "",
            ))

    # 5. MyJobMag (Nigerian job board)
    if JobSource.MYJOBMAG in sources:
        for query in queries:
            ingesters.append(MyJobMagIngester(query=query))

    # 6. Fuzu (African job board)
    if JobSource.FUZU in sources:
        for query in queries:
            for location in locations:
                ingesters.append(FuzuIngester(query=query, location=location))

    # 7. JobGurus (Nigerian job board)
    if JobSource.JOBGURUS in sources:
        for query in queries:
            ingesters.append(JobGurusIngester(query=query))

    # 8. Jobberman (Nigerian job board)
    if JobSource.JOBBERMAN in sources:
        for query in queries:
            ingesters.append(JobbermanIngester(query=query))

    return MultiSourceAggregator(
        ingesters=ingesters,
        target_count=target_count,
    )


def _build_ingester():
    queries_raw = os.getenv("INGEST_QUERIES", "software engineer,backend developer,python developer")
    queries = [q.strip() for q in queries_raw.split(",") if q.strip()]

    locations_raw = os.getenv("INGEST_LOCATIONS", "remote,United States")
    locations = [l.strip() for l in locations_raw.split(",") if l.strip()]

    sources_raw = os.getenv("INGEST_SOURCES", "workable,myjobmag,fuzu,jobgurus,jobberman")
    source_map = {
        "workable": JobSource.WORKABLE,
        "myjobmag": JobSource.MYJOBMAG,
        "fuzu": JobSource.FUZU,
        "jobgurus": JobSource.JOBGURUS,
        "jobberman": JobSource.JOBBERMAN,
    }
    sources = []
    for s in sources_raw.split(","):
        s = s.strip().lower()
        if s in source_map:
            sources.append(source_map[s])

    target_count = int(os.getenv("TARGET_JOB_COUNT", "200"))

    return build_dynamic_aggregator(
        queries=queries,
        locations=locations,
        sources=sources,
        target_count=target_count,
    )


def _build_parser() -> BaseParser:
    """
    Build a HybridParser: tries Gemini first, falls back to structured extraction.
    """
    gemini_key = os.getenv("GEMINI_API_KEY")

    fallback = StructuredParser()

    if gemini_key:
        primary = GeminiParser(
            api_key=gemini_key,
            model_name=os.getenv("GEMINI_MODEL", "gemini-2.0-flash"),
            fallback_model=os.getenv("GEMINI_FALLBACK_MODEL", "gemini-2.5-pro"),
        )
        return HybridParser(primary=primary, fallback=fallback)

    print("GEMINI_API_KEY not set — using StructuredParser only")
    return fallback


# Singleton pipeline — built once at startup
@lru_cache(maxsize=1)
def _build_pipeline() -> JobPipeline:
    store = _build_store()

    return JobPipeline(
        ingester=_build_ingester(),
        parser=_build_parser(),
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
