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


def _build_ingester():
    jsearch_key = os.getenv("JSEARCH_API_KEY")
    if jsearch_key:
        return JSearchIngester(
            api_key=jsearch_key,
            query=os.getenv("INGEST_QUERY", "software engineer"),
            location=os.getenv("INGEST_LOCATION", "United States"),
        )
    return IndeedRSSIngester(
        query=os.getenv("INGEST_QUERY", "software engineer"),
        location=os.getenv("INGEST_LOCATION", "remote"),
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
        max_concurrent_parses=int(os.getenv("MAX_CONCURRENT_PARSES", "5")),
    )


# FastAPI Depends callables
def get_pipeline() -> JobPipeline:
    return _build_pipeline()


def get_store() -> BaseStore:
    return _build_pipeline().store
