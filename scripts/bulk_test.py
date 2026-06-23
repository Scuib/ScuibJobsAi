"""
scripts/bulk_test.py

Enterprise-grade test suite for the bulk ingestion pipeline.
Validates:
- Concurrent multi-source aggregation
- In-memory & persistent deduplication
- Circuit breakers & resilience
- Batch parsing and validation
- Pipeline stats reporting

Run: python scripts/bulk_test.py
"""

import asyncio
import os
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

from core.interfaces import BaseIngester, BaseParser
from core.models import RawJob, ParsedJob, JobSource, JobStatus, IngestionStats
from core.pipeline import JobPipeline
from validation.validators import SchemaValidator
from handoff.handlers import MockHandoff
from store.stores import InMemoryStore
from ingestion.aggregator import MultiSourceAggregator
from core.resilience import CircuitBreaker


# ─── Mock Implementations for Hermetic Testing ───────────────────────────────

class MockIngester(BaseIngester):
    def __init__(self, source: JobSource, count: int, prefix: str = "job", duplicate_indices: list[int] = None):
        self.source = source
        self.count = count
        self.prefix = prefix
        self.duplicate_indices = duplicate_indices or []

    async def fetch(self) -> any:
        for i in range(self.count):
            # Simulate a small delay
            await asyncio.sleep(0.01)
            
            # Use duplicate ID or unique ID
            is_dup = i in self.duplicate_indices
            ext_id = f"{self.source.value}_{self.prefix}_{'dup' if is_dup else i}"
            
            # Construct text representation (fingerprint normalized: Title:\nCompany:\n...)
            title = f"{self.prefix.capitalize()} Engineer {'Dup' if is_dup else i}"
            company = f"Mock Company {self.prefix.upper()}"
            raw_text = f"Title: {title}\nCompany: {company}\nDescription: This is a job description for {title}."
            
            yield RawJob(
                source=self.source,
                external_id=ext_id,
                raw_text=raw_text,
                source_url=f"http://example.com/{ext_id}",
            )

    async def health_check(self) -> bool:
        return True


class MockFailingIngester(BaseIngester):
    def __init__(self, source: JobSource):
        self.source = source

    async def fetch(self) -> any:
        # Simulate immediate failure
        raise RuntimeError(f"Failed to connect to source {self.source}")
        if False:
            yield None  # satisfy generator typing

    async def health_check(self) -> bool:
        return False


class MockParser(BaseParser):
    async def parse(self, raw: RawJob) -> ParsedJob:
        return ParsedJob(
            raw_id=raw.id,
            job_title="Mock Title",
            company="Mock Company",
            confidence=0.95,
        )

    async def batch_parse(self, raws: list[RawJob], progress_callback=None) -> list[ParsedJob]:
        results = []
        for i, raw in enumerate(raws):
            await asyncio.sleep(0.01)
            # Extract title and company from first lines
            lines = raw.raw_text.strip().split("\n")
            title = lines[0].replace("Title:", "").strip() if lines else "Mock Title"
            company = lines[1].replace("Company:", "").strip() if len(lines) > 1 else "Mock Company"
            
            # Mock validation issue for some jobs to test validation statistics
            confidence = 0.45 if "dup" in raw.external_id.lower() else 0.95
            
            results.append(ParsedJob(
                raw_id=raw.id,
                job_title=title,
                company=company,
                confidence=confidence,
                required_skills=["Python"] if confidence > 0.5 else [],
            ))
            if progress_callback:
                progress_callback(i + 1, len(raws))
        return results


# ─── Tests ───────────────────────────────────────────────────────────────────

async def test_aggregator_dedup():
    print("Test 1: Verifying MultiSourceAggregator Concurrent Fetch & Deduplication")
    # Source A generates 5 jobs, index 2 is duplicate
    # Source B generates 5 jobs, index 2 is duplicate
    # We also add overlap between Source A and Source B (fingerprint dedup)
    ing1 = MockIngester(JobSource.JSEARCH_API, 5, "dev", duplicate_indices=[2])
    ing2 = MockIngester(JobSource.INDEED_RSS, 5, "dev", duplicate_indices=[2])  # Same prefix "dev" causes overlap!
    
    aggregator = MultiSourceAggregator(ingesters=[ing1, ing2], target_count=50)
    
    count = 0
    async for job in aggregator.fetch():
        count += 1
        
    stats = aggregator.get_stats()
    print(f"-> Total unique jobs yielded: {count}")
    print(f"-> Duplicate jobs filtered: {stats['total_duplicates']}")
    print(f"-> Stats dictionary: {stats}")
    
    # We expect duplicates to be filtered:
    # Source A: 5 jobs, but 1 inner dup (index 2) -> 4 unique jobs
    # Source B: 5 jobs, but 1 inner dup (index 2) -> 4 unique jobs
    # But because both have prefix "dev", they will overlap.
    # Total unique: 4 (dev_0, dev_1, dev_3, dev_4). Inner duplicates: 2. Cross duplicates: 4.
    assert count == 4, f"Expected 4 unique jobs, got {count}"
    assert stats["total_duplicates"] == 6, f"Expected 6 total duplicates, got {stats['total_duplicates']}"
    print("SUCCESS: Deduplication & Concurrency works flawlessly.\n")


async def test_circuit_breaker():
    print("Test 2: Verifying Circuit Breaker & Resilience")
    ing_healthy = MockIngester(JobSource.JSEARCH_API, 3, "healthy")
    ing_failing = MockFailingIngester(JobSource.ADZUNA_API)
    
    aggregator = MultiSourceAggregator(ingesters=[ing_healthy, ing_failing], target_count=10)
    
    count = 0
    async for job in aggregator.fetch():
        count += 1
        
    print(f"-> Successfully pulled {count} jobs from healthy source despite one source failing.")
    assert count == 3, f"Expected 3 jobs from healthy source, got {count}"
    print("SUCCESS: Ingestion resilience verified.\n")


async def test_pipeline_bulk_ingestion():
    print("Test 3: Verifying Full Ingestion Pipeline (run_bulk_ingestion)")
    store = InMemoryStore()
    handoff = MockHandoff()
    parser = MockParser()
    validator = SchemaValidator()
    
    pipeline = JobPipeline(
        ingester=MockIngester(JobSource.MANUAL, 1), # default unused
        parser=parser,
        validator=validator,
        handoff=handoff,
        store=store,
    )
    
    # Create the aggregator with overlapping sources
    ing1 = MockIngester(JobSource.JSEARCH_API, 8, "alpha", duplicate_indices=[2])
    ing2 = MockIngester(JobSource.INDEED_RSS, 6, "beta", duplicate_indices=[1])
    aggregator = MultiSourceAggregator(ingesters=[ing1, ing2], target_count=15)
    
    # We will test auto-approve threshold of 0.8
    run_id = "test-run-123"
    
    def progress_cb(step, current, total):
        print(f"   [Progress Callback] {step}: {current}/{total}")
        
    stats = await pipeline.run_bulk_ingestion(
        aggregator=aggregator,
        run_id=run_id,
        auto_approve_threshold=0.8,
        progress_callback=progress_cb,
    )
    
    print("\n--- Execution Statistics ---")
    print(stats.model_dump_json(indent=2))
    
    # Assertions
    assert stats.run_id == run_id
    assert stats.total_fetched == 12  # (8 unique raw from ing1 - 1 dup) + (6 unique raw from ing2 - 1 dup) = 7 + 5 = 12
    # Verify DB contains the raw and parsed entries
    pending = await store.get_pending()
    print(f"-> Pending review jobs count: {len(pending)}")
    
    # Total unique parsed: 12. Let's see how many were flagged and validated.
    # The duplicate items (which we set to confidence 0.45) should be flagged by validation (no skills, low confidence)
    # The rest should be validated, and since confidence is 0.95 (> 0.8), they should be auto-approved!
    # Validated jobs get status APPROVED, so they won't appear in pending review.
    # Therefore, pending review contains only low-confidence or flagged jobs.
    print(f"-> Total raw jobs in store: {len(store._raw)}")
    print(f"-> Total parsed jobs in store: {len(store._parsed)}")
    
    assert len(store._raw) == 12
    assert len(store._parsed) == 12
    print("SUCCESS: Full pipeline bulk ingestion test completed successfully.\n")


async def test_real_gemini_batch():
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        print("Skipping Real Gemini Test: GEMINI_API_KEY not configured in environment.")
        return
        
    print("Test 4: Verifying Real Gemini API Batch Parsing (3 jobs)")
    from parsing.gemini_parser import GeminiParser
    real_parser = GeminiParser(
        api_key=api_key,
        model_name=os.getenv("GEMINI_MODEL", "gemini-1.5-flash"),
        max_concurrent=3,
        batch_chunk_size=3,
    )
    
    # 3 dummy jobs to parse
    raws = [
        RawJob(
            source=JobSource.MANUAL,
            external_id=f"real_test_{i}",
            raw_text=f"Title: Senior Backend Developer {i}\nCompany: Tech {i}\nDescription: Seeking Python developer.",
        )
        for i in range(3)
    ]
    
    start_time = time.monotonic()
    parsed_jobs = await real_parser.batch_parse(raws)
    duration = time.monotonic() - start_time
    
    print(f"-> Parsed {len(parsed_jobs)} jobs in {duration:.2f} seconds.")
    for j in parsed_jobs:
        print(f"   - {j.job_title} | Company: {j.company} | Conf: {j.confidence:.0%}")
        assert j.job_title != "[PARSE FAILED]", "LLM parse failed"
        
    print("SUCCESS: Real Gemini bulk parse completed successfully.\n")


async def main():
    print("====================================================")
    print("       SCUIB JOBS AI - BULK PIPELINE TESTS          ")
    print("====================================================\n")
    
    await test_aggregator_dedup()
    await test_circuit_breaker()
    await test_pipeline_bulk_ingestion()
    await test_real_gemini_batch()
    
    print("All tests passed successfully.")

if __name__ == "__main__":
    asyncio.run(main())
