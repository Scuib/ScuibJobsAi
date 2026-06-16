"""
scripts/phase1_test.py

Phase 1 POC: paste a job description, validate LLM extraction,
inspect the payload that would go to Dozie's algorithm.

Run: python scripts/phase1_test.py
"""

import asyncio
import json
import os
from dotenv import load_dotenv
load_dotenv()

from core.models import RawJob, JobSource
from parsing.gemini_parser import GeminiParser
from validation.validators import SchemaValidator
from handoff.handlers import MockHandoff, FileHandoff
from store.stores import InMemoryStore
from core.pipeline import JobPipeline
from ingestion.ingesters import ManualIngester

SAMPLE_JOB = """
Senior Backend Engineer - Remote (US)
TechCorp Inc | $140,000 - $180,000/year

We're looking for a senior backend engineer to join our platform team.
You'll architect and build high-throughput data pipelines processing
millions of events daily.

Requirements:
- 5+ years of backend engineering experience
- Strong proficiency in Python (FastAPI or Django) or Go
- Experience with distributed systems and message queues (Kafka, RabbitMQ)
- PostgreSQL and Redis expertise
- Familiarity with Docker and Kubernetes

Nice to have:
- Experience with data streaming (Flink, Spark)
- Open source contributions
- Prior startup experience

We offer competitive compensation, full remote, unlimited PTO, and
strong equity. Apply via our careers page.
"""


async def main():
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        print("ERROR: Set GEMINI_API_KEY in your .env file")
        return
    model_name = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")

    store = InMemoryStore()
    handoff = MockHandoff()

    pipeline = JobPipeline(
        ingester=ManualIngester(raw_text=SAMPLE_JOB),
        parser=GeminiParser(api_key=api_key, model_name=model_name),
        validator=SchemaValidator(),
        handoff=handoff,
        store=store,
    )

    print("=== Phase 1: Ingesting sample job ===\n")
    raw = RawJob(source=JobSource.MANUAL, raw_text=SAMPLE_JOB)
    result = await pipeline.ingest_single(raw)

    print(f"Result: {result}\n")

    pending = await store.get_pending()
    if not pending:
        print("No pending jobs — check parse errors above")
        return

    job = pending[0]
    print(f"=== Parsed Job (confidence: {job.confidence:.0%}) ===")
    print(f"Title:           {job.job_title}")
    print(f"Company:         {job.company}")
    print(f"Location:        {job.location} | Remote: {job.remote}")
    print(f"Salary:          {job.salary}")
    print(f"Required skills: {job.required_skills}")
    print(f"Experience:      {job.years_experience} yrs")
    print(f"Parse warnings:  {job.parse_warnings}")
    print(f"Validation:      {job.validation_issues or 'CLEAN'}\n")

    print("=== Approving and sending to algorithm (MockHandoff) ===\n")
    approve_result = await pipeline.approve_and_send(job.id, reviewer="silas", notes="Phase 1 test")
    print(f"Approve result: {approve_result}\n")

    if handoff.sent_jobs:
        payload = handoff.sent_jobs[0]
        print("=== Payload sent to Dozie's algorithm ===")
        print(json.dumps(payload.model_dump(mode="json"), indent=2))


if __name__ == "__main__":
    asyncio.run(main())
