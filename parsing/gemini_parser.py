"""
parsing/gemini_parser.py

Enterprise-grade Gemini parser for high-throughput job extraction.

Upgrades over POC:
- Chunked batch processing with adaptive concurrency
- Per-job retry with exponential backoff + jitter
- Automatic backoff on 429/503 (Gemini rate limits)
- Fallback model support (flash → pro on repeated failures)
- Progress callback for real-time UI updates
- Parse latency tracking for metrics
"""

import asyncio
import json
import logging
import time
from typing import Callable, Any
from datetime import datetime
from core.interfaces import BaseParser
from core.models import RawJob, ParsedJob, SalaryRange
from core.resilience import AdaptiveRateLimiter

logger = logging.getLogger(__name__)

EXTRACTION_PROMPT = """
You are a strict data extraction engine for job postings. 
Extract the following fields from the raw job posting text below.
Return ONLY a valid JSON object — no explanation, no markdown, no backticks.

Required JSON schema:
{
  "job_title": "string (required)",
  "company": "string or null",
  "location": "string or null",
  "remote": true | false,
  "salary": {
    "min": integer or null,
    "max": integer or null,
    "currency": "USD",
    "period": "yearly | monthly | hourly"
  } or null,
  "required_skills": ["string", ...],
  "preferred_skills": ["string", ...],
  "years_experience": integer or null,
  "education_level": "string or null",
  "employment_type": "full-time | part-time | contract | internship or null",
  "description_clean": "2-3 sentence plain summary of the role",
  "confidence": 0.0 to 1.0,
  "parse_warnings": ["string", ...]
}

Rules:
- confidence: 1.0 = all fields extracted cleanly. Lower if data is missing or ambiguous.
- parse_warnings: note any fields that were ambiguous or missing.
- required_skills vs preferred_skills: use "required" for must-haves, "preferred" for nice-to-haves.
- Normalize salary to yearly USD where possible.
- If you cannot determine a field, use null — do NOT hallucinate values.

Raw job posting:
{raw_text}
"""


class GeminiParser(BaseParser):
    """
    Enterprise-grade Gemini parser with adaptive concurrency,
    chunked batch processing, and automatic rate limit handling.

    Key capabilities:
    - Parses individual jobs or batches of 200+ jobs
    - Adaptive concurrency: starts at max_concurrent, backs off on 429s
    - Chunked batching: processes jobs in chunks to avoid overwhelming the API
    - Per-job retry with exponential backoff (3 attempts)
    - Optional fallback model on repeated failures
    - Progress callback for real-time reporting
    - Parse latency tracking for metrics
    """

    def __init__(
        self,
        api_key: str,
        model_name: str = "gemini-1.5-flash",
        fallback_model: str | None = "gemini-1.5-pro",
        max_concurrent: int = 10,
        batch_chunk_size: int = 10,
        max_retries: int = 3,
    ):
        import google.generativeai as genai
        genai.configure(api_key=api_key)
        self._genai = genai
        self.model = genai.GenerativeModel(model_name)
        self.model_name = model_name

        # Fallback model for when primary fails repeatedly
        self.fallback_model = None
        self.fallback_model_name = fallback_model
        if fallback_model and fallback_model != model_name:
            self.fallback_model = genai.GenerativeModel(fallback_model)

        # Concurrency control
        self.max_concurrent = max_concurrent
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._rate_limiter = AdaptiveRateLimiter(max_rpm=60, min_rpm=5)
        self.batch_chunk_size = batch_chunk_size
        self.max_retries = max_retries

        # Metrics
        self._total_parsed: int = 0
        self._total_errors: int = 0
        self._total_retries: int = 0
        self._total_fallbacks: int = 0

    async def parse(self, raw: RawJob) -> ParsedJob:
        """Parse a single raw job with retry and optional fallback."""
        return await self._parse_with_retry(raw)

    async def batch_parse(
        self,
        raws: list[RawJob],
        progress_callback: Callable[[int, int], Any] | None = None,
    ) -> list[ParsedJob]:
        """
        Enterprise batch parse: process jobs in chunks with adaptive concurrency.

        Args:
            raws: List of raw jobs to parse
            progress_callback: Optional callback(parsed_count, total_count)
                               called after each successful parse
        """
        total = len(raws)
        results: list[ParsedJob] = []
        parsed_count = 0

        logger.info(
            f"GeminiParser: starting batch parse of {total} jobs "
            f"(chunk_size={self.batch_chunk_size}, "
            f"max_concurrent={self.max_concurrent})"
        )

        # Process in chunks to manage memory and provide checkpoints
        for chunk_start in range(0, total, self.batch_chunk_size):
            chunk_end = min(chunk_start + self.batch_chunk_size, total)
            chunk = raws[chunk_start:chunk_end]

            # Parse chunk concurrently (bounded by semaphore)
            chunk_tasks = [
                self._parse_with_semaphore(raw) for raw in chunk
            ]
            chunk_results = await asyncio.gather(
                *chunk_tasks, return_exceptions=True
            )

            for i, result in enumerate(chunk_results):
                if isinstance(result, Exception):
                    logger.error(
                        f"GeminiParser: batch job {chunk_start + i} failed: {result}"
                    )
                    # Create a failure ParsedJob
                    results.append(ParsedJob(
                        raw_id=chunk[i].id,
                        job_title="[PARSE FAILED]",
                        model_used=self.model_name,
                        confidence=0.0,
                        parse_warnings=[f"Batch parse error: {result}"],
                    ))
                    self._total_errors += 1
                else:
                    results.append(result)

                parsed_count += 1
                if progress_callback:
                    try:
                        progress_callback(parsed_count, total)
                    except Exception:
                        pass  # Don't let callback errors break parsing

            logger.info(
                f"GeminiParser: chunk complete — "
                f"{parsed_count}/{total} parsed "
                f"({self._total_errors} errors, "
                f"{self._total_retries} retries, "
                f"{self._total_fallbacks} fallbacks)"
            )

        return results

    async def _parse_with_semaphore(self, raw: RawJob) -> ParsedJob:
        """Parse with concurrency limiter."""
        async with self._semaphore:
            return await self._parse_with_retry(raw)

    async def _parse_with_retry(self, raw: RawJob) -> ParsedJob:
        """Parse a single job with retry, backoff, and optional fallback."""
        prompt = EXTRACTION_PROMPT.replace("{raw_text}", raw.raw_text[:8000])
        last_exception = None

        for attempt in range(1, self.max_retries + 1):
            try:
                # Rate limiting
                await self._rate_limiter.acquire()

                start_time = time.monotonic()
                parsed_job = await self._call_model(
                    raw, prompt, self.model, self.model_name
                )
                elapsed = time.monotonic() - start_time

                self._rate_limiter.record_success()
                self._total_parsed += 1

                logger.debug(
                    f"GeminiParser: parsed job {raw.id} in {elapsed:.2f}s "
                    f"(confidence={parsed_job.confidence})"
                )
                return parsed_job

            except Exception as e:
                last_exception = e
                self._total_retries += 1

                # Check if it's a rate limit error
                error_str = str(e).lower()
                if "429" in error_str or "resource exhausted" in error_str:
                    self._rate_limiter.record_throttle()

                if attempt < self.max_retries:
                    import random
                    delay = min(2.0 * (2 ** (attempt - 1)), 30.0)
                    delay *= 0.5 + random.random()
                    logger.warning(
                        f"GeminiParser: attempt {attempt}/{self.max_retries} "
                        f"failed for {raw.id} ({type(e).__name__}). "
                        f"Retrying in {delay:.1f}s..."
                    )
                    await asyncio.sleep(delay)

        # All retries failed — try fallback model if available
        if self.fallback_model:
            try:
                logger.info(
                    f"GeminiParser: falling back to {self.fallback_model_name} "
                    f"for {raw.id}"
                )
                await self._rate_limiter.acquire()
                parsed_job = await self._call_model(
                    raw, prompt, self.fallback_model, self.fallback_model_name
                )
                self._total_fallbacks += 1
                self._total_parsed += 1
                return parsed_job
            except Exception as fallback_err:
                logger.error(
                    f"GeminiParser: fallback model also failed for {raw.id}: "
                    f"{fallback_err}"
                )

        # Complete failure
        self._total_errors += 1
        logger.error(
            f"GeminiParser: all attempts exhausted for {raw.id}. "
            f"Last error: {last_exception}"
        )
        return ParsedJob(
            raw_id=raw.id,
            job_title="[PARSE FAILED]",
            model_used=self.model_name,
            confidence=0.0,
            parse_warnings=[
                f"All {self.max_retries} parse attempts failed: {last_exception}"
            ],
        )

    async def _call_model(
        self,
        raw: RawJob,
        prompt: str,
        model: Any,
        model_name: str,
    ) -> ParsedJob:
        """Execute a single Gemini API call and parse the response."""
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: model.generate_content(prompt)
        )

        raw_json = response.text.strip()
        # Strip accidental markdown fences
        if raw_json.startswith("```"):
            raw_json = raw_json.split("```")[1]
            if raw_json.startswith("json"):
                raw_json = raw_json[4:]
        raw_json = raw_json.strip()

        data = json.loads(raw_json)
        return self._build_parsed_job(raw, data, model_name)

    def _build_parsed_job(self, raw: RawJob, data: dict, model_name: str) -> ParsedJob:
        salary_data = data.get("salary")
        salary = None
        if salary_data:
            salary = SalaryRange(
                min=salary_data.get("min"),
                max=salary_data.get("max"),
                currency=salary_data.get("currency", "USD"),
                period=salary_data.get("period", "yearly"),
            )

        return ParsedJob(
            raw_id=raw.id,
            job_title=data.get("job_title", "[UNKNOWN]"),
            company=data.get("company"),
            location=data.get("location"),
            remote=data.get("remote", False),
            salary=salary,
            required_skills=data.get("required_skills", []),
            preferred_skills=data.get("preferred_skills", []),
            years_experience=data.get("years_experience"),
            education_level=data.get("education_level"),
            employment_type=data.get("employment_type"),
            description_clean=data.get("description_clean"),
            model_used=model_name,
            confidence=float(data.get("confidence", 1.0)),
            parse_warnings=data.get("parse_warnings", []),
        )

    def get_stats(self) -> dict:
        """Return parser performance stats."""
        return {
            "model": self.model_name,
            "fallback_model": self.fallback_model_name,
            "total_parsed": self._total_parsed,
            "total_errors": self._total_errors,
            "total_retries": self._total_retries,
            "total_fallbacks": self._total_fallbacks,
            "rate_limiter": self._rate_limiter.stats(),
        }
