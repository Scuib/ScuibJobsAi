"""
parsing/groq_parser.py

Groq fallback parser — OpenAI-compatible chat completions API.
Used as second fallback after Gemini: Gemini -> Groq -> StructuredParser.

Env:
  GROQ_API_KEY  (required)
  GROQ_MODEL    (default: llama-3.3-70b-versatile)
"""

import asyncio
import json
import logging
import time
from typing import Callable, Any

import httpx

from core.interfaces import BaseParser
from core.models import RawJob, ParsedJob, SalaryRange, stable_job_id
from core.resilience import AdaptiveRateLimiter
from parsing.gemini_parser import EXTRACTION_PROMPT

logger = logging.getLogger(__name__)

GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
DEFAULT_MODEL = "llama-3.3-70b-versatile"


class GroqParser(BaseParser):
    """
    Groq LLM parser with same contract as GeminiParser.
    Calls Groq's OpenAI-compatible endpoint with retry + rate limiting.
    """

    def __init__(
        self,
        api_key: str,
        model_name: str = DEFAULT_MODEL,
        max_concurrent: int = 10,
        batch_chunk_size: int = 10,
        max_retries: int = 3,
        timeout: int = 30,
    ):
        if not api_key:
            raise ValueError("GROQ_API_KEY is required for GroqParser")
        self.api_key = api_key
        self.model_name = model_name
        self.max_concurrent = max_concurrent
        self.batch_chunk_size = batch_chunk_size
        self.max_retries = max_retries
        self.timeout = timeout

        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._rate_limiter = AdaptiveRateLimiter(max_rpm=60, min_rpm=5)

        self._total_parsed = 0
        self._total_errors = 0
        self._total_retries = 0

    async def parse(self, raw: RawJob) -> ParsedJob:
        return await self._parse_with_retry(raw)

    async def batch_parse(
        self,
        raws: list[RawJob],
        progress_callback: Callable[[int, int], Any] | None = None,
    ) -> list[ParsedJob]:
        total = len(raws)
        results: list[ParsedJob] = []
        parsed_count = 0

        logger.info(
            f"GroqParser: batch parse {total} jobs "
            f"(chunk_size={self.batch_chunk_size}, max_concurrent={self.max_concurrent})"
        )

        for chunk_start in range(0, total, self.batch_chunk_size):
            chunk_end = min(chunk_start + self.batch_chunk_size, total)
            chunk = raws[chunk_start:chunk_end]
            tasks = [self._parse_with_semaphore(r) for r in chunk]
            chunk_results = await asyncio.gather(*tasks, return_exceptions=True)

            for i, result in enumerate(chunk_results):
                if isinstance(result, Exception):
                    logger.error(f"GroqParser: chunk job {chunk_start+i} failed: {result}")
                    results.append(
                        ParsedJob(
                            id=stable_job_id(chunk[i].source.value, chunk[i].external_id),
                            raw_id=chunk[i].id,
                            job_title="[PARSE FAILED]",
                            source_url=chunk[i].source_url,
                            application_link=chunk[i].source_url,
                            posted_date=(chunk[i].metadata or {}).get("posted_date"),
                            model_used=self.model_name,
                            confidence=0.0,
                            parse_warnings=[f"Groq parse error: {result}"],
                        )
                    )
                    self._total_errors += 1
                else:
                    results.append(result)

                parsed_count += 1
                if progress_callback:
                    try:
                        progress_callback(parsed_count, total)
                    except Exception:
                        pass

        return results

    async def _parse_with_semaphore(self, raw: RawJob) -> ParsedJob:
        async with self._semaphore:
            return await self._parse_with_retry(raw)

    async def _parse_with_retry(self, raw: RawJob) -> ParsedJob:
        prompt = EXTRACTION_PROMPT.replace("{raw_text}", raw.raw_text[:8000])
        last_exc = None

        for attempt in range(1, self.max_retries + 1):
            try:
                await self._rate_limiter.acquire()
                start = time.monotonic()
                parsed = await self._call_model(raw, prompt)
                elapsed = time.monotonic() - start
                self._rate_limiter.record_success()
                self._total_parsed += 1
                logger.debug(f"GroqParser: {raw.id} in {elapsed:.2f}s conf={parsed.confidence}")
                return parsed
            except Exception as e:
                last_exc = e
                self._total_retries += 1
                err = str(e).lower()
                if "429" in err or "rate" in err:
                    self._rate_limiter.record_throttle()
                if attempt < self.max_retries:
                    import random

                    delay = min(2.0 * (2 ** (attempt - 1)), 30.0) * (0.5 + random.random())
                    logger.warning(
                        f"GroqParser: attempt {attempt}/{self.max_retries} failed {raw.id} ({type(e).__name__}): {e} — retry {delay:.1f}s"
                    )
                    await asyncio.sleep(delay)

        self._total_errors += 1
        logger.error(f"GroqParser: all attempts failed {raw.id}: {last_exc}")
        return ParsedJob(
            id=stable_job_id(raw.source.value, raw.external_id),
            raw_id=raw.id,
            job_title="[PARSE FAILED]",
            source_url=raw.source_url,
            application_link=raw.source_url,
            posted_date=(raw.metadata or {}).get("posted_date"),
            model_used=self.model_name,
            confidence=0.0,
            parse_warnings=[f"All {self.max_retries} Groq attempts failed: {last_exc}"],
        )

    async def _call_model(self, raw: RawJob, prompt: str) -> ParsedJob:
        payload = {
            "model": self.model_name,
            "messages": [
                {
                    "role": "system",
                    "content": "You are a strict JSON extraction engine. Return ONLY valid JSON.",
                },
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.1,
            "response_format": {"type": "json_object"},
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.post(GROQ_API_URL, json=payload, headers=headers)
            resp.raise_for_status()
            data = resp.json()

        content = data["choices"][0]["message"]["content"].strip()
        if content.startswith("```"):
            content = content.split("```")[1]
            if content.startswith("json"):
                content = content[4:]
            content = content.strip()

        parsed_json = json.loads(content)
        return self._build_parsed_job(raw, parsed_json)

    def _build_parsed_job(self, raw: RawJob, data: dict) -> ParsedJob:
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
            id=stable_job_id(raw.source.value, raw.external_id),
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
            source_url=raw.source_url,
            application_link=data.get("application_link") or raw.source_url,
            posted_date=data.get("posted_date") or (raw.metadata or {}).get("posted_date"),
            model_used=self.model_name,
            confidence=float(data.get("confidence", 1.0)),
            parse_warnings=data.get("parse_warnings", []),
        )

    def get_stats(self) -> dict:
        return {
            "model": self.model_name,
            "total_parsed": self._total_parsed,
            "total_errors": self._total_errors,
            "total_retries": self._total_retries,
            "rate_limiter": self._rate_limiter.stats(),
        }
