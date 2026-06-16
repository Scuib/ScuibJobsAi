"""
parsing/gemini_parser.py

Concrete BaseParser using Google Gemini via google-generativeai.
Extracts structured job data from raw text with strict JSON output.
"""

import json
import logging
from datetime import datetime
from core.interfaces import BaseParser
from core.models import RawJob, ParsedJob, SalaryRange

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
    Parses raw job text using Gemini Flash (fast, cheap for extraction tasks).
    Swap model_name to gemini-1.5-pro for higher accuracy if needed.
    """

    def __init__(self, api_key: str, model_name: str = "gemini-1.5-flash"):
        import google.generativeai as genai
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(model_name)
        self.model_name = model_name

    async def parse(self, raw: RawJob) -> ParsedJob:
        import asyncio
        prompt = EXTRACTION_PROMPT.replace("{raw_text}", raw.raw_text[:8000])  # Token safety cap

        try:
            # Gemini's Python SDK is sync; run in executor to not block event loop
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.model.generate_content(prompt)
            )

            raw_json = response.text.strip()
            # Strip accidental markdown fences
            if raw_json.startswith("```"):
                raw_json = raw_json.split("```")[1]
                if raw_json.startswith("json"):
                    raw_json = raw_json[4:]
            raw_json = raw_json.strip()

            data = json.loads(raw_json)
            return self._build_parsed_job(raw, data)

        except json.JSONDecodeError as e:
            logger.error(f"Gemini returned invalid JSON for raw {raw.id}: {e}")
            # Return a minimal ParsedJob with failure signal
            return ParsedJob(
                raw_id=raw.id,
                job_title="[PARSE FAILED]",
                model_used=self.model_name,
                confidence=0.0,
                parse_warnings=[f"JSON decode error: {e}"],
            )
        except Exception as e:
            logger.error(f"Gemini API error for raw {raw.id}: {e}")
            raise

    async def batch_parse(self, raws: list[RawJob]) -> list[ParsedJob]:
        import asyncio
        # Parallel parse — semaphore in pipeline limits concurrency upstream
        tasks = [self.parse(raw) for raw in raws]
        return await asyncio.gather(*tasks)

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
            model_used=self.model_name,
            confidence=float(data.get("confidence", 1.0)),
            parse_warnings=data.get("parse_warnings", []),
        )
