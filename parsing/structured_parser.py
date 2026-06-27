"""
parsing/structured_parser.py

Fallback parser that extracts structured fields without an LLM.
Handles jobs from known sources (Workable JSON API) and falls back to
regex-based extraction for raw HTML/text sources.
"""

import json
import logging
import re
from core.interfaces import BaseParser
from core.models import RawJob, ParsedJob, SalaryRange, JobSource

logger = logging.getLogger(__name__)


class StructuredParser(BaseParser):
    """
    Parser that extracts fields from structured source metadata or raw text.
    - For Workable: uses metadata fields directly (title, company, location, etc.)
    - For others: regex extraction from raw_text
    - Always sets a lower confidence than LLM would
    """

    async def parse(self, raw: RawJob) -> ParsedJob:
        if raw.source == JobSource.WORKABLE:
            return self._parse_workable(raw)
        return self._parse_generic(raw)

    async def batch_parse(
        self, raws: list[RawJob], progress_callback=None
    ) -> list[ParsedJob]:
        results = []
        for i, raw in enumerate(raws):
            results.append(await self.parse(raw))
            if progress_callback:
                progress_callback(i + 1, len(raws))
        return results

    def _parse_workable(self, raw: RawJob) -> ParsedJob:
        meta = raw.metadata or {}
        title = meta.get("title", "") or _extract_title(raw.raw_text)
        company = meta.get("company", "") or _extract_company(raw.raw_text)

        loc = meta.get("location", {}) or {}
        location = ", ".join(
            filter(None, [loc.get("city", ""), loc.get("subregion", ""), loc.get("countryName", "")])
        ) or _extract_location(raw.raw_text)

        workplace = meta.get("workplace", "")
        remote = workplace == "remote"

        emp_type = meta.get("employment_type", "") or _extract_employment_type(raw.raw_text)

        salary = _extract_salary(raw.raw_text)
        skills = _extract_skills(raw.raw_text)

        return ParsedJob(
            raw_id=raw.id,
            job_title=title,
            company=company,
            location=location,
            remote=remote,
            salary=salary,
            required_skills=skills,
            employment_type=emp_type,
            description_clean=_clean_description(raw.raw_text),
            model_used="structured_parser",
            confidence=0.7,
            parse_warnings=["Parsed via structured extraction (no LLM)"] if not remote else [],
        )

    def _parse_generic(self, raw: RawJob) -> ParsedJob:
        text = raw.raw_text
        title = _extract_title(text)
        company = _extract_company(text)
        location = _extract_location(text)
        remote = "remote" in text.lower() or "hybrid" in text.lower()
        salary = _extract_salary(text)
        skills = _extract_skills(text)
        emp_type = _extract_employment_type(text)

        warnings = []
        if not title:
            warnings.append("Could not extract job title")
        if not company:
            warnings.append("Could not extract company name")

        return ParsedJob(
            raw_id=raw.id,
            job_title=title or "[UNKNOWN]",
            company=company,
            location=location,
            remote=remote,
            salary=salary,
            required_skills=skills,
            employment_type=emp_type,
            description_clean=_clean_description(text),
            model_used="structured_parser",
            confidence=0.5 if title else 0.3,
            parse_warnings=warnings,
        )


def _extract_title(text: str) -> str | None:
    patterns = [
        r"^Title:\s*(.+)",
        r"(?:job\s*)?title[:\s]+([^\n]+)",
        r"^([A-Z][A-Za-z\s]+(?:Engineer|Developer|Manager|Designer|Analyst|Architect|Consultant|Lead|Head|Director))",
    ]
    for p in patterns:
        m = re.search(p, text, re.IGNORECASE | re.MULTILINE)
        if m:
            return m.group(1).strip()
    return None


def _extract_company(text: str) -> str | None:
    patterns = [
        r"^Company:\s*(.+)",
        r"company[:\s]+([^\n]+)",
        r"at\s+([A-Z][A-Za-z0-9\s&.]+?)(?:\s*(?:-|\||in|\n|$))",
    ]
    for p in patterns:
        m = re.search(p, text, re.IGNORECASE | re.MULTILINE)
        if m:
            return m.group(1).strip()
    return None


def _extract_location(text: str) -> str | None:
    patterns = [
        r"^Location:\s*(.+)",
        r"location[:\s]+([^\n]+)",
        r"(?:remote|hybrid|on.?site)\s*(?:-|\||in)?\s*([A-Za-z,\s]+)",
        r"(?:in|at)\s+([A-Z][a-z]+(?:\s*,\s*[A-Z][a-z]+)?)",
    ]
    for p in patterns:
        m = re.search(p, text, re.IGNORECASE | re.MULTILINE)
        if m:
            cand = m.group(1).strip()
            if len(cand) > 2 and len(cand) < 100:
                return cand
    return None


def _extract_salary(text: str) -> SalaryRange | None:
    patterns = [
        r"(?:\$|USD|GBP|EUR)?\s*(\d{2,3}(?:,\d{3})?(?:k|K)?)\s*(?:-|–|to)\s*(?:\$|USD|GBP|EUR)?\s*(\d{2,3}(?:,\d{3})?(?:k|K)?)\s*(?:\s*(?:/year|/yr|/annum|yearly|/hr|/hour|hourly|monthly|/mo))?",
        r"(?:salary|range|pay)[:\s]+(?:\$|USD|GBP|EUR)?\s*(\d[\d,]*)\s*(?:-|–|to)\s*(?:\$|USD|GBP|EUR)?\s*(\d[\d,]*)",
    ]
    for p in patterns:
        for m in re.finditer(p, text, re.IGNORECASE):
            if not m:
                continue

            def parse_salary(s: str) -> int | None:
                s = s.replace(",", "").lower().strip()
                if s.endswith("k"):
                    try:
                        return int(float(s[:-1]) * 1000)
                    except ValueError:
                        return None
                try:
                    val = int(s)
                    # If value is unreasonably small (like < 1000), it's probably not a salary
                    if val < 1000:
                        return None
                    return val
                except ValueError:
                    return None

            first = parse_salary(m.group(1))
            second = parse_salary(m.group(2)) if len(m.groups()) >= 2 else None
            if first and second and first < 5_000_000 and second < 5_000_000:
                return SalaryRange(min=min(first, second), max=max(first, second))
    return None


def _extract_skills(text: str) -> list[str]:
    known_skills = [
        "Python", "Java", "JavaScript", "TypeScript", "Go", "Rust", "C++", "C#",
        "React", "Angular", "Vue", "Node.js", "Django", "Flask", "FastAPI",
        "PostgreSQL", "MySQL", "MongoDB", "Redis", "Kubernetes", "Docker",
        "AWS", "Azure", "GCP", "Terraform", "CI/CD", "Git", "Linux",
        "Machine Learning", "AI", "Data Science", "NLP", "Computer Vision",
        "REST API", "GraphQL", "gRPC", "Kafka", "RabbitMQ", "Spark", "Flink",
        "Agile", "Scrum", "SQL", "NoSQL", "HTML", "CSS", "Sass",
        "Figma", "Photoshop", "Illustrator", "UI/UX", "Product Management",
    ]
    found = []
    text_lower = text.lower()
    for skill in known_skills:
        if skill.lower() in text_lower:
            found.append(skill)
    return found


def _extract_employment_type(text: str) -> str | None:
    patterns = [
        r"employment\s*(?:type)?[:\s]+(full.time|part.time|contract|internship|freelance)",
        r"(full.time|part.time|contractor?|internship|freelance)",
    ]
    for p in patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            return m.group(1).strip().lower()
    return None


def _clean_description(text: str) -> str:
    lines = text.split("\n")
    cleaned = []
    in_description = False
    for line in lines:
        stripped = line.strip()
        if re.match(r"(?:description|requirements|qualifications|about|responsibilities|what you'll do)[:\s]",
                    stripped, re.IGNORECASE):
            in_description = True
        if in_description and stripped:
            cleaned.append(stripped)
    return "\n".join(cleaned[:100]) if cleaned else text[:2000]
