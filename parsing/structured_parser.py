"""
parsing/structured_parser.py

Fallback parser that extracts structured fields without an LLM.
Handles jobs from known sources (Workable JSON API) and falls back to
regex-based extraction for raw HTML/text sources.
"""

import json
import logging
import re
from datetime import datetime, timedelta
from core.interfaces import BaseParser
from core.models import RawJob, ParsedJob, SalaryRange, JobSource, stable_job_id

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
            id=stable_job_id(raw.source.value, raw.external_id),
            raw_id=raw.id,
            source=raw.source.value,
            job_title=title,
            company=company,
            location=location,
            remote=remote,
            salary=salary,
            required_skills=skills,
            employment_type=emp_type,
            description_clean=_clean_description(raw.raw_text),
            source_url=raw.source_url,
            application_link=raw.source_url,
            posted_date=meta.get("posted_date") or _extract_posted_date(raw.raw_text, raw.fetched_at),
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
            id=stable_job_id(raw.source.value, raw.external_id),
            raw_id=raw.id,
            job_title=title or "[UNKNOWN]",
            company=company,
            location=location,
            remote=remote,
            salary=salary,
            required_skills=skills,
            employment_type=emp_type,
            source=raw.source.value,
            description_clean=_clean_description(text),
            source_url=raw.source_url,
            application_link=raw.source_url,
            posted_date=(raw.metadata or {}).get("posted_date") or _extract_posted_date(text, raw.fetched_at),
            model_used="structured_parser",
            confidence=0.5 if title else 0.3,
            parse_warnings=warnings,
        )


def _extract_title(text: str) -> str | None:
    patterns = [
        r"^Title:\s*(.+)",
        r"(?:job\s*)?title[:\s]+([^\n]+)",
        # Tech roles
        r"^([A-Z][A-Za-z\s]+(?:Engineer|Developer|Manager|Designer|Analyst|Architect|Consultant|Lead|Head|Director))",
        # Non-tech roles: support, admin, sales, finance, HR, creative
        r"^([A-Z][A-Za-z\s]+(?:Representative|Assistant|Associate|Specialist|Coordinator|Agent|Clerk|Officer|Executive|Accountant|Marketer|Writer|Nurse|Teacher|Driver|Cleaner|Cook|Security|Cashier|Receptionist|Secretary))",
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
        # Tech
        "Python", "Java", "JavaScript", "TypeScript", "Go", "Rust", "C++", "C#",
        "React", "Angular", "Vue", "Node.js", "Django", "Flask", "FastAPI",
        "PostgreSQL", "MySQL", "MongoDB", "Redis", "Kubernetes", "Docker",
        "AWS", "Azure", "GCP", "Terraform", "CI/CD", "Git", "Linux",
        "Machine Learning", "AI", "Data Science", "NLP", "Computer Vision",
        "REST API", "GraphQL", "gRPC", "Kafka", "RabbitMQ", "Spark", "Flink",
        "Agile", "Scrum", "SQL", "NoSQL", "HTML", "CSS", "Sass",
        "Figma", "Photoshop", "Illustrator", "UI/UX", "Product Management",
        # Customer service & support
        "Customer Service", "Customer Support", "Communication", "Call Center",
        "CRM", "Zendesk", "Intercom", "Live Chat", "Complaint Resolution",
        # Admin / office / VA
        "Data Entry", "Microsoft Office", "MS Excel", "MS Word", "Google Workspace",
        "Typing", "Scheduling", "Calendar Management", "Email Management",
        "Bookkeeping", "Record Keeping", "Filing", "Transcription",
        # Sales & marketing
        "Sales", "Marketing", "Digital Marketing", "Social Media", "SEO",
        "Content Writing", "Copywriting", "Lead Generation", "Negotiation",
        "Market Research", "Brand Management", "Advertising",
        # Finance / HR
        "Accounting", "Payroll", "Invoicing", "QuickBooks", "Financial Reporting",
        "Recruitment", "Human Resources", "Onboarding", "Training",
        # General professional
        "Project Management", "Time Management", "Problem Solving",
        "Teamwork", "Attention to Detail", "Multitasking", "Report Writing",
    ]
    found = []
    for skill in known_skills:
        # Word-boundary match so short skills don't hit substrings
        # ("AI" in "email", "Go" in "Google", "Java" in "JavaScript").
        pattern = r"(?<!\w)" + re.escape(skill) + r"(?!\w)"
        if re.search(pattern, text, re.IGNORECASE):
            found.append(skill)
    return found


_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _extract_posted_date(text: str, fetched_at: datetime | None = None) -> str | None:
    """
    Best-effort posted-date extraction from HTML board text → 'YYYY-MM-DD'.
    Only trusts dates appearing near 'post' context, plus ISO dates.
    Relative dates ('X days ago') resolve against fetched_at (defaults to now).
    Returns None when nothing reliable is found (caller keeps the job).
    """
    ref = fetched_at or datetime.utcnow()

    # ISO date anywhere
    m = re.search(r"(\d{4}-\d{2}-\d{2})", text)
    if m:
        return m.group(1)

    # '24th Sep, 2026' / '3rd September 2026' (ordinal day, optional comma)
    m = re.search(
        r"(\d{1,2})(?:st|nd|rd|th)\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s*,?\s*(\d{4})",
        text, re.IGNORECASE,
    )
    if m:
        try:
            return datetime(int(m.group(3)), _MONTHS[m.group(2).lower()[:3]], int(m.group(1))).date().isoformat()
        except ValueError:
            pass

    # '12 Sep 2026' / '12 September 2026'
    m = re.search(
        r"(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+(\d{4})",
        text, re.IGNORECASE,
    )
    if m:
        try:
            return datetime(int(m.group(3)), _MONTHS[m.group(2).lower()[:3]], int(m.group(1))).date().isoformat()
        except ValueError:
            pass

    # 'Sep 12, 2026' / 'September 12, 2026'
    m = re.search(
        r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+(\d{1,2}),?\s+(\d{4})",
        text, re.IGNORECASE,
    )
    if m:
        try:
            return datetime(int(m.group(3)), _MONTHS[m.group(1).lower()[:3]], int(m.group(2))).date().isoformat()
        except ValueError:
            pass

    # Relative dates — require 'post' nearby to avoid false positives
    # e.g. 'Posted 3 days ago', 'Posted: 2 weeks ago', 'Posted today'
    for m in re.finditer(
        r"post(?:ed)?[^.\n]{0,30}?\b(\d+)\s+(day|week|month)s?\s+ago\b"
        r"|\b(\d+)\s+(day|week|month)s?\s+ago\b[^.\n]{0,30}?post",
        text, re.IGNORECASE,
    ):
        num = int(m.group(1) or m.group(3))
        unit = (m.group(2) or m.group(4)).lower()
        delta = {"day": num, "week": num * 7, "month": num * 30}[unit]
        return (ref - timedelta(days=delta)).date().isoformat()

    m = re.search(r"post(?:ed)?[^.\n]{0,20}?\b(today|yesterday)\b", text, re.IGNORECASE)
    if m:
        days = 1 if m.group(1).lower() == "yesterday" else 0
        return (ref - timedelta(days=days)).date().isoformat()

    return None


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
