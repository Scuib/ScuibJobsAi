"""
validation/validators.py

Programmatic rule engine that runs BEFORE human review.
Flags structural/logical issues — doesn't block jobs, just annotates them.
"""

from core.interfaces import BaseValidator
from core.models import ParsedJob


class SchemaValidator(BaseValidator):
    """
    Validates a ParsedJob against business rules.
    Returns (is_valid, issues) — issues are surfaced in the admin panel diff view.

    Extend by subclassing and overriding `_rules()`.
    """

    MIN_CONFIDENCE_THRESHOLD = 0.5
    MAX_SANE_SALARY = 5_000_000
    MIN_SANE_SALARY = 10_000

    async def validate(self, parsed: ParsedJob) -> tuple[bool, list[str]]:
        issues = []
        for rule in self._rules():
            issue = rule(parsed)
            if issue:
                issues.append(issue)
        return len(issues) == 0, issues

    def _rules(self):
        """Override or extend to add/remove rules."""
        return [
            self._rule_title_present,
            self._rule_skills_present,
            self._rule_low_confidence,
            self._rule_salary_sanity,
            self._rule_parse_failed,
        ]

    # ─── Individual rules ─────────────────────────────────────────────────────

    @staticmethod
    def _rule_title_present(p: ParsedJob) -> str | None:
        if not p.job_title or p.job_title in ("[UNKNOWN]", "[PARSE FAILED]"):
            return "Missing or invalid job title"
        return None

    @staticmethod
    def _rule_skills_present(p: ParsedJob) -> str | None:
        if not p.required_skills:
            return "No required skills extracted — verify manually"
        return None

    def _rule_low_confidence(self, p: ParsedJob) -> str | None:
        if p.confidence < self.MIN_CONFIDENCE_THRESHOLD:
            return f"Low LLM confidence ({p.confidence:.0%}) — review extraction carefully"
        return None

    def _rule_salary_sanity(self, p: ParsedJob) -> str | None:
        if p.salary is None:
            return None
        s = p.salary
        if s.min and s.min < self.MIN_SANE_SALARY:
            return f"Suspiciously low salary min: {s.min}"
        if s.max and s.max > self.MAX_SANE_SALARY:
            return f"Suspiciously high salary max: {s.max}"
        if s.min and s.max and s.min > s.max:
            return f"Salary min ({s.min}) exceeds max ({s.max})"
        return None

    @staticmethod
    def _rule_parse_failed(p: ParsedJob) -> str | None:
        if "[PARSE FAILED]" in p.job_title:
            return "LLM parsing failed — raw text may be corrupt or too short"
        return None


class StrictValidator(SchemaValidator):
    """
    Stricter variant: also requires company, location, and employment type.
    Use when data quality requirements are higher.
    """

    def _rules(self):
        return super()._rules() + [
            self._rule_company_present,
            self._rule_location_present,
            self._rule_employment_type,
        ]

    @staticmethod
    def _rule_company_present(p: ParsedJob) -> str | None:
        if not p.company:
            return "Missing company name"
        return None

    @staticmethod
    def _rule_location_present(p: ParsedJob) -> str | None:
        if not p.location and not p.remote:
            return "Missing location and not marked as remote"
        return None

    @staticmethod
    def _rule_employment_type(p: ParsedJob) -> str | None:
        if not p.employment_type:
            return "Missing employment type"
        return None
