"""
parsing/hybrid_parser.py

Combines GeminiParser (preferred) with StructuredParser (fallback).
Tries LLM first; if it fails or returns low confidence, falls back to structured extraction.
"""

import logging
from core.interfaces import BaseParser
from core.models import RawJob, ParsedJob

logger = logging.getLogger(__name__)


class HybridParser(BaseParser):
    """
    Tries the primary (Gemini) parser first. On failure or low confidence,
    falls back to the structured (regex) parser.
    """

    def __init__(self, primary: BaseParser, fallback: BaseParser):
        self.primary = primary
        self.fallback = fallback

    async def parse(self, raw: RawJob) -> ParsedJob:
        try:
            result = await self.primary.parse(raw)
            if result.job_title != "[PARSE FAILED]" and result.confidence >= 0.3:
                return result
            logger.info(f"HybridParser: primary gave low confidence ({result.confidence}), trying fallback")
        except Exception as e:
            logger.warning(f"HybridParser: primary failed ({e}), trying fallback")

        try:
            fallback_result = await self.fallback.parse(raw)
            fallback_result.parse_warnings = list(fallback_result.parse_warnings or [])
            fallback_result.parse_warnings.append("LLM unavailable — used structured fallback")
            return fallback_result
        except Exception as e:
            logger.error(f"HybridParser: fallback also failed: {e}")
            raise

    async def batch_parse(
        self, raws: list[RawJob], progress_callback=None
    ) -> list[ParsedJob]:
        results = []
        for i, raw in enumerate(raws):
            results.append(await self.parse(raw))
            if progress_callback:
                try:
                    progress_callback(i + 1, len(raws))
                except Exception:
                    pass
        return results
