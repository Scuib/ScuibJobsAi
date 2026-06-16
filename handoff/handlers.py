"""
handoff/handlers.py

Concrete BaseHandoff implementations.
HTTPHandoff: POST to Dozie's algorithm endpoint.
MockHandoff: for testing / Phase 1 dry runs.
"""

import json
import logging
from datetime import datetime
import httpx

from core.interfaces import BaseHandoff
from core.models import ValidatedJob, HandoffPayload

logger = logging.getLogger(__name__)


class HTTPHandoff(BaseHandoff):
    """
    POSTs the validated job payload to Dozie's algorithm endpoint.
    Handles retries and auth headers.
    """

    def __init__(
        self,
        endpoint_url: str,
        api_key: str | None = None,
        timeout: int = 30,
        max_retries: int = 3,
    ):
        self.endpoint_url = endpoint_url
        self.timeout = timeout
        self.max_retries = max_retries
        self._headers = {"Content-Type": "application/json"}
        if api_key:
            self._headers["Authorization"] = f"Bearer {api_key}"

    async def send(self, job: ValidatedJob) -> HandoffPayload:
        payload = HandoffPayload.from_validated(job)
        body = payload.model_dump(mode="json")

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            for attempt in range(1, self.max_retries + 1):
                try:
                    response = await client.post(
                        self.endpoint_url,
                        json=body,
                        headers=self._headers,
                    )
                    response.raise_for_status()
                    logger.info(f"Handoff succeeded for job {payload.job_id} (attempt {attempt})")
                    return payload
                except httpx.HTTPStatusError as e:
                    if attempt == self.max_retries or e.response.status_code < 500:
                        logger.error(f"Handoff HTTP error {e.response.status_code} for {payload.job_id}: {e}")
                        raise
                    logger.warning(f"Handoff attempt {attempt} failed, retrying...")
                except httpx.RequestError as e:
                    if attempt == self.max_retries:
                        raise
                    logger.warning(f"Handoff network error attempt {attempt}: {e}")

        return payload  # Unreachable but satisfies return type

    async def health_check(self) -> bool:
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                r = await client.get(self.endpoint_url.rstrip("/") + "/health")
                return r.status_code < 500
        except Exception:
            return False


class MockHandoff(BaseHandoff):
    """
    Phase 1 / testing handoff. Logs the payload instead of sending it.
    Stores sent jobs in memory for test assertions.
    """

    def __init__(self):
        self.sent_jobs: list[HandoffPayload] = []

    async def send(self, job: ValidatedJob) -> HandoffPayload:
        payload = HandoffPayload.from_validated(job)
        self.sent_jobs.append(payload)
        logger.info(f"[MockHandoff] Would send job: {payload.model_dump_json(indent=2)}")
        return payload

    async def health_check(self) -> bool:
        return True


class FileHandoff(BaseHandoff):
    """
    Writes payloads as JSON lines to a file.
    Useful for local dev, batch exports, or when Dozie's endpoint isn't ready.
    """

    def __init__(self, output_path: str):
        self.output_path = output_path

    async def send(self, job: ValidatedJob) -> HandoffPayload:
        payload = HandoffPayload.from_validated(job)
        with open(self.output_path, "a") as f:
            f.write(payload.model_dump_json() + "\n")
        logger.info(f"[FileHandoff] Wrote job {payload.job_id} to {self.output_path}")
        return payload

    async def health_check(self) -> bool:
        try:
            with open(self.output_path, "a"):
                return True
        except OSError:
            return False
