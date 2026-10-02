"""SendGrid email service using the v3 Web API via httpx."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import List, Optional

import httpx

from ..config import Settings

logger = logging.getLogger(__name__)

SENDGRID_API_URL = "https://api.sendgrid.com/v3/mail/send"
_MAX_RETRIES = 3
_RETRY_BACKOFF_BASE = 2  # seconds


@dataclass
class EmailRecipient:
    email: str
    name: Optional[str] = None


@dataclass
class EmailMessage:
    to: List[EmailRecipient]
    subject: str
    html_body: str
    plain_body: str
    reply_to: Optional[EmailRecipient] = None


@dataclass
class SendResult:
    success: bool
    status_code: int = 0
    error: Optional[str] = None
    recipients: List[str] = field(default_factory=list)


class EmailService:
    """Sends transactional email via the SendGrid v3 Web API."""

    def __init__(self, settings: Settings) -> None:
        self.api_key = settings.sendgrid_api_key
        self.from_email = settings.sendgrid_from_email
        self.from_name = settings.sendgrid_from_name

    def is_configured(self) -> bool:
        return bool(self.api_key and self.from_email)

    def _build_payload(self, message: EmailMessage) -> dict:
        personalizations = [
            {
                "to": [
                    {"email": r.email, **({"name": r.name} if r.name else {})}
                    for r in message.to
                ]
            }
        ]
        payload: dict = {
            "personalizations": personalizations,
            "from": {
                "email": self.from_email,
                **({"name": self.from_name} if self.from_name else {}),
            },
            "subject": message.subject,
            "content": [
                {"type": "text/plain", "value": message.plain_body},
                {"type": "text/html", "value": message.html_body},
            ],
        }
        if message.reply_to:
            payload["reply_to"] = {
                "email": message.reply_to.email,
                **({"name": message.reply_to.name} if message.reply_to.name else {}),
            }
        return payload

    async def send_async(self, message: EmailMessage) -> SendResult:
        """Send an email asynchronously with exponential-backoff retries."""
        if not self.is_configured():
            logger.warning("SendGrid is not configured (SENDGRID_API_KEY missing). Email skipped.")
            return SendResult(success=False, error="SendGrid not configured")

        recipients = [r.email for r in message.to]
        payload = self._build_payload(message)
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        last_error: Optional[str] = None
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    response = await client.post(SENDGRID_API_URL, json=payload, headers=headers)

                if response.status_code in (200, 202):
                    logger.info(
                        "Email sent successfully via SendGrid | subject=%r | to=%s",
                        message.subject,
                        recipients,
                    )
                    return SendResult(success=True, status_code=response.status_code, recipients=recipients)

                if response.status_code == 401:
                    logger.error("SendGrid 401 Unauthorized — check SENDGRID_API_KEY and Mail Send permission")
                    return SendResult(
                        success=False,
                        status_code=401,
                        error="Unauthorized — invalid or missing API key",
                        recipients=recipients,
                    )

                # 4xx (except 429) are permanent failures — don't retry
                if 400 <= response.status_code < 500 and response.status_code != 429:
                    body = response.text[:500]
                    logger.error("SendGrid %d error | body=%s", response.status_code, body)
                    return SendResult(
                        success=False,
                        status_code=response.status_code,
                        error=body,
                        recipients=recipients,
                    )

                # 5xx or 429 — retry
                last_error = f"HTTP {response.status_code}: {response.text[:200]}"
                logger.warning(
                    "SendGrid transient error (attempt %d/%d): %s",
                    attempt,
                    _MAX_RETRIES,
                    last_error,
                )

            except httpx.TimeoutException as exc:
                last_error = f"Timeout: {exc}"
                logger.warning("SendGrid timeout (attempt %d/%d): %s", attempt, _MAX_RETRIES, exc)
            except httpx.RequestError as exc:
                last_error = f"Request error: {exc}"
                logger.warning("SendGrid request error (attempt %d/%d): %s", attempt, _MAX_RETRIES, exc)

            if attempt < _MAX_RETRIES:
                await asyncio.sleep(_RETRY_BACKOFF_BASE ** attempt)

        logger.error("SendGrid: all %d attempts failed. Last error: %s", _MAX_RETRIES, last_error)
        return SendResult(success=False, error=last_error, recipients=recipients)

    def send(self, message: EmailMessage) -> SendResult:
        """Synchronous wrapper around send_async (for use in non-async contexts)."""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # We're inside an async context — return a coroutine that callers must await
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    future = pool.submit(asyncio.run, self.send_async(message))
                    return future.result()
            else:
                return loop.run_until_complete(self.send_async(message))
        except RuntimeError:
            return asyncio.run(self.send_async(message))
