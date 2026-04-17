"""
Notification service: builds and dispatches weekly summary and daily change emails.

Entry points
------------
send_daily_changes(test_recipients)
    Detects today's health, support, usage, Gong, and renewal changes across all accounts
    and sends one digest email per CSM (plus global daily subscribers). In test mode, the
    same combined digest goes to every address in test_recipients instead.

send_weekly_summaries(week_start, test_recipients, weekly_delivery, ...)
    Sends weekly account narratives in one of three delivery modes:
      per_account      — one email per account → CSM owner + global weekly subscribers
      per_csm          — one portfolio digest per CSM with all their accounts
      department_digest — one email per digest_recipients address, accounts grouped by department
    In test mode, all mail is redirected to test_recipients (*@ifs.com only).

Internal flow
-------------
  1. Query Databricks for data (accounts, health, summaries, changes).
  2. Determine recipients (CSM owner + global recipients from notification_recipients table).
  3. Check per-user opt-out preferences.
  4. Render Jinja2 HTML templates (backend/app/templates/).
  5. Send via EmailService (SendGrid v3).

See docs/notifications.md for the full reference.
"""

from __future__ import annotations

import logging
import os
import random
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
import re
from typing import Dict, List, Optional, Tuple

from jinja2 import Environment, FileSystemLoader, select_autoescape

from .databricks import DatabricksService
from .email_service import EmailMessage, EmailRecipient, EmailService, SendResult
from ..config import Settings

logger = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).parent.parent / "templates"


def _build_jinja_env() -> Environment:
    return Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        autoescape=select_autoescape(["html"]),
    )


def _plain_from_html(html: str) -> str:
    """Very simple HTML → plain-text strip (good enough for email fallback)."""
    text = re.sub(r"<[^>]+>", "", html)
    text = re.sub(r"[ \t]{2,}", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class NotificationService:
    """
    Orchestrates weekly summary and daily change email campaigns.

    Public methods: send_weekly_summaries(), send_daily_changes(), send_test_email().
    All other methods are private helpers prefixed with _.
    """

    def __init__(self, db: DatabricksService, email_svc: EmailService, settings: Settings) -> None:
        self.db = db
        self.email_svc = email_svc
        self.settings = settings
        self.jinja = _build_jinja_env()

    # ------------------------------------------------------------------
    # Weekly Summary
    # ------------------------------------------------------------------

    @staticmethod
    def _is_ifs_test_email(email: str) -> bool:
        """Restrict test-only sends to corporate IFS addresses."""
        e = (email or "").strip().lower()
        return bool(re.match(r"^.+@ifs\.com$", e))

    @staticmethod
    def _parse_digest_recipients(raw: Optional[str]) -> List[str]:
        """Parse a comma-separated *@ifs.com recipient list into a clean list of addresses."""
        if not raw:
            return []
        return [e.strip().lower() for e in raw.split(",") if e.strip()]

    # ------------------------------------------------------------------
    # Weekly Summary — per_account (default, original behaviour)
    # ------------------------------------------------------------------

    async def _send_weekly_per_account(
        self,
        summaries: list,
        health_map: dict,
        csm_map: dict,
        test_recipients: Optional[List[str]],
        prefix: str,
    ) -> dict:
        global_recipients = [] if test_recipients else self.db.get_notification_recipients(
            active_only=True, receive_all_weekly=True
        )
        total_sent = total_failed = total_skipped = 0
        errors: List[str] = []

        for summary in summaries:
            account_id = summary["account_id"]
            health = health_map.get(account_id, {})
            csm_info = csm_map.get(account_id, {})

            ctx = {
                "account_name": summary["account_name"],
                "week_start": summary["week_start"],
                "week_end": summary["week_end"],
                "narrative": summary["narrative"],
                "gong_summary": summary["gong_summary"],
                "health_score": health.get("score", 0),
                "health_category": health.get("category", "Unknown"),
                "renewal_days": health.get("renewal_days"),
                "total_arr": health.get("total_arr", 0),
                "nearest_renewal_arr": health.get("nearest_renewal_arr", 0),
            }

            if test_recipients:
                to_send = [EmailRecipient(email=e, name="Test") for e in test_recipients]
            else:
                pool: List[Tuple[str, Optional[str]]] = []
                csm_email = csm_info.get("csm_email")
                if csm_email:
                    pool.append((csm_email, csm_info.get("csm_name")))
                for gr in global_recipients:
                    e = gr["user_email"]
                    if e not in [r[0] for r in pool]:
                        pool.append((e, gr.get("user_name")))
                if not pool:
                    total_skipped += 1
                    continue
                to_send = []
                for e, n in pool:
                    prefs = self.db.get_user_notification_preferences(e)
                    if prefs.get("notification.weekly_summary", "enabled") == "disabled":
                        total_skipped += 1
                        continue
                    to_send.append(EmailRecipient(email=e, name=n))
                if not to_send:
                    continue

            subject = (
                f"{prefix}Weekly Summary: {summary['account_name']} "
                f"({ctx['health_category']} \u00b7 {summary['week_start']})"
            )
            html = self.jinja.get_template("weekly_summary.html").render(**ctx)
            r = await self.email_svc.send_async(
                EmailMessage(to=to_send, subject=subject, html_body=html, plain_body=_plain_from_html(html))
            )
            if r.success:
                total_sent += len(to_send)
            else:
                total_failed += len(to_send)
                errors.append(f"{summary['account_name']}: {r.error}")

        return {"sent": total_sent, "failed": total_failed, "skipped": total_skipped, "errors": errors}

    # ------------------------------------------------------------------
    # Weekly Summary — per_csm (one portfolio digest per CSM)
    # ------------------------------------------------------------------

    async def _send_weekly_per_csm(
        self,
        summaries: list,
        health_map: dict,
        csm_map: dict,
        test_recipients: Optional[List[str]],
        prefix: str,
    ) -> dict:
        # Group account summaries by csm_email
        csm_accounts: Dict[str, List[dict]] = defaultdict(list)
        csm_names: Dict[str, str] = {}

        for summary in summaries:
            account_id = summary["account_id"]
            health = health_map.get(account_id, {})
            csm_info = csm_map.get(account_id, {})
            csm_email = csm_info.get("csm_email")
            if not csm_email:
                continue
            csm_names[csm_email] = csm_info.get("csm_name", "")
            csm_accounts[csm_email].append({
                "account_name": summary["account_name"],
                "week_start": summary["week_start"],
                "week_end": summary["week_end"],
                "narrative": summary["narrative"],
                "gong_summary": summary["gong_summary"],
                "health_score": health.get("score", 0),
                "health_category": health.get("category", "Unknown"),
                "renewal_days": health.get("renewal_days"),
                "total_arr": health.get("total_arr", 0),
                "nearest_renewal_arr": health.get("nearest_renewal_arr", 0),
            })

        if not csm_accounts:
            return {"sent": 0, "failed": 0, "skipped": len(summaries), "errors": []}

        # Resolve week dates from first summary
        first = summaries[0]
        week_start = first["week_start"]
        week_end = first["week_end"]

        if test_recipients:
            all_accounts = [acct for accts in csm_accounts.values() for acct in accts]
            groups = [("", "Test", all_accounts)]
        else:
            groups = [
                (email, csm_names.get(email, ""), accounts)
                for email, accounts in csm_accounts.items()
            ]

        total_sent = total_failed = total_skipped = 0
        errors: List[str] = []

        for email, name, accounts in groups:
            if not test_recipients:
                prefs = self.db.get_user_notification_preferences(email)
                if prefs.get("notification.weekly_summary", "enabled") == "disabled":
                    total_skipped += len(accounts)
                    continue

            subject = (
                f"{prefix}Weekly Portfolio Digest — {name or email} "
                f"({len(accounts)} account{'s' if len(accounts) != 1 else ''} \u00b7 {week_start})"
                if not test_recipients
                else f"{prefix}Weekly Digest (all CSMs \u00b7 {week_start})"
            )
            ctx = {
                "csm_name": name or email,
                "week_start": week_start,
                "week_end": week_end,
                "accounts": accounts,
                "test_mode": bool(test_recipients),
            }
            html = self.jinja.get_template("weekly_summary_csm_digest.html").render(**ctx)
            if test_recipients:
                to = [EmailRecipient(email=e, name="Test") for e in test_recipients]
            else:
                to = [EmailRecipient(email=email, name=name or None)]
            r = await self.email_svc.send_async(
                EmailMessage(
                    to=to,
                    subject=subject,
                    html_body=html,
                    plain_body=_plain_from_html(html),
                )
            )
            if r.success:
                total_sent += len(to)
            else:
                total_failed += len(to)
                err_who = ", ".join(test_recipients) if test_recipients else (email or "")
                errors.append(f"{err_who}: {r.error}")

        return {"sent": total_sent, "failed": total_failed, "skipped": total_skipped, "errors": errors}

    # ------------------------------------------------------------------
    # Weekly Summary — department_digest (one email per digest_recipient)
    # ------------------------------------------------------------------

    async def _send_weekly_department_digest(
        self,
        summaries: list,
        health_map: dict,
        csm_map: dict,
        digest_recipients: List[str],
        test_recipients: Optional[List[str]],
        prefix: str,
    ) -> dict:
        first = summaries[0]
        week_start, week_end = first["week_start"], first["week_end"]

        # Build flat list of account rows enriched with health + CSM info
        rows = []
        for summary in summaries:
            account_id = summary["account_id"]
            health = health_map.get(account_id, {})
            csm_info = csm_map.get(account_id, {})
            rows.append({
                "account_name": summary["account_name"],
                "narrative": summary["narrative"],
                "gong_summary": summary["gong_summary"],
                "health_score": health.get("score", 0),
                "health_category": health.get("category", "Unknown"),
                "renewal_days": health.get("renewal_days"),
                "total_arr": health.get("total_arr", 0),
                "csm_name": csm_info.get("csm_name", "Unassigned"),
                "csm_department": csm_info.get("csm_department") or "Unknown",
            })

        # Group and sort by department, then account name
        by_dept: Dict[str, list] = defaultdict(list)
        for row in rows:
            by_dept[row["csm_department"]].append(row)
        departments = [
            {"name": dept, "accounts": sorted(accts, key=lambda a: a["account_name"])}
            for dept, accts in sorted(by_dept.items())
        ]

        good_count = sum(1 for r in rows if r["health_category"] == "Good")
        at_risk_count = sum(1 for r in rows if r["health_category"] == "At Risk")
        critical_count = len(rows) - good_count - at_risk_count

        ctx = {
            "week_start": week_start,
            "week_end": week_end,
            "total_accounts": len(rows),
            "departments": departments,
            "good_count": good_count,
            "at_risk_count": at_risk_count,
            "critical_count": critical_count,
            "test_mode": bool(test_recipients),
        }
        subject = (
            f"{prefix}Weekly Department Digest — {len(rows)} accounts \u00b7 {week_start}"
        )
        html = self.jinja.get_template("weekly_summary_department_digest.html").render(**ctx)
        plain = _plain_from_html(html)

        send_to = list(test_recipients) if test_recipients else digest_recipients
        total_sent = total_failed = 0
        errors: List[str] = []

        for email in send_to:
            r = await self.email_svc.send_async(
                EmailMessage(
                    to=[EmailRecipient(email=email)],
                    subject=subject,
                    html_body=html,
                    plain_body=plain,
                )
            )
            if r.success:
                total_sent += 1
            else:
                total_failed += 1
                errors.append(f"{email}: {r.error}")

        return {"sent": total_sent, "failed": total_failed, "skipped": 0, "errors": errors}

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def send_weekly_summaries(
        self,
        week_start: Optional[str] = None,
        test_recipients: Optional[List[str]] = None,
        weekly_delivery: str = "per_account",
        digest_recipients: Optional[str] = None,
        test_single_account: bool = False,
        test_account_pick: str = "first",
    ) -> dict:
        """
        Dispatch weekly summary emails.

        weekly_delivery:
          - ``per_account``        (default) — one email per account → CSM + global recipients.
          - ``per_csm``            — one portfolio digest per CSM; with test_recipients, one
                                     combined email to each tester covering all CSMs.
          - ``department_digest``  — one email per ``digest_recipients`` address, all accounts
                                     grouped by CSM department; requires digest_recipients.

        test_recipients: list of *@ifs.com addresses; overrides real recipients so no real users are mailed.
        digest_recipients: comma-separated *@ifs.com list; required for department_digest.
        test_single_account: If True with ``test_recipients`` and ``per_account``, send only one
            weekly mail (first account or random per ``test_account_pick``) instead of one per account.
        test_account_pick: ``first`` (default) or ``random`` — only used when test_single_account applies.
        """
        logger.info(
            "send_weekly_summaries: starting delivery=%s test_recipients=%s test_single=%s pick=%s",
            weekly_delivery, test_recipients, test_single_account, test_account_pick,
        )

        test_recipient_str = ", ".join(test_recipients) if test_recipients else None

        VALID_MODES = {"per_account", "per_csm", "department_digest"}
        if weekly_delivery not in VALID_MODES:
            return {
                "emails_sent": 0, "emails_failed": 0, "skipped": 0,
                "errors": [f"Invalid weekly_delivery '{weekly_delivery}'; must be one of {sorted(VALID_MODES)}"],
                "detail": "Invalid weekly_delivery parameter",
                "test_mode": bool(test_recipients), "test_recipient": test_recipient_str,
            }

        parsed_digest = self._parse_digest_recipients(digest_recipients)
        if weekly_delivery == "department_digest" and not parsed_digest and not test_recipients:
            return {
                "emails_sent": 0, "emails_failed": 0, "skipped": 0,
                "errors": ["digest_recipients is required for weekly_delivery=department_digest"],
                "detail": "Missing digest_recipients",
                "test_mode": bool(test_recipients), "test_recipient": test_recipient_str,
            }

        summaries = self.db.get_weekly_summaries_for_notification(week_start=week_start)
        if not summaries:
            logger.warning("send_weekly_summaries: no summaries found")
            return {
                "emails_sent": 0, "emails_failed": 0, "skipped": 0, "errors": [],
                "detail": "No summaries available",
                "test_mode": bool(test_recipients), "test_recipient": test_recipient_str,
            }

        n_summaries_before_sample = len(summaries)
        sampling_note = ""
        if (
            weekly_delivery == "per_account"
            and test_recipients
            and test_single_account
            and n_summaries_before_sample > 1
        ):
            pick = (test_account_pick or "first").strip().lower()
            if pick not in ("first", "random"):
                pick = "first"
            if pick == "random":
                summaries = [random.choice(summaries)]
            else:
                summaries = [summaries[0]]
            acct_name = summaries[0].get("account_name", "")
            sampling_note = (
                f" test_sample=1/{n_summaries_before_sample} pick={pick} account={acct_name}"
            )
            logger.info(
                "send_weekly_summaries: per_account test_single enabled — one account only (%s)",
                acct_name,
            )

        health_map = self.db.get_latest_health_scores_for_notification()
        accounts_with_csm = self.db.get_accounts_with_csm_emails()
        csm_map: Dict[str, dict] = {a["account_id"]: a for a in accounts_with_csm}
        prefix = "[TEST] " if test_recipients else ""

        if weekly_delivery == "per_csm":
            result = await self._send_weekly_per_csm(summaries, health_map, csm_map, test_recipients, prefix)
        elif weekly_delivery == "department_digest":
            result = await self._send_weekly_department_digest(
                summaries, health_map, csm_map, parsed_digest, test_recipients, prefix
            )
        else:
            result = await self._send_weekly_per_account(summaries, health_map, csm_map, test_recipients, prefix)

        logger.info(
            "send_weekly_summaries[%s]: sent=%d failed=%d skipped=%d",
            weekly_delivery, result["sent"], result["failed"], result["skipped"],
        )
        return {
            "emails_sent": result["sent"],
            "emails_failed": result["failed"],
            "skipped": result["skipped"],
            "errors": result["errors"][:20],
            "detail": (
                f"[{weekly_delivery}] Processed {len(summaries)} account(s)"
                + sampling_note
                + (f" (test \u2192 {test_recipient_str})" if test_recipient_str else "")
            ),
            "test_mode": bool(test_recipients),
            "test_recipient": test_recipient_str,
        }

    # ------------------------------------------------------------------
    # Daily Changes
    # ------------------------------------------------------------------

    async def send_daily_changes(self, test_recipients: Optional[List[str]] = None) -> dict:
        """
        Detect all changes across health, support, usage, Gong, and renewals,
        then send per-recipient digest emails.
        If ``test_recipients`` is set (*@ifs.com), the same combined digest is sent to each address.
        """
        logger.info("send_daily_changes: starting test_recipients=%s", test_recipients)
        test_recipient_str = ", ".join(test_recipients) if test_recipients else None

        # Fetch all change data in parallel (sync DB calls, but we can batch)
        health_changes  = self.db.get_health_score_changes_for_notification()
        support_changes = self.db.get_support_changes_for_notification()
        usage_changes   = self.db.get_pendo_usage_changes_for_notification()
        gong_changes    = self.db.get_gong_changes_for_notification()
        renewal_alerts  = self.db.get_renewal_alerts_for_notification()

        # Build account_id → latest health category (for the account header badge)
        health_map = self.db.get_latest_health_scores_for_notification()

        # Group all changes by account_id
        by_account: Dict[str, dict] = {}

        def _get_or_create(account_id: str, account_name: str) -> dict:
            if account_id not in by_account:
                h = health_map.get(account_id, {})
                by_account[account_id] = {
                    "account_id": account_id,
                    "account_name": account_name,
                    "current_category": h.get("category", ""),
                    "health_changes": [],
                    "support_changes": [],
                    "usage_changes": [],
                    "gong_changes": [],
                    "renewal_alerts": [],
                }
            return by_account[account_id]

        for item in health_changes:
            _get_or_create(item["account_id"], item["account_name"])["health_changes"].append(item)
        for item in support_changes:
            _get_or_create(item["account_id"], item["account_name"])["support_changes"].append(item)
        for item in usage_changes:
            _get_or_create(item["account_id"], item["account_name"])["usage_changes"].append(item)
        for item in gong_changes:
            _get_or_create(item["account_id"], item["account_name"])["gong_changes"].append(item)
        for item in renewal_alerts:
            _get_or_create(item["account_id"], item["account_name"])["renewal_alerts"].append(item)

        if not by_account:
            logger.info("send_daily_changes: no changes detected today")
            return {
                "emails_sent": 0,
                "emails_failed": 0,
                "skipped": 0,
                "errors": [],
                "detail": "No changes detected",
                "test_mode": bool(test_recipients),
                "test_recipient": test_recipient_str,
            }

        if test_recipients:
            all_accounts = list(by_account.values())
            recipient_accounts = {e: all_accounts for e in test_recipients}
            recipient_names = {e: "Test" for e in test_recipients}
        else:
            accounts_with_csm = self.db.get_accounts_with_csm_emails()
            csm_map: Dict[str, dict] = {a["account_id"]: a for a in accounts_with_csm}

            global_recipients = self.db.get_notification_recipients(
                active_only=True, receive_all_daily=True
            )
            global_emails = {gr["user_email"]: gr.get("user_name") for gr in global_recipients}

            recipient_accounts = defaultdict(list)
            recipient_names: Dict[str, Optional[str]] = {}

            for account_id, changes in by_account.items():
                csm_info = csm_map.get(account_id, {})
                csm_email = csm_info.get("csm_email")

                if csm_email:
                    recipient_accounts[csm_email].append(changes)
                    recipient_names[csm_email] = csm_info.get("csm_name")

                for email, name in global_emails.items():
                    if email != csm_email:
                        recipient_accounts[email].append(changes)
                        recipient_names[email] = name

            if not recipient_accounts:
                logger.warning("send_daily_changes: changes detected but no recipients found")
                return {
                    "emails_sent": 0,
                    "emails_failed": 0,
                    "skipped": 0,
                    "errors": [],
                    "detail": "No recipients configured",
                    "test_mode": False,
                    "test_recipient": None,
                }

        total_sent = 0
        total_failed = 0
        total_skipped = 0
        errors: List[str] = []
        report_date = date.today().strftime("%A, %B %-d, %Y") if os.name != "nt" else date.today().strftime("%A, %B %d, %Y")

        # Category preference keys
        PREF_MAP = {
            "health_changes":  "notification.daily_health",
            "support_changes": "notification.daily_support",
            "usage_changes":   "notification.daily_usage",
            "gong_changes":    "notification.daily_gong",
            "renewal_alerts":  "notification.daily_renewal",
        }

        for email, account_list in recipient_accounts.items():
            if test_recipients:
                filtered_accounts = list(account_list)
            else:
                prefs = self.db.get_user_notification_preferences(email)
                filtered_accounts = []
                for acc in account_list:
                    filtered = dict(acc)
                    for field_key, pref_key in PREF_MAP.items():
                        if prefs.get(pref_key, "enabled") == "disabled":
                            filtered[field_key] = []
                    has_any = any(filtered[k] for k in PREF_MAP)
                    if has_any:
                        filtered_accounts.append(filtered)

            if not filtered_accounts:
                total_skipped += 1
                continue

            template_ctx = {
                "report_date": report_date,
                "total_accounts": len(filtered_accounts),
                "accounts": filtered_accounts,
            }

            prefix = "[TEST] " if test_recipients else ""
            subject = (
                f"{prefix}Daily Account Changes — {len(filtered_accounts)} account"
                f"{'s' if len(filtered_accounts) != 1 else ''} ({report_date})"
            )
            html_body = self.jinja.get_template("daily_changes.html").render(**template_ctx)
            plain_body = _plain_from_html(html_body)

            to = [EmailRecipient(email=email, name=recipient_names.get(email))]
            result: SendResult = await self.email_svc.send_async(
                EmailMessage(to=to, subject=subject, html_body=html_body, plain_body=plain_body)
            )
            if result.success:
                total_sent += 1
            else:
                total_failed += 1
                errors.append(f"{email}: {result.error}")

        logger.info(
            "send_daily_changes: sent=%d failed=%d skipped=%d",
            total_sent, total_failed, total_skipped,
        )
        return {
            "emails_sent": total_sent,
            "emails_failed": total_failed,
            "skipped": total_skipped,
            "errors": errors[:20],
            "detail": f"Detected changes in {len(by_account)} accounts, notified {total_sent} recipients"
            + (f" (test → {test_recipient_str})" if test_recipient_str else ""),
            "test_mode": bool(test_recipients),
            "test_recipient": test_recipient_str,
        }

    # ------------------------------------------------------------------
    # Test Email
    # ------------------------------------------------------------------

    async def send_test_email(self, to_email: str, to_name: Optional[str] = None) -> SendResult:
        """Send a verification email to confirm SendGrid is working."""
        from datetime import timezone
        sent_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        template_ctx = {
            "to_email": to_email,
            "from_email": self.settings.sendgrid_from_email,
            "environment": self.settings.environment,
            "sent_at": sent_at,
        }
        html_body = self.jinja.get_template("test_email.html").render(**template_ctx)
        plain_body = _plain_from_html(html_body)
        message = EmailMessage(
            to=[EmailRecipient(email=to_email, name=to_name)],
            subject=f"[{self.settings.environment.upper()}] Iona CSM — SendGrid Integration Test",
            html_body=html_body,
            plain_body=plain_body,
        )
        return await self.email_svc.send_async(message)


def get_notification_service(db: DatabricksService, settings: Settings) -> NotificationService:
    """Factory used by FastAPI dependency injection."""
    email_svc = EmailService(settings)
    return NotificationService(db=db, email_svc=email_svc, settings=settings)
