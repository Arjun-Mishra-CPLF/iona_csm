"""Notifications API: trigger email campaigns and manage notification recipients."""

import logging
import re
from typing import List, Optional, Set

from fastapi import APIRouter, Depends, HTTPException, Query

from ..config import Settings, get_settings
from ..models.schemas import (
    NotificationRecipient,
    NotificationRecipientCreate,
    NotificationRecipientUpdate,
    TestEmailRequest,
    TriggerEmailResponse,
)
from ..services.databricks import DatabricksService, get_databricks_service
from ..services.notification_service import (
    NotificationService,
    get_notification_service,
)

logger = logging.getLogger(__name__)

router = APIRouter()


def _validate_test_recipients(raw: Optional[str]) -> Optional[List[str]]:
    """
    Parse ``test_recipient`` query value into one or more normalized *@ifs.com addresses.

    Accepts comma- or semicolon-separated lists (whitespace trimmed, duplicates removed).
    """
    if not raw or not str(raw).strip():
        return None
    parts = re.split(r"[,;]+", str(raw))
    out: List[str] = []
    seen: Set[str] = set()
    for p in parts:
        e = p.strip().lower()
        if not e:
            continue
        if not NotificationService._is_ifs_test_email(e):
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Each test_recipient entry must be a valid *@ifs.com address; "
                    f"invalid: {e!r} (owners are never emailed in test mode)."
                ),
            )
        if e not in seen:
            seen.add(e)
            out.append(e)
    return out or None


def _get_notification_service(
    db: DatabricksService = Depends(get_databricks_service),
    settings: Settings = Depends(get_settings),
) -> NotificationService:
    return get_notification_service(db=db, settings=settings)


# ──────────────────────────────────────────────────────────────────────────────
# Trigger endpoints
# ──────────────────────────────────────────────────────────────────────────────

@router.post(
    "/trigger/weekly-summary",
    response_model=TriggerEmailResponse,
    summary="Trigger weekly summary emails",
    description=(
        "Sends weekly account summary emails. Delivery mode is controlled by `weekly_delivery`:\n\n"
        "- **`per_account`** (default) — one email per account to the CSM owner and all global "
        "`receive_all_weekly` recipients. With `test_recipient`, every per-account email is "
        "redirected to those addresses (*@ifs.com; comma- or semicolon-separated for multiple testers). "
        "Use `test_single_account=true` with "
        "`test_recipient` to send **one** sample mail only (`test_account_pick=first` or `random`).\n"
        "- **`per_csm`** — one portfolio digest per CSM with all their accounts in a single email. "
        "With `test_recipient`, one combined email covering all CSMs is sent to each test address.\n"
        "- **`department_digest`** — one email per address in `digest_recipients` (comma-separated "
        "*@ifs.com), listing all accounts grouped by CSM department. With `test_recipient`, the same "
        "body is sent to each test address.\n\n"
        "Designed to be triggered by a Databricks Job each Monday after weekly summary data is ready."
    ),
)
async def trigger_weekly_summary(
    week_start: Optional[str] = Query(
        default=None,
        description="ISO date (YYYY-MM-DD) for the week to send. Defaults to the latest available week.",
    ),
    test_recipient: Optional[str] = Query(
        default=None,
        description=(
            "One or more *@ifs.com addresses (comma or semicolon separated). "
            "Overrides real recipients so no real users are emailed."
        ),
    ),
    weekly_delivery: str = Query(
        default="per_account",
        description=(
            "Email delivery mode: 'per_account' (default, one email per account), "
            "'per_csm' (one portfolio digest per CSM), or "
            "'department_digest' (one email per digest_recipients address, accounts grouped by department)."
        ),
    ),
    digest_recipients: Optional[str] = Query(
        default=None,
        description=(
            "Comma-separated *@ifs.com addresses that receive the department digest. "
            "Required when weekly_delivery=department_digest and test_recipient is not set."
        ),
    ),
    test_single_account: bool = Query(
        default=False,
        description=(
            "When true with test_recipient and weekly_delivery=per_account, send only one weekly "
            "summary email (sample account) instead of one per account. Ignored without test_recipient."
        ),
    ),
    test_account_pick: str = Query(
        default="first",
        description="With test_single_account: 'first' (alphabetically first account) or 'random'.",
    ),
    svc: NotificationService = Depends(_get_notification_service),
) -> TriggerEmailResponse:
    test_to = _validate_test_recipients(test_recipient)
    pick = (test_account_pick or "first").strip().lower()
    if pick not in ("first", "random"):
        raise HTTPException(
            status_code=400,
            detail="test_account_pick must be 'first' or 'random'.",
        )
    logger.info(
        "POST /notifications/trigger/weekly-summary week_start=%s delivery=%s test_recipient=%s test_single=%s pick=%s",
        week_start, weekly_delivery, test_to, test_single_account, pick,
    )
    result = await svc.send_weekly_summaries(
        week_start=week_start,
        test_recipients=test_to,
        weekly_delivery=weekly_delivery,
        digest_recipients=digest_recipients,
        test_single_account=test_single_account,
        test_account_pick=pick,
    )
    return TriggerEmailResponse(
        success=result["emails_failed"] == 0,
        emails_sent=result["emails_sent"],
        emails_failed=result["emails_failed"],
        skipped=result["skipped"],
        errors=result["errors"],
        detail=result["detail"],
        test_mode=result.get("test_mode", False),
        test_recipient=result.get("test_recipient"),
    )


@router.post(
    "/trigger/daily-changes",
    response_model=TriggerEmailResponse,
    summary="Trigger daily account change alerts",
    description=(
        "Detects today's changes across health scores, support tickets, Pendo usage, "
        "Gong activity, and renewal proximity, then sends digest emails. "
        "Designed to be called by a Databricks Job every morning after health score notebook completes. "
        "Optional query `test_recipient=a@ifs.com` or comma-separated `a@ifs.com,b@ifs.com` sends the same "
        "combined digest to each test address (*@ifs.com only)."
    ),
)
async def trigger_daily_changes(
    test_recipient: Optional[str] = Query(
        default=None,
        description="If set (*@ifs.com, comma/semicolon-separated for multiple), same digest to each address.",
    ),
    svc: NotificationService = Depends(_get_notification_service),
) -> TriggerEmailResponse:
    test_to = _validate_test_recipients(test_recipient)
    logger.info("POST /notifications/trigger/daily-changes test_recipient=%s", test_to)
    result = await svc.send_daily_changes(test_recipients=test_to)
    return TriggerEmailResponse(
        success=result["emails_failed"] == 0,
        emails_sent=result["emails_sent"],
        emails_failed=result["emails_failed"],
        skipped=result["skipped"],
        errors=result["errors"],
        detail=result["detail"],
        test_mode=result.get("test_mode", False),
        test_recipient=result.get("test_recipient"),
    )


@router.post(
    "/test-email",
    response_model=TriggerEmailResponse,
    summary="Send a SendGrid integration test email",
    description="Sends a single test email to verify the SendGrid credentials and template rendering are working.",
)
async def send_test_email(
    body: TestEmailRequest,
    svc: NotificationService = Depends(_get_notification_service),
) -> TriggerEmailResponse:
    logger.info("POST /notifications/test-email to=%s", body.to_email)
    result = await svc.send_test_email(to_email=body.to_email, to_name=body.to_name)
    return TriggerEmailResponse(
        success=result.success,
        emails_sent=1 if result.success else 0,
        emails_failed=0 if result.success else 1,
        errors=[] if result.success else [result.error or "Unknown error"],
        detail=f"Status code: {result.status_code}" if result.status_code else "",
        test_mode=False,
        test_recipient=None,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Notification recipient management
# ──────────────────────────────────────────────────────────────────────────────

@router.get(
    "/recipients",
    response_model=List[NotificationRecipient],
    summary="List notification recipients",
    description="Returns all users enrolled in the bulk notification system.",
)
async def list_recipients(
    active_only: bool = Query(default=True, description="Filter to active recipients only"),
    receive_all_weekly: Optional[bool] = Query(default=None, description="Filter by weekly subscription"),
    receive_all_daily: Optional[bool] = Query(default=None, description="Filter by daily subscription"),
    db: DatabricksService = Depends(get_databricks_service),
) -> List[NotificationRecipient]:
    rows = db.get_notification_recipients(
        active_only=active_only,
        receive_all_weekly=receive_all_weekly,
        receive_all_daily=receive_all_daily,
    )
    return [NotificationRecipient(**r) for r in rows]


@router.post(
    "/recipients",
    response_model=NotificationRecipient,
    status_code=201,
    summary="Add a notification recipient",
)
async def create_recipient(
    body: NotificationRecipientCreate,
    db: DatabricksService = Depends(get_databricks_service),
) -> NotificationRecipient:
    existing = db.get_notification_recipient(body.user_email)
    if existing:
        raise HTTPException(status_code=409, detail=f"Recipient {body.user_email} already exists")

    # Ensure table exists on first write
    db.ensure_notification_recipients_table()

    ok = db.create_notification_recipient(
        user_email=body.user_email,
        user_name=body.user_name,
        role=body.role,
        receive_all_weekly=body.receive_all_weekly,
        receive_all_daily=body.receive_all_daily,
    )
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to create recipient")

    row = db.get_notification_recipient(body.user_email)
    if not row:
        raise HTTPException(status_code=500, detail="Recipient created but could not be retrieved")
    return NotificationRecipient(**row)


@router.put(
    "/recipients/{user_email:path}",
    response_model=NotificationRecipient,
    summary="Update a notification recipient",
)
async def update_recipient(
    user_email: str,
    body: NotificationRecipientUpdate,
    db: DatabricksService = Depends(get_databricks_service),
) -> NotificationRecipient:
    existing = db.get_notification_recipient(user_email)
    if not existing:
        raise HTTPException(status_code=404, detail=f"Recipient {user_email} not found")

    updates = body.model_dump(exclude_none=True)
    ok = db.update_notification_recipient(user_email, updates)
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to update recipient")

    row = db.get_notification_recipient(user_email)
    return NotificationRecipient(**row)  # type: ignore[arg-type]


@router.delete(
    "/recipients/{user_email:path}",
    status_code=204,
    summary="Remove a notification recipient",
)
async def delete_recipient(
    user_email: str,
    db: DatabricksService = Depends(get_databricks_service),
) -> None:
    existing = db.get_notification_recipient(user_email)
    if not existing:
        raise HTTPException(status_code=404, detail=f"Recipient {user_email} not found")

    ok = db.delete_notification_recipient(user_email)
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to delete recipient")


# ──────────────────────────────────────────────────────────────────────────────
# Notification preference keys (informational)
# ──────────────────────────────────────────────────────────────────────────────

@router.get(
    "/preference-keys",
    summary="List available notification preference keys",
    description=(
        "Returns the preference keys users can set via PUT /api/preferences/{key} "
        "to opt out of specific notification categories. Default value is 'enabled'. "
        "Set to 'disabled' to opt out."
    ),
)
async def list_preference_keys() -> dict:
    return {
        "preference_keys": [
            {
                "key": "notification.weekly_summary",
                "description": "Weekly account summary email (sent Mondays)",
                "default": "enabled",
            },
            {
                "key": "notification.daily_health",
                "description": "Daily alert when health score category changes or score moves >10 points",
                "default": "enabled",
            },
            {
                "key": "notification.daily_support",
                "description": "Daily alert for new critical/high support tickets",
                "default": "enabled",
            },
            {
                "key": "notification.daily_usage",
                "description": "Daily alert when Pendo active visitors drop >30% vs 7-day average",
                "default": "enabled",
            },
            {
                "key": "notification.daily_gong",
                "description": "Daily alert for new Gong calls, risk tracker hits, or no-meeting warnings",
                "default": "enabled",
            },
            {
                "key": "notification.daily_renewal",
                "description": "Daily alert when a renewal enters the 30/60/90-day proximity window",
                "default": "enabled",
            },
        ],
        "usage": "PUT /api/preferences/{key} with body {\"value\": \"disabled\"} to opt out.",
    }
