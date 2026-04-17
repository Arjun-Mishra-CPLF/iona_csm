# Databricks notebook source
# Notebook: trigger_notification_weekly_summary
# Purpose: Call the Iona CSM FastAPI after weekly summaries exist in Delta.
# Schedule: After compute_weekly_summaries (+ optional compute_gong_weekly_summaries).
# Full documentation: docs/notifications.md
#
# Delivery modes — IONA_NOTIFICATION_WEEKLY_DELIVERY:
#   per_account        — one email per account → CSM owner + global recipients (default)
#   per_csm            — one portfolio digest per CSM (test mode: one combined email to each tester)
#   department_digest  — one email per IONA_NOTIFICATION_DIGEST_RECIPIENTS address, grouped by dept
#
# Production: omit IONA_NOTIFICATION_TEST_RECIPIENT so real CSMs receive mail.
# Testing:    set IONA_NOTIFICATION_TEST_RECIPIENT (*@ifs.com, comma/semicolon for multiple).
#
# Per-account test mode: IONA_NOTIFICATION_TEST_SINGLE_ACCOUNT=true (default) sends ONE sample
# account email instead of one per account. IONA_NOTIFICATION_TEST_ACCOUNT_PICK: first | random.

import os
import requests

# Job Parameters → widgets; env fallback. Apps URL needs OIDC token exchange (see daily notebook header).
def _ensure_widget(name: str, default: str = "") -> None:
    try:
        dbutils.widgets.text(name, default)
    except Exception:
        pass


_ensure_widget("IONA_CSM_APP_URL", "")
_ensure_widget("IONA_NOTIFICATION_TEST_RECIPIENT", "")
_ensure_widget("IONA_NOTIFICATION_WEEK_START", "")
_ensure_widget("IONA_HTTP_AUTH_SECRET_SCOPE", "")
_ensure_widget("IONA_HTTP_AUTH_SECRET_KEY", "")
_ensure_widget("DATABRICKS_HOST", "")
_ensure_widget("IONA_CSM_APP_NAME", "")
_ensure_widget("IONA_NOTIFICATION_WEEKLY_DELIVERY", "per_account")
_ensure_widget("IONA_NOTIFICATION_DIGEST_RECIPIENTS", "")
_ensure_widget("IONA_NOTIFICATION_TEST_SINGLE_ACCOUNT", "true")
_ensure_widget("IONA_NOTIFICATION_TEST_ACCOUNT_PICK", "first")


def _cfg(widget_key: str, env_key: str, default: str = "") -> str:
    w = dbutils.widgets.get(widget_key).strip()
    if w:
        return w
    return (os.environ.get(env_key, default) or "").strip()


def _truthy(val: str) -> bool:
    return (val or "").strip().lower() in ("1", "true", "yes", "y", "on")


def _get_workspace_bearer_token() -> str:
    """Bearer token for Authorization: Bearer … (Serverless: use PAT secret or DATABRICKS_TOKEN)."""
    t = (os.environ.get("DATABRICKS_TOKEN") or os.environ.get("TOKEN") or "").strip()
    if t:
        return t
    try:
        ctx = dbutils.notebook.getContext()
        if ctx is not None:
            api = ctx.apiToken()
            if api is not None:
                v = api.get()
                if v:
                    return v
    except Exception:
        pass
    try:
        return (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .apiToken()
            .get()
        )
    except Exception:
        pass
    scope = _cfg("IONA_HTTP_AUTH_SECRET_SCOPE", "IONA_HTTP_AUTH_SECRET_SCOPE")
    key = _cfg("IONA_HTTP_AUTH_SECRET_KEY", "IONA_HTTP_AUTH_SECRET_KEY")
    if scope and key:
        try:
            s = dbutils.secrets.get(scope=scope, key=key)
            if s and s.strip():
                return s.strip()
        except Exception:
            pass
    try:
        s = dbutils.secrets.get(scope="iona", key="IONA_APP_HTTP_PAT")
        if s and s.strip():
            return s.strip()
    except Exception:
        pass
    raise RuntimeError(
        "No Databricks bearer token (Serverless has no notebook getContext). "
        "Put a workspace PAT in secret iona/IONA_APP_HTTP_PAT or set DATABRICKS_TOKEN on the job."
    )


def _workspace_host() -> str:
    h = _cfg(
        "DATABRICKS_HOST",
        "DATABRICKS_HOST",
        "https://dbc-97a2feb3-3e52.cloud.databricks.com",
    ).strip().rstrip("/")
    if not h.lower().startswith("http"):
        h = "https://" + h.lstrip("/")
    return h


def _oidc_exchange_for_app_token(workspace_host: str, subject_pat: str, app_oauth_client_id: str) -> str:
    url = f"{workspace_host}/oidc/v1/token"
    data = {
        "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
        "subject_token": subject_pat,
        "subject_token_type": "urn:databricks:params:oauth:token-type:personal-access-token",
        "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
        "scope": "all-apis",
        "audience": app_oauth_client_id,
    }
    r = requests.post(url, data=data, timeout=120)
    if r.status_code != 200:
        raise RuntimeError(f"OIDC token exchange failed HTTP {r.status_code}: {r.text[:1200]}")
    js = r.json()
    tok = js.get("access_token")
    if not tok:
        raise RuntimeError(f"OIDC token exchange returned no access_token: {js}")
    return tok


def _get_app_api_bearer_token() -> str:
    pat = _get_workspace_bearer_token()
    host = _workspace_host()
    app_name = _cfg("IONA_CSM_APP_NAME", "IONA_CSM_APP_NAME", "iona-cx")
    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient(host=host, token=pat)
        app = w.apps.get(app_name)
        audience = app.oauth2_app_client_id
        if not audience:
            raise RuntimeError(f"App '{app_name}' has no oauth2_app_client_id")
        return _oidc_exchange_for_app_token(host, pat, audience)
    except ImportError as e:
        raise RuntimeError(
            "databricks-sdk is required for app token exchange. In the notebook: %pip install databricks-sdk"
        ) from e


# ═══ CONFIG ═══
APP_URL = (
    _cfg("IONA_CSM_APP_URL", "IONA_CSM_APP_URL", "https://iona-cx-1057997375544232.aws.databricksapps.com")
).rstrip("/")
# *@ifs.com only — overrides all real recipients; leave empty for production
TEST_RECIPIENT = _cfg("IONA_NOTIFICATION_TEST_RECIPIENT", "IONA_NOTIFICATION_TEST_RECIPIENT")
# Optional ISO week_start (YYYY-MM-DD); empty = latest week in table
WEEK_START = _cfg("IONA_NOTIFICATION_WEEK_START", "IONA_NOTIFICATION_WEEK_START")
# per_account | per_csm | department_digest
WEEKLY_DELIVERY = _cfg("IONA_NOTIFICATION_WEEKLY_DELIVERY", "IONA_NOTIFICATION_WEEKLY_DELIVERY", "per_account")
# Required for department_digest (comma-separated *@ifs.com); ignored for other modes
DIGEST_RECIPIENTS = _cfg("IONA_NOTIFICATION_DIGEST_RECIPIENTS", "IONA_NOTIFICATION_DIGEST_RECIPIENTS")
TEST_SINGLE_ACCOUNT = _cfg("IONA_NOTIFICATION_TEST_SINGLE_ACCOUNT", "IONA_NOTIFICATION_TEST_SINGLE_ACCOUNT", "true")
TEST_ACCOUNT_PICK = _cfg("IONA_NOTIFICATION_TEST_ACCOUNT_PICK", "IONA_NOTIFICATION_TEST_ACCOUNT_PICK", "first")

token = _get_app_api_bearer_token()
print("Obtained app-scoped OAuth token for Databricks Apps request.")

params = {}
if WEEK_START:
    params["week_start"] = WEEK_START
if TEST_RECIPIENT:
    params["test_recipient"] = TEST_RECIPIENT
if WEEKLY_DELIVERY and WEEKLY_DELIVERY != "per_account":
    params["weekly_delivery"] = WEEKLY_DELIVERY
if DIGEST_RECIPIENTS:
    params["digest_recipients"] = DIGEST_RECIPIENTS
# One sample weekly mail when testing per_account (avoids N emails to tester)
if TEST_RECIPIENT and WEEKLY_DELIVERY == "per_account" and _truthy(TEST_SINGLE_ACCOUNT):
    params["test_single_account"] = True
    pick = (TEST_ACCOUNT_PICK or "first").strip().lower()
    if pick == "random":
        params["test_account_pick"] = "random"

url = f"{APP_URL}/api/notifications/trigger/weekly-summary"
print(f"POST {url} params={params}")

response = requests.post(
    url,
    headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    },
    params=params,
    timeout=600,
)

print(f"HTTP {response.status_code}")
try:
    result = response.json()
except Exception:
    result = {}
print(result)

if response.status_code != 200:
    body = response.text[:500]
    hint = ""
    if response.status_code == 404:
        hint = (
            " This path exists in current app code; 404 usually means an outdated app deploy — "
            "re-upload `app/` and redeploy the Databricks App."
        )
    raise RuntimeError(f"Weekly notification trigger failed: {body}{hint}")

if not result.get("success"):
    raise RuntimeError(f"Weekly emails had failures: {result.get('errors')}")

print("✓ Weekly notification trigger completed successfully.")
