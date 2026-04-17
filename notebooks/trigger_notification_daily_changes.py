# Databricks notebook source
# Notebook: trigger_notification_daily_changes
# Purpose: Call the Iona CSM FastAPI after daily health scores (and related data) are fresh.
# Schedule: After compute_health_scores (daily snapshot) in the same job or a dependent job.
# Full documentation: docs/notifications.md
#
# Production: omit IONA_NOTIFICATION_TEST_RECIPIENT so real CSMs receive mail.
# Testing: set IONA_NOTIFICATION_TEST_RECIPIENT to one or more *@ifs.com addresses
#   (comma or semicolon separated) — same combined digest is sent to each inbox only.
#
# Auth: Databricks Apps reject raw PATs — OIDC token exchange is done automatically.
# PAT source order: DATABRICKS_TOKEN env → secret iona/IONA_APP_HTTP_PAT (recommended for Serverless)
#   → IONA_HTTP_AUTH_SECRET_SCOPE + IONA_HTTP_AUTH_SECRET_KEY widgets.

import os
import requests


def _ensure_widget(name: str, default: str = "") -> None:
    try:
        dbutils.widgets.text(name, default)
    except Exception:
        pass


_ensure_widget("IONA_CSM_APP_URL", "")
_ensure_widget("IONA_NOTIFICATION_TEST_RECIPIENT", "")
_ensure_widget("IONA_HTTP_AUTH_SECRET_SCOPE", "")
_ensure_widget("IONA_HTTP_AUTH_SECRET_KEY", "")
_ensure_widget("DATABRICKS_HOST", "")
_ensure_widget("IONA_CSM_APP_NAME", "")


def _cfg(widget_key: str, env_key: str, default: str = "") -> str:
    w = dbutils.widgets.get(widget_key).strip()
    if w:
        return w
    return (os.environ.get(env_key, default) or "").strip()


def _get_workspace_bearer_token() -> str:
    """Bearer token for Authorization: Bearer … when calling the Databricks App URL."""
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
        "Option A — secret: databricks secrets put-secret iona IONA_APP_HTTP_PAT --string-value <workspace PAT> "
        "(PAT user must access the app). Option B — set job env DATABRICKS_TOKEN. "
        "Option C — task Parameters IONA_HTTP_AUTH_SECRET_SCOPE + IONA_HTTP_AUTH_SECRET_KEY."
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
    """Exchange a workspace PAT for an audience-scoped token accepted by Databricks Apps."""
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
    """PAT → app-scoped OAuth Bearer (required for *.databricksapps.com API calls)."""
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


APP_URL = (
    _cfg("IONA_CSM_APP_URL", "IONA_CSM_APP_URL", "https://iona-cx-1057997375544232.aws.databricksapps.com")
).rstrip("/")
TEST_RECIPIENT = _cfg("IONA_NOTIFICATION_TEST_RECIPIENT", "IONA_NOTIFICATION_TEST_RECIPIENT")

token = _get_app_api_bearer_token()
print("Obtained app-scoped OAuth token for Databricks Apps request.")

params = {}
if TEST_RECIPIENT:
    params["test_recipient"] = TEST_RECIPIENT

url = f"{APP_URL}/api/notifications/trigger/daily-changes"
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
            " This path is registered in current repo code; 404 usually means the Databricks App "
            "deployment is older than the notifications API — re-upload `app/` (including "
            "`api/notifications.py` and `api/__init__.py`) and click Deploy on the app."
        )
    raise RuntimeError(f"Daily notification trigger failed: {body}{hint}")

if not result.get("success"):
    raise RuntimeError(f"Daily emails had failures: {result.get('errors')}")

print("✓ Daily notification trigger completed successfully.")
