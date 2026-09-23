"""Databricks SQL connector service."""

import logging
import os
import re
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from typing import Any, Generator, List, Optional, Tuple

from ..config import Settings, get_settings
from ..models.schemas import (
    Account,
    AccountDetail,
    AccountFullDetail,
    GongActivityAnalysis,
    GongCallSummary,
    GongTrackerSignal,
    AccountListResponse,
    AccountStatus,
    AccountWithCSM,
    AccountWithCSMListResponse,
    ActionAlert,
    RenewalInfo,
    BenchmarkData,
    BenchmarkMetric,
    ChangeEvent,
    ContractContext,
    ContractEvent,
    ContractGroup,
    LuminanceDocument,
    ContributingFactor,
    ConfluenceImplementationResponse,
    CSM,
    CSMListResponse,
    CSMStats,
    CustomerEvent,
    CustomerGrowthResponse,
    CustomerGrowthSummary,
    ExpansionOpportunity,
    FeatureAdoption,
    PendoDailyMetric,
    PendoTabData,
    PendoUsageSummary,
    HealthBreakdown,
    HealthDistribution,
    HealthScore,
    HealthScoreFactor,
    HealthScoreDetail,
    RenewalContractLine,
    RenewalHealthInsightResponse,
    HumanNote,
    MeetingBrief,
    MetricsSummary,
    MonthlyGrowthPoint,
    ProductWhitespace,
    RecommendedAction,
    RiskAssessment,
    RiskFactor,
    SentimentAnalysis,
    SentimentInteraction,
    SentimentSource,
    Signal,
    SignalDetail,
    SupportAnalysis,
    SupportTicket,
    Task,
    TaskCreate,
    TicketTheme,
    ResolutionStats,
    ResolutionBucket,
    UsageAnalysis,
    UsageTrend,
    ValueGoal,
    ValueRealization,
    WhitespaceAnalysis,
)

logger = logging.getLogger(__name__)

# Initialize Databricks SDK Config for automatic credential management
_databricks_cfg = None

def get_databricks_config():
    """Get or create Databricks SDK Config instance."""
    global _databricks_cfg
    if _databricks_cfg is None:
        try:
            from databricks.sdk.core import Config
            _databricks_cfg = Config()
            logger.info(f"Databricks SDK Config initialized. Host: {_databricks_cfg.host}")
        except Exception as e:
            logger.warning(f"Could not initialize Databricks SDK Config: {e}")
            _databricks_cfg = None
    return _databricks_cfg


def calculate_search_score(name: str, pattern: str) -> int:
    """
    Calculate a search relevance score for ranking results.
    
    Scoring:
    - Exact match (case-insensitive): 100 points
    - Starts with pattern: 80 points
    - Contains as whole word: 60 points
    - Contains pattern anywhere: 40 points
    - Regex match: 30 points
    - No match: 0 points
    """
    if not name or not pattern:
        return 0

    name_lower = name.lower()
    pattern_lower = pattern.lower()

    # Exact match
    if name_lower == pattern_lower:
        return 100

    # Starts with pattern
    if name_lower.startswith(pattern_lower):
        return 80

    # Contains as whole word (word boundary match)
    try:
        if re.search(rf'\b{re.escape(pattern_lower)}\b', name_lower):
            return 60
    except re.error:
        pass

    # Contains pattern anywhere (substring)
    if pattern_lower in name_lower:
        return 40

    # Try regex match (if pattern is valid regex)
    try:
        if re.search(pattern, name, re.IGNORECASE):
            return 30
    except re.error:
        pass

    return 0


def matches_search(name: str, pattern: str) -> bool:
    """
    Check if a name matches the search pattern.
    Supports both simple substring matching and regex patterns.
    """
    if not pattern:
        return True
    if not name:
        return False

    # First try simple substring match (case-insensitive)
    if pattern.lower() in name.lower():
        return True

    # Then try regex match
    try:
        if re.search(pattern, name, re.IGNORECASE):
            return True
    except re.error:
        # Invalid regex, already tried substring match
        pass

    return False

# Table configuration
DIM_CUSTOMERS_TABLE = "silver.silver_layer.dim_customers"
GONG_CALL_TABLE = "silver.silver_layer.fct_gong_call"
GONG_BRIDGE_TABLE = "silver.silver_layer.bridge_gong_call_to_sf_object"
GONG_TRACKER_HIT_TABLE = "silver.silver_layer.fct_gong_call_tracker_hit"
GONG_KEY_POINT_TABLE = "silver.silver_layer.fct_gong_call_key_point"
GONG_PARTICIPANT_TABLE = "silver.silver_layer.fct_gong_call_participant"

GONG_RISK_TRACKERS = {
    "Competitors", "Competition", "Objections", "Customer objections",
    "Customer concerns", "Discount", "Reactions to pricing",
}
GONG_ENGAGEMENT_TRACKERS = {
    "Value (tracker)", "Champion", "Business goals (tracker)",
    "Strategic business goals", "Decision process (tracker)",
    "Decision process (beta)", "Economic buyer",
}
DIM_USERS_TABLE = "silver.silver_layer.dim_salesforce_ae_user"
DIM_FRESHDESK_CUSTOMERS_TABLE = "silver.silver_layer.dim_freshdesk_account_customers"
FCT_FRESHDESK_TICKETS_TABLE = "silver.silver_layer.fct_freshdesk_ticket_history"
DIM_FRESHDESK_CONVERSATION_SUMMARY_TABLE = "silver.silver_layer.dim_freshdesk_ticket_conversation_summary"
KB_CONFLUENCE_CUSTOMER_CONTEXT_TABLE = "silver.silver_layer.kb_confluence_customer_context"
FCT_CONTRACTS_TABLE = "silver.silver_layer.fct_contracts"


class DatabricksService:
    """Service for querying Databricks SQL."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self._connection = None

    def _has_databricks_config(self) -> bool:
        """Check if Databricks configuration is available."""
        # Check for SDK Config (Databricks Apps environment)
        sdk_config = get_databricks_config()
        if sdk_config and sdk_config.host:
            has_path = bool(self.settings.effective_http_path or self.settings.databricks_warehouse_id)
            logger.info(f"SDK Config available - Host: {sdk_config.host}, Has Path: {has_path}")
            return has_path
        
        # Fallback to manual config (local dev)
        has_host = bool(self.settings.databricks_host)
        has_path = bool(self.settings.effective_http_path)
        has_token = bool(self.settings.databricks_token)
        
        logger.info(f"Manual config check - Host: {has_host}, Path: {has_path}, Token: {has_token}")
        return has_host and has_path and has_token

    @contextmanager
    def get_connection(self) -> Generator[Any, None, None]:
        """Get a Databricks SQL connection."""
        if not self._has_databricks_config():
            logger.warning("Databricks not configured, using mock data")
            yield None
            return

        connection = None
        try:
            from databricks import sql
            
            # Try to use Databricks SDK Config first (works in Databricks Apps)
            sdk_config = get_databricks_config()
            
            if sdk_config and sdk_config.host and not self.settings.databricks_token:
                # Databricks Apps environment - use SDK Config
                server_hostname = sdk_config.host.replace("https://", "").replace("http://", "")
                
                # Get warehouse ID from environment or settings
                warehouse_id = self.settings.databricks_warehouse_id or os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
                if warehouse_id:
                    http_path = f"/sql/1.0/warehouses/{warehouse_id}"
                else:
                    http_path = self.settings.effective_http_path
                
                logger.info(f"Using Databricks SDK Config authentication")
                logger.info(f"Server: {server_hostname}")
                logger.info(f"HTTP Path: {http_path}")
                
                # Use credentials_provider with SDK's authenticate method
                connection = sql.connect(
                    server_hostname=server_hostname,
                    http_path=http_path,
                    credentials_provider=lambda: sdk_config.authenticate,
                )
            else:
                # Local dev environment - use token
                server_hostname = self.settings.databricks_host
                http_path = self.settings.effective_http_path
                
                logger.info(f"Using access token authentication (local dev)")
                logger.info(f"Server: {server_hostname}")
                logger.info(f"HTTP Path: {http_path}")
                
                connection = sql.connect(
                    server_hostname=server_hostname,
                    http_path=http_path,
                    access_token=self.settings.databricks_token,
                )
            
            logger.info("SQL connection established successfully")
        except ImportError as e:
            logger.error(f"Failed to import databricks SQL connector: {e}")
            connection = None
        except Exception as e:
            logger.error(f"Failed to connect to Databricks: {type(e).__name__}: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            connection = None

        try:
            yield connection
        finally:
            if connection:
                try:
                    connection.close()
                    logger.info("SQL connection closed")
                except Exception as e:
                    logger.warning(f"Error closing connection: {e}")

    def _derive_health_score(
        self,
        renewal_days: Optional[int],
        engagement_6m: Optional[float],
        engagement_3m: Optional[float],
        account_status: Optional[str],
    ) -> HealthScore:
        """
        Derive health score from contract dates and engagement metrics.
        
        Logic:
        - Critical: Contract ending in <30 days OR very low engagement OR status indicates churn
        - At Risk: Contract ending in 30-90 days OR declining engagement
        - Good: Contract >90 days out AND healthy engagement
        """
        # Check for critical status indicators
        critical_statuses = ["churned", "churn", "cancelled", "lost", "inactive"]
        if account_status and account_status.lower() in critical_statuses:
            return HealthScore.CRITICAL

        # Check renewal timeline
        if renewal_days is not None:
            if renewal_days <= 30:
                return HealthScore.CRITICAL
            elif renewal_days <= 90:
                return HealthScore.AT_RISK

        # Check engagement decline (if 3-month is significantly lower than 6-month average)
        if engagement_6m and engagement_3m:
            # Calculate expected 3-month engagement (half of 6-month)
            expected_3m = engagement_6m / 2
            if expected_3m > 0 and engagement_3m < expected_3m * 0.5:
                # Engagement dropped by more than 50%
                return HealthScore.AT_RISK

        # Very low engagement is a risk
        if engagement_3m is not None and engagement_3m < 10:
            return HealthScore.AT_RISK

        return HealthScore.GOOD

    def _derive_primary_signal(
        self,
        renewal_days: Optional[int],
        engagement_6m: Optional[float],
        engagement_3m: Optional[float],
        account_status: Optional[str],
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Derive primary signal and signal type from account data.
        
        Returns: (signal_description, signal_type)
        """
        # Check renewal urgency first
        if renewal_days is not None:
            if renewal_days <= 30:
                return f"Renewal in {renewal_days} days", "renewal"
            elif renewal_days <= 60:
                return "Upcoming Renewal", "renewal"

        # Check engagement decline
        if engagement_6m and engagement_3m:
            expected_3m = engagement_6m / 2
            if expected_3m > 0:
                decline_pct = ((expected_3m - engagement_3m) / expected_3m) * 100
                if decline_pct > 25:
                    return f"Usage Down {int(decline_pct)}%", "usage"

        # Check for expansion opportunity (high engagement)
        if engagement_3m and engagement_3m > 500:
            return "Expansion Opportunity", "expansion"

        # Check account status
        if account_status:
            status_lower = account_status.lower()
            if "risk" in status_lower:
                return "At Risk Status", "churn"
            elif "active" in status_lower and renewal_days and renewal_days > 180:
                return "Stable", None

        return None, None

    def _map_account_status(self, status: Optional[str]) -> AccountStatus:
        """Map account_status_c to AccountStatus enum."""
        if not status:
            return AccountStatus.STABLE

        status_lower = status.lower()
        
        if any(s in status_lower for s in ["attention", "risk", "churn", "critical"]):
            return AccountStatus.NEEDS_ATTENTION
        elif any(s in status_lower for s in ["progress", "working", "pending"]):
            return AccountStatus.IN_PROGRESS
        else:
            return AccountStatus.STABLE

    def _renewal_tier_deduction_and_detail(
        self, renewal_days: Optional[int]
    ) -> Tuple[int, str]:
        """
        Raw renewal risk deduction (max 25) from days to nearest rev-rec end.

        DATEDIFF(rev_rec_end, today) is negative when the end date is in the past.
        In Python, any negative renewal_days satisfies ``<= 30``, so all overdue
        lines currently map to the critical band — use explicit past-due wording.
        """
        if renewal_days is None or renewal_days >= 999:
            return 0, "No upcoming renewal"
        rd = int(renewal_days)
        if rd < 0:
            past = abs(rd)
            return 25, f"{past} days past rev-rec end (critical)"
        if rd <= 30:
            return 25, f"{rd} days until rev-rec end (critical)"
        if rd <= 60:
            return 18, f"{rd} days until rev-rec end (urgent)"
        if rd <= 90:
            return 12, f"{rd} days until rev-rec end (soon)"
        if rd <= 180:
            return 5, f"{rd} days until rev-rec end"
        return 0, f"{rd}+ days until rev-rec end"

    def _tier_renewal_deduction_days(self, renewal_days: Optional[int]) -> Tuple[int, str]:
        """Backward-compatible alias for tier + human-readable renewal line."""
        return self._renewal_tier_deduction_and_detail(renewal_days)

    def _materiality_renewal_adjustment(
        self, base_deduction: int, lines: List[dict]
    ) -> Tuple[int, str, dict]:
        """
        Reduce renewal deduction when the nearest renewal is a small share of
        near-term renewal ARR (e.g. $50k tail vs $1M core in the same window).
        """
        meta: dict = {
            "base_deduction": base_deduction,
            "nearest_days": None,
            "nearest_arr_eur": 0.0,
            "near_term_arr_eur": 0.0,
            "share_of_near_term": 0.0,
            "weight": 1.0,
        }
        if not lines or base_deduction == 0:
            return base_deduction, "", meta

        near = [
            L
            for L in lines
            if L.get("renewal_days") is not None and 0 <= int(L["renewal_days"]) <= 365
        ]
        total_near = sum(float(L.get("arr_eur") or 0) for L in near)
        if total_near <= 0:
            total_near = 1.0

        valid = [L for L in lines if L.get("renewal_days") is not None and int(L["renewal_days"]) < 999]
        if not valid:
            return base_deduction, "", meta
        nearest = min(valid, key=lambda x: int(x["renewal_days"]))
        n_arr = float(nearest.get("arr_eur") or 0)
        share = n_arr / total_near if total_near > 0 else 1.0
        # Weight: full deduction when nearest line is most of near-term book; scale down tail renewals
        weight = max(0.15, min(1.0, share * 2.0))
        if share < 0.10 and base_deduction >= 12:
            weight = min(weight, 0.35)
        adjusted = int(round(base_deduction * weight))
        adjusted = max(0, min(25, adjusted))
        rt = str(nearest.get("revenue_type") or "")
        nd = int(nearest["renewal_days"])
        if nd < 0:
            lead = f"Nearest line is {abs(nd)}d past rev-rec end ({rt})"
        else:
            lead = f"Nearest renewal in {nd}d ({rt})"
        detail = (
            f"{lead}, "
            f"€{n_arr:,.0f} ≈ {share*100:.0f}% of €{total_near:,.0f} near-term renewal ARR — "
            f"deduction scaled ({base_deduction} → {adjusted}) so small contracts don't dominate the score."
        )
        meta.update(
            {
                "nearest_days": int(nearest["renewal_days"]),
                "nearest_arr_eur": n_arr,
                "near_term_arr_eur": total_near,
                "share_of_near_term": round(share, 4),
                "weight": round(weight, 3),
            }
        )
        return adjusted, detail, meta

    def get_renewal_contract_lines_for_account(self, account_id: str) -> List[dict]:
        """Open renewal contract rows for materiality-weighted health (same filters as portfolio ARR)."""
        FCT_TABLE = FCT_CONTRACTS_TABLE
        safe = self._sql_escape(account_id)
        rows: List[dict] = []
        try:
            with self.get_connection() as conn:
                if conn is None:
                    return []
                cur = conn.cursor()
                cur.execute(
                    f"""
                    SELECT
                        COALESCE(revenue_type, '') AS revenue_type,
                        COALESCE(ARR_EUR, 0) AS arr_eur,
                        rev_rec_end_date AS renewal_date,
                        DATEDIFF(rev_rec_end_date, CURRENT_DATE()) AS renewal_days,
                        COALESCE(CONTRACT_GROUP, '') AS contract_group
                    FROM {FCT_TABLE}
                    WHERE account_id = '{safe}'
                      AND RENEWAL_NOT_YET_CONTRACTED = 'Y'
                      AND revenue_type NOT IN ('Services', 'Perpetual')
                      AND (churn_expected_occurred IS NULL OR churn_expected_occurred = 'nan')
                      AND rev_rec_end_date > CURRENT_DATE()
                    ORDER BY renewal_days ASC
                    """
                )
                for r in cur.fetchall() or []:
                    rd = r[3]
                    try:
                        rd_int = int(rd) if rd is not None else None
                    except (TypeError, ValueError):
                        rd_int = None
                    rows.append(
                        {
                            "revenue_type": str(r[0] or ""),
                            "arr_eur": float(r[1] or 0),
                            "renewal_date": r[2],
                            "renewal_days": rd_int,
                            "contract_group": str(r[4] or ""),
                        }
                    )
                cur.close()
        except Exception as e:
            logger.warning(f"get_renewal_contract_lines_for_account: {e}")
            return []
        return rows

    def _llm_renewal_narrative(self, payload: dict) -> Optional[str]:
        """Optional Databricks ai_query narrative for CSM-facing explanation."""
        import json

        try:
            body = json.dumps(payload, default=str)[:10000]
            body_esc = body.replace("'", "''")
            model = os.environ.get(
                "DATABRICKS_LLM_MODEL", "databricks-meta-llama-3-1-70b-instruct"
            )
            with self.get_connection() as conn:
                if conn is None:
                    return None
                cur = conn.cursor()
                prompt = (
                    "You are a customer success coach. Using only the JSON facts below, "
                    "write 2-4 short sentences for a CSM: explain renewal risk in calm language, "
                    "mention materiality (large vs small renewals) when relevant, and one practical focus. "
                    "No markdown, no bullet points. JSON: "
                ) + body_esc
                sql = f"SELECT ai_query('{model}', '{prompt}')"
                cur.execute(sql)
                row = cur.fetchone()
                cur.close()
                if row and row[0]:
                    return str(row[0]).strip()
        except Exception as e:
            logger.warning(f"_llm_renewal_narrative: {e}")
        return None

    def _coerce_sql_date(self, val: Any) -> Optional[date]:
        if val is None:
            return None
        if isinstance(val, date) and not isinstance(val, datetime):
            return val
        if isinstance(val, datetime):
            return val.date()
        try:
            return date.fromisoformat(str(val)[:10])
        except (ValueError, TypeError):
            return None

    def get_renewal_health_insight(
        self,
        account_id: str,
        account_name: str,
        renewal_days_dim: Optional[int],
        with_llm: bool = False,
    ) -> RenewalHealthInsightResponse:
        """Contract-level renewal context, materiality-adjusted deduction, optional LLM."""
        lines = self.get_renewal_contract_lines_for_account(account_id)
        rd_eff: Optional[int] = renewal_days_dim
        if lines:
            valid_rd = [
                int(L["renewal_days"])
                for L in lines
                if L.get("renewal_days") is not None and int(L["renewal_days"]) < 999
            ]
            if valid_rd:
                rd_eff = min(valid_rd)

        base_ded, base_lbl = self._tier_renewal_deduction_days(rd_eff)
        adjusted = base_ded
        det = base_lbl
        weight = 1.0
        near_term = 0.0
        nearest_arr = 0.0
        share = 0.0
        meta: dict = {}

        if lines and base_ded > 0:
            adjusted, mat_detail, meta = self._materiality_renewal_adjustment(base_ded, lines)
            det = mat_detail if mat_detail else base_lbl
            weight = float(meta.get("weight") or 1.0)
            near_term = float(meta.get("near_term_arr_eur") or 0.0)
            nearest_arr = float(meta.get("nearest_arr_eur") or 0.0)
            share = float(meta.get("share_of_near_term") or 0.0)
        elif lines:
            near = [
                L
                for L in lines
                if L.get("renewal_days") is not None and 0 <= int(L["renewal_days"]) <= 365
            ]
            near_term = sum(float(L.get("arr_eur") or 0) for L in near)
            valid = [L for L in lines if L.get("renewal_days") is not None and int(L["renewal_days"]) < 999]
            if valid:
                nearest = min(valid, key=lambda x: int(x["renewal_days"]))
                nearest_arr = float(nearest.get("arr_eur") or 0)
                total_denom = near_term if near_term > 0 else max(nearest_arr, 1.0)
                share = nearest_arr / total_denom if total_denom > 0 else 0.0
            if not det:
                det = base_lbl

        contracts: List[RenewalContractLine] = []
        for L in lines:
            contracts.append(
                RenewalContractLine(
                    revenue_type=str(L.get("revenue_type") or ""),
                    arr_eur=float(L.get("arr_eur") or 0),
                    renewal_date=self._coerce_sql_date(L.get("renewal_date")),
                    renewal_days=L.get("renewal_days"),
                    contract_group=(L.get("contract_group") or None) or None,
                )
            )

        nearest_days_out: Optional[int] = None
        if rd_eff is not None and rd_eff < 999:
            nearest_days_out = int(rd_eff)

        llm: Optional[str] = None
        if with_llm:
            payload = {
                "account_name": account_name,
                "nearest_renewal_days": nearest_days_out,
                "base_renewal_deduction": base_ded,
                "adjusted_renewal_deduction": adjusted,
                "materiality_weight": weight,
                "near_term_arr_eur": near_term,
                "nearest_line_arr_eur": nearest_arr,
                "share_of_near_term": share,
                "deterministic_explanation": det,
                "contract_lines": [
                    {
                        "revenue_type": c.revenue_type,
                        "arr_eur": c.arr_eur,
                        "renewal_days": c.renewal_days,
                    }
                    for c in contracts[:30]
                ],
            }
            llm = self._llm_renewal_narrative(payload)

        return RenewalHealthInsightResponse(
            account_id=account_id,
            account_name=account_name,
            contracts=contracts,
            nearest_renewal_days=nearest_days_out,
            base_renewal_deduction=base_ded,
            adjusted_renewal_deduction=adjusted,
            materiality_weight=weight,
            near_term_arr_eur=near_term,
            nearest_line_arr_eur=nearest_arr,
            share_of_near_term=share,
            deterministic_explanation=det,
            llm_narrative=llm,
        )

    def calculate_health_score_detail(
        self,
        account_name: str,
        renewal_days: Optional[int],
        pendo_data: Optional[dict] = None,
        freshdesk_data: Optional[dict] = None,
        account_id: Optional[str] = None,
        gong_data=None,  # Optional[GongActivityAnalysis]
    ) -> HealthScoreDetail:
        """
        Calculate comprehensive health score with detailed breakdown.

        Higher score = healthier account (inverted from risk scoring)

        Components (deductions from 100):
        - Contract Renewal (max -20): Based on days to renewal
        - Pendo Product Usage (max -35): Based on product usage and user activity trends
        - Freshdesk Support (max -30): Based on open tickets and sentiment
        - Gong Sentiment (max -15): Based on risk vs engagement tracker signals over 90d

        Score mapping:
        - 70-100: Good (healthy)
        - 40-69: At Risk (needs attention)
        - 0-39: Critical (urgent)

        Args:
            pendo_data: Pre-fetched Pendo metrics (from batch query). If None, will query individually.
            freshdesk_data: Pre-fetched Freshdesk metrics (from batch query). If None, will query individually.
            gong_data: GongActivityAnalysis with risk_signal_calls / engagement_signal_calls. If None, Gong factor is skipped.
        """
        factors = []
        has_pendo = False
        has_freshdesk = False
        
        # ═══════════════════════════════════════════════════════════════
        # 1. CONTRACT RENEWAL (max 25 point deduction, materiality-adjusted)
        # ═══════════════════════════════════════════════════════════════
        renewal_deduction = 0
        renewal_detail = ""
        renewal_lines: List[dict] = []
        if account_id:
            renewal_lines = self.get_renewal_contract_lines_for_account(account_id)
        rd_eff = renewal_days
        if renewal_lines:
            valid_rd = [
                int(L["renewal_days"])
                for L in renewal_lines
                if L.get("renewal_days") is not None and int(L["renewal_days"]) < 999
            ]
            if valid_rd:
                rd_eff = min(valid_rd)
        base_ded, base_lbl = self._tier_renewal_deduction_days(rd_eff)
        if renewal_lines and base_ded > 0:
            renewal_deduction, mat_detail, _meta = self._materiality_renewal_adjustment(
                base_ded, renewal_lines
            )
            renewal_detail = mat_detail if mat_detail else base_lbl
        else:
            renewal_deduction = base_ded
            renewal_detail = base_lbl
        
        factors.append(HealthScoreFactor(
            name="Contract Renewal",
            points=renewal_deduction,
            max_points=20,
            detail=renewal_detail,
            icon="📅"
        ))
        
        # ═══════════════════════════════════════════════════════════════
        # 2. PENDO PRODUCT USAGE (max 40 point deduction)
        # Combined factor for product engagement and user activity trends
        # ═══════════════════════════════════════════════════════════════
        pendo_deduction = 0
        pendo_detail = ""
        
        try:
            # Use pre-fetched data if available, otherwise query individually
            if pendo_data is None:
                pendo_data = self._get_pendo_engagement_metrics(account_name)
            if pendo_data:
                has_pendo = True
                current_visitors = pendo_data.get("current_visitors", 0)
                previous_visitors = pendo_data.get("previous_visitors", 0)
                
                # Calculate change percentage
                if previous_visitors > 0:
                    change_pct = ((current_visitors - previous_visitors) / previous_visitors) * 100
                    
                    if current_visitors == 0:
                        pendo_deduction = 40
                        pendo_detail = f"No activity last 30d (was {previous_visitors} users)"
                    elif change_pct <= -50:
                        pendo_deduction = 35
                        pendo_detail = f"↓{abs(int(change_pct))}% severe decline ({current_visitors} vs {previous_visitors} users)"
                    elif change_pct <= -30:
                        pendo_deduction = 25
                        pendo_detail = f"↓{abs(int(change_pct))}% significant decline ({current_visitors} users)"
                    elif change_pct <= -10:
                        pendo_deduction = 12
                        pendo_detail = f"↓{abs(int(change_pct))}% slight decline ({current_visitors} users)"
                    elif change_pct >= 10:
                        pendo_deduction = 0
                        pendo_detail = f"↑{int(change_pct)}% growth ({current_visitors} users)"
                    else:
                        pendo_deduction = 0
                        pendo_detail = f"Stable ({current_visitors} active users)"
                elif current_visitors > 0:
                    pendo_deduction = 0
                    pendo_detail = f"{current_visitors} active users (baseline)"
                else:
                    pendo_deduction = 20
                    pendo_detail = "No activity recorded"
            else:
                pendo_detail = "No Pendo integration"
        except Exception as e:
            logger.warning(f"Error getting Pendo engagement: {e}")
            pendo_detail = "Unable to fetch data"
        
        factors.append(HealthScoreFactor(
            name="Pendo Product Usage",
            points=pendo_deduction,
            max_points=35,
            detail=pendo_detail,
            icon="📊"
        ))
        
        # ═══════════════════════════════════════════════════════════════
        # 3. FRESHDESK SUPPORT (max 25 point deduction)
        # ═══════════════════════════════════════════════════════════════
        support_deduction = 0
        support_detail = ""
        
        try:
            # Use pre-fetched data if available, otherwise query individually
            support_data = freshdesk_data
            if support_data is None:
                support_data = self._get_freshdesk_support_metrics(account_name)
            if support_data:
                has_freshdesk = True
                open_critical = support_data.get("open_critical", 0)
                open_high = support_data.get("open_high", 0)
                open_total = support_data.get("open_total", 0)
                avg_sentiment = support_data.get("avg_sentiment", 0)
                
                if open_critical > 0:
                    support_deduction += 20
                if open_high > 0:
                    support_deduction += min(10, open_high * 5)
                if avg_sentiment < -0.5:
                    support_deduction += 10
                
                support_deduction = min(35, support_deduction)
                
                if open_critical > 0:
                    support_detail = f"{open_critical} critical ticket(s)"
                elif open_high > 0:
                    support_detail = f"{open_high} high priority ticket(s)"
                elif open_total > 0:
                    support_detail = f"{open_total} open ticket(s)"
                else:
                    support_detail = "No open tickets"
                
                if avg_sentiment < -0.5:
                    support_detail += " • Negative sentiment"
            else:
                support_detail = "No Freshdesk integration"
        except Exception as e:
            logger.warning(f"Error getting Freshdesk metrics: {e}")
            support_detail = "Unable to fetch data"
        
        factors.append(HealthScoreFactor(
            name="Freshdesk Support",
            points=support_deduction,
            max_points=30,
            detail=support_detail,
            icon="🎫"
        ))

        # ═══════════════════════════════════════════════════════════════
        # 4. GONG SENTIMENT (max 15 point deduction)
        # Based on risk vs engagement tracker signals over last 90d.
        # Skipped entirely if account has no Gong data (gong_data is None).
        # ═══════════════════════════════════════════════════════════════
        if gong_data is not None:
            r = gong_data.risk_signal_calls
            e = gong_data.engagement_signal_calls
            total = r + e
            if total == 0:
                gong_pts = 3
                gong_detail = "Calls recorded but no notable topics detected."
            elif r == 0:
                gong_pts = 0
                gong_detail = f"Positive signals in {e} call(s) — champion/value topics."
            elif e == 0:
                gong_pts = 15
                gong_detail = f"Risk topics in {r} call(s) with no positive signals."
            elif r > e:
                gong_pts = 10
                gong_detail = f"Risk signals outweigh positive — {r} risk vs {e} engagement calls."
            else:
                gong_pts = 5
                gong_detail = f"Mixed signals — {r} risk, {e} engagement calls."
            factors.append(HealthScoreFactor(
                name="Gong Sentiment",
                points=gong_pts,
                max_points=15,
                detail=gong_detail,
                icon="📞",
            ))

        # ═══════════════════════════════════════════════════════════════
        # CALCULATE HEALTH SCORE (100 - total deductions)
        # Max possible deductions: Renewal(20) + Pendo(35) + Freshdesk(30) + Gong(15) = 100
        # ═══════════════════════════════════════════════════════════════
        total_deductions = sum(f.points for f in factors)
        health_score = 100 - total_deductions
        health_score = min(100, max(0, health_score))  # Clamp 0-100
        
        # Map to category (higher score = healthier)
        if health_score >= 70:
            category = HealthScore.GOOD
        elif health_score >= 40:
            category = HealthScore.AT_RISK
        else:
            category = HealthScore.CRITICAL
        
        return HealthScoreDetail(
            score=health_score,
            category=category,
            factors=factors,
            has_pendo=has_pendo,
            has_freshdesk=has_freshdesk,
            has_gong=gong_data is not None,
            scoring_version="rule-based-v1.4"
        )

    def _get_precomputed_health_scores(self, conn) -> dict:
        """
        Fetch pre-computed health scores from daily snapshot table.
        Returns dict[account_id -> HealthScoreDetail] for ALL accounts.
        
        This is MUCH faster than real-time calculation (~100ms vs ~60s).
        The table is updated daily by a Databricks job.
        """
        HEALTH_SCORES_TABLE = "silver.silver_layer.account_health_scores_history"
        result = {}
        
        try:
            cursor = conn.cursor()
            
            # Get the latest scores (most recent score_date)
            query = f"""
                SELECT
                    account_id,
                    account_name,
                    health_score,
                    health_category,
                    renewal_days,
                    current_visitors,
                    previous_visitors,
                    open_critical,
                    open_high,
                    open_total,
                    has_pendo,
                    has_freshdesk,
                    risk_signal_calls,
                    engagement_signal_calls,
                    has_gong
                FROM {HEALTH_SCORES_TABLE}
                WHERE score_date = (SELECT MAX(score_date) FROM {HEALTH_SCORES_TABLE})
            """
            
            cursor.execute(query)
            rows = cursor.fetchall()
            cursor.close()
            
            for row in rows:
                account_id = row[0]
                if not account_id:
                    continue
                    
                health_score = int(row[2]) if row[2] is not None else 100
                health_category_str = row[3] if row[3] else "Good"
                renewal_days = int(row[4]) if row[4] is not None else 999
                current_visitors = int(row[5]) if row[5] is not None else 0
                previous_visitors = int(row[6]) if row[6] is not None else 0
                open_critical = int(row[7]) if row[7] is not None else 0
                open_high = int(row[8]) if row[8] is not None else 0
                open_total = int(row[9]) if row[9] is not None else 0
                has_pendo = bool(row[10]) if row[10] is not None else False
                has_freshdesk = bool(row[11]) if row[11] is not None else False
                risk_signal_calls = int(row[12]) if row[12] is not None else -1
                engagement_signal_calls = int(row[13]) if row[13] is not None else -1
                has_gong = bool(row[14]) if row[14] is not None else False

                # Map category string to enum
                if health_category_str == "Critical":
                    category = HealthScore.CRITICAL
                elif health_category_str == "At Risk":
                    category = HealthScore.AT_RISK
                else:
                    category = HealthScore.GOOD

                # Reconstruct factors from pre-computed data
                factors = self._reconstruct_health_factors(
                    renewal_days=renewal_days,
                    current_visitors=current_visitors,
                    previous_visitors=previous_visitors,
                    open_critical=open_critical,
                    open_high=open_high,
                    open_total=open_total,
                    has_pendo=has_pendo,
                    has_freshdesk=has_freshdesk,
                    risk_signal_calls=risk_signal_calls,
                    engagement_signal_calls=engagement_signal_calls,
                    has_gong=has_gong,
                )
                
                result[account_id] = HealthScoreDetail(
                    score=health_score,
                    category=category,
                    factors=factors,
                    has_pendo=has_pendo,
                    has_freshdesk=has_freshdesk,
                    has_gong=has_gong,
                    scoring_version="precomputed-v1.0"
                )
            
            logger.info(f"Loaded {len(result)} pre-computed health scores")
            return result
            
        except Exception as e:
            logger.warning(f"Failed to load pre-computed health scores: {e}")
            return {}

    def _reconstruct_health_factors(
        self,
        renewal_days: int,
        current_visitors: int,
        previous_visitors: int,
        open_critical: int,
        open_high: int,
        open_total: int,
        has_pendo: bool,
        has_freshdesk: bool,
        risk_signal_calls: int = -1,
        engagement_signal_calls: int = -1,
        has_gong: bool = False,
    ) -> List[HealthScoreFactor]:
        """
        Reconstruct health score factors from pre-computed metrics.
        This matches the logic in calculate_health_score_detail().
        """
        factors = []
        
        # 1. CONTRACT RENEWAL (max 25 point deduction)
        if renewal_days >= 999:
            renewal_deduction, renewal_detail = 0, "No upcoming renewal"
        else:
            renewal_deduction, renewal_detail = self._renewal_tier_deduction_and_detail(
                renewal_days
            )
        
        factors.append(HealthScoreFactor(
            name="Contract Renewal",
            points=renewal_deduction,
            max_points=20,
            detail=renewal_detail,
            icon="📅"
        ))
        
        # 2. PENDO PRODUCT USAGE (max 40 point deduction)
        if has_pendo:
            if previous_visitors > 0:
                change_pct = ((current_visitors - previous_visitors) / previous_visitors) * 100
                
                if current_visitors == 0:
                    pendo_deduction = 40
                    pendo_detail = f"No activity last 30d (was {previous_visitors} users)"
                elif change_pct <= -50:
                    pendo_deduction = 35
                    pendo_detail = f"↓{abs(int(change_pct))}% severe decline ({current_visitors} vs {previous_visitors} users)"
                elif change_pct <= -30:
                    pendo_deduction = 25
                    pendo_detail = f"↓{abs(int(change_pct))}% significant decline ({current_visitors} users)"
                elif change_pct <= -10:
                    pendo_deduction = 12
                    pendo_detail = f"↓{abs(int(change_pct))}% slight decline ({current_visitors} users)"
                elif change_pct >= 10:
                    pendo_deduction = 0
                    pendo_detail = f"↑{int(change_pct)}% growth ({current_visitors} users)"
                else:
                    pendo_deduction = 0
                    pendo_detail = f"Stable ({current_visitors} active users)"
            elif current_visitors > 0:
                pendo_deduction = 0
                pendo_detail = f"{current_visitors} active users (baseline)"
            else:
                pendo_deduction = 20
                pendo_detail = "No activity recorded"
        else:
            pendo_deduction = 0
            pendo_detail = "No Pendo integration"
        
        factors.append(HealthScoreFactor(
            name="Pendo Product Usage",
            points=pendo_deduction,
            max_points=35,
            detail=pendo_detail,
            icon="📊"
        ))
        
        # 3. FRESHDESK SUPPORT (max 35 point deduction)
        if has_freshdesk:
            support_deduction = 0
            if open_critical > 0:
                support_deduction += 20
            if open_high > 0:
                support_deduction += min(10, open_high * 5)
            support_deduction = min(35, support_deduction)
            
            if open_critical > 0:
                support_detail = f"{open_critical} critical ticket(s)"
            elif open_high > 0:
                support_detail = f"{open_high} high priority ticket(s)"
            elif open_total > 0:
                support_detail = f"{open_total} open ticket(s)"
            else:
                support_detail = "No open tickets"
        else:
            support_deduction = 0
            support_detail = "No Freshdesk integration"
        
        factors.append(HealthScoreFactor(
            name="Freshdesk Support",
            points=support_deduction,
            max_points=35,
            detail=support_detail,
            icon="🎫"
        ))

        # 4. GONG SENTIMENT (max 15 point deduction)
        if has_gong and risk_signal_calls >= 0:
            r = risk_signal_calls
            e = engagement_signal_calls if engagement_signal_calls >= 0 else 0
            total = r + e
            if total == 0:
                gong_pts = 3
                gong_detail = "Calls recorded but no notable topics detected."
            elif r == 0:
                gong_pts = 0
                gong_detail = f"Positive signals in {e} call(s) — champion/value topics."
            elif e == 0:
                gong_pts = 15
                gong_detail = f"Risk topics in {r} call(s) with no positive signals."
            elif r > e:
                gong_pts = 10
                gong_detail = f"Risk signals outweigh positive — {r} risk vs {e} engagement calls."
            else:
                gong_pts = 5
                gong_detail = f"Mixed signals — {r} risk, {e} engagement calls."
            factors.append(HealthScoreFactor(
                name="Gong Sentiment",
                points=gong_pts,
                max_points=15,
                detail=gong_detail,
                icon="📞",
            ))

        return factors

    def _get_all_pendo_metrics_batch(self, conn, account_names: List[str]) -> dict:
        """
        Batch fetch Pendo metrics for multiple accounts in ONE query.
        Returns dict[account_name -> metrics_dict] keyed by INPUT account names.
        Uses case-insensitive matching to handle name variations.
        """
        PENDO_ACCOUNTS = "silver.silver_layer.dim_pendo_account_customers"
        PENDO_ACCOUNT_DAILY = "silver.silver_layer.fct_pendo_account_daily_metrics"
        
        result = {}
        if not account_names or conn is None:
            return result
        
        try:
            cursor = conn.cursor()
            
            # Build lookup map: lowercase -> original input name
            input_name_lookup = {name.lower().strip(): name for name in account_names if name}
            
            # Escape all account names for SQL
            safe_names = [self._sql_escape(name) for name in account_names if name]
            if not safe_names:
                return result
            
            names_in = ", ".join([f"'{n}'" for n in safe_names])
            
            # Query using case-insensitive match (LOWER)
            batch_query = f"""
                WITH account_pendo_ids AS (
                    SELECT pc.name as pendo_name, pc.id as pendo_id
                    FROM {PENDO_ACCOUNTS} pc
                    WHERE LOWER(TRIM(pc.name)) IN ({", ".join([f"LOWER('{n}')" for n in safe_names])})
                )
                SELECT 
                    api.pendo_name,
                    SUM(CASE WHEN p.date_day >= DATE_SUB(CURRENT_DATE(), 30) THEN COALESCE(p.count_active_visitors, 0) ELSE 0 END) as current_visitors,
                    SUM(CASE WHEN p.date_day >= DATE_SUB(CURRENT_DATE(), 60) AND p.date_day < DATE_SUB(CURRENT_DATE(), 30) THEN COALESCE(p.count_active_visitors, 0) ELSE 0 END) as previous_visitors
                FROM account_pendo_ids api
                LEFT JOIN {PENDO_ACCOUNT_DAILY} p ON p.account_id = api.pendo_id AND p.date_day >= DATE_SUB(CURRENT_DATE(), 60)
                GROUP BY api.pendo_name
            """
            
            cursor.execute(batch_query)
            rows = cursor.fetchall()
            cursor.close()
            
            # Map results back to INPUT names (not Pendo names)
            for row in rows:
                pendo_name = row[0]
                if pendo_name:
                    # Find matching input name via case-insensitive lookup
                    lookup_key = pendo_name.lower().strip()
                    original_input_name = input_name_lookup.get(lookup_key, pendo_name)
                    result[original_input_name] = {
                        "current_visitors": int(row[1] or 0),
                        "previous_visitors": int(row[2] or 0),
                    }
            
            logger.info(f"Batch Pendo query returned metrics for {len(result)} accounts")
            return result
            
        except Exception as e:
            logger.warning(f"_get_all_pendo_metrics_batch error: {e}")
            return result

    def _get_all_freshdesk_metrics_batch(self, conn, account_names: List[str]) -> dict:
        """
        Batch fetch Freshdesk metrics for multiple accounts in ONE query.
        Returns dict[account_name -> metrics_dict] keyed by INPUT account names.
        Uses case-insensitive matching to handle name variations.
        """
        result = {}
        if not account_names or conn is None:
            return result
        
        try:
            cursor = conn.cursor()
            
            # Build lookup map: lowercase -> original input name
            input_name_lookup = {name.lower().strip(): name for name in account_names if name}
            
            # Escape all account names for SQL
            safe_names = [self._sql_escape(name) for name in account_names if name]
            if not safe_names:
                return result
            
            # Query using case-insensitive match (LOWER)
            batch_query = f"""
                WITH company_mapping AS (
                    SELECT fc.name as freshdesk_name, fc.id as company_id
                    FROM {DIM_FRESHDESK_CUSTOMERS_TABLE} fc
                    WHERE LOWER(TRIM(fc.name)) IN ({", ".join([f"LOWER('{n}')" for n in safe_names])})
                      AND fc._fivetran_deleted = false
                )
                SELECT 
                    cm.freshdesk_name,
                    COUNT(CASE WHEN t.label_for_customer NOT IN ('Closed', 'Resolved') AND t.priority = 'Urgent' THEN 1 END) as open_critical,
                    COUNT(CASE WHEN t.label_for_customer NOT IN ('Closed', 'Resolved') AND t.priority = 'High' THEN 1 END) as open_high,
                    COUNT(CASE WHEN t.label_for_customer NOT IN ('Closed', 'Resolved') THEN 1 END) as open_total,
                    AVG(COALESCE(cs.net_sentiment_score, 0)) as avg_sentiment
                FROM company_mapping cm
                LEFT JOIN {FCT_FRESHDESK_TICKETS_TABLE} t ON t.company_id = cm.company_id
                LEFT JOIN {DIM_FRESHDESK_CONVERSATION_SUMMARY_TABLE} cs ON t.id = cs.ticket_id
                GROUP BY cm.freshdesk_name
            """
            
            cursor.execute(batch_query)
            rows = cursor.fetchall()
            cursor.close()
            
            # Map results back to INPUT names (not Freshdesk names)
            for row in rows:
                freshdesk_name = row[0]
                if freshdesk_name:
                    # Find matching input name via case-insensitive lookup
                    lookup_key = freshdesk_name.lower().strip()
                    original_input_name = input_name_lookup.get(lookup_key, freshdesk_name)
                    result[original_input_name] = {
                        "open_critical": int(row[1] or 0),
                        "open_high": int(row[2] or 0),
                        "open_total": int(row[3] or 0),
                        "avg_sentiment": float(row[4] or 0),
                    }
            
            logger.info(f"Batch Freshdesk query returned metrics for {len(result)} accounts")
            return result
            
        except Exception as e:
            logger.warning(f"_get_all_freshdesk_metrics_batch error: {e}")
            return result

    def _get_pendo_engagement_metrics(self, account_name: str) -> Optional[dict]:
        """
        Get Pendo engagement metrics for an account.
        Returns current 30-day and previous 30-day metrics for comparison.
        """
        PENDO_ACCOUNTS = "silver.silver_layer.dim_pendo_account_customers"
        PENDO_ACCOUNT_DAILY = "silver.silver_layer.fct_pendo_account_daily_metrics"
        
        try:
            with self.get_connection() as conn:
                if conn is None:
                    return None
                
                safe_name = self._sql_escape(account_name)
                cursor = conn.cursor()
                
                # Find pendo account IDs
                id_query = f"""
                    SELECT pc.id
                    FROM {PENDO_ACCOUNTS} pc
                    WHERE pc.name = '{safe_name}'
                """
                cursor.execute(id_query)
                pendo_rows = cursor.fetchall()
                pendo_ids = [r[0] for r in pendo_rows if r[0]]
                
                if not pendo_ids:
                    cursor.close()
                    return None
                
                ids_in = ", ".join([f"'{self._sql_escape(pid)}'" for pid in pendo_ids])
                
                # Get 30-day metrics (current and previous)
                metrics_query = f"""
                    SELECT
                        SUM(CASE WHEN date_day >= DATE_SUB(CURRENT_DATE(), 30) THEN count_active_visitors ELSE 0 END) as current_visitors,
                        SUM(CASE WHEN date_day >= DATE_SUB(CURRENT_DATE(), 60) AND date_day < DATE_SUB(CURRENT_DATE(), 30) THEN count_active_visitors ELSE 0 END) as previous_visitors,
                        SUM(CASE WHEN date_day >= DATE_SUB(CURRENT_DATE(), 30) THEN COALESCE(sum_minutes, 0) ELSE 0 END) as current_minutes,
                        SUM(CASE WHEN date_day >= DATE_SUB(CURRENT_DATE(), 60) AND date_day < DATE_SUB(CURRENT_DATE(), 30) THEN COALESCE(sum_minutes, 0) ELSE 0 END) as previous_minutes,
                        COUNT(DISTINCT CASE WHEN date_day >= DATE_SUB(CURRENT_DATE(), 30) THEN date_day END) as current_days,
                        COUNT(DISTINCT CASE WHEN date_day >= DATE_SUB(CURRENT_DATE(), 60) AND date_day < DATE_SUB(CURRENT_DATE(), 30) THEN date_day END) as previous_days
                    FROM {PENDO_ACCOUNT_DAILY}
                    WHERE account_id IN ({ids_in})
                      AND date_day >= DATE_SUB(CURRENT_DATE(), 60)
                """
                cursor.execute(metrics_query)
                row = cursor.fetchone()
                cursor.close()
                
                if row:
                    return {
                        "current_visitors": int(row[0] or 0),
                        "previous_visitors": int(row[1] or 0),
                        "current_minutes": float(row[2] or 0),
                        "previous_minutes": float(row[3] or 0),
                        "current_days": int(row[4] or 0),
                        "previous_days": int(row[5] or 0),
                    }
                return None
        except Exception as e:
            logger.warning(f"_get_pendo_engagement_metrics error: {e}")
            return None

    def _derive_primary_signal_from_health(
        self, health_detail: HealthScoreDetail
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Derive primary signal from health score factors.
        Returns the most impactful factor as the primary signal.
        """
        if not health_detail.factors:
            return None, None
        
        # Find the factor with highest points (biggest risk contributor)
        max_factor = max(health_detail.factors, key=lambda f: f.points)
        
        if max_factor.points == 0:
            return "Healthy", None
        
        # Map factor name to signal type
        signal_type_map = {
            "Renewal": "renewal",
            "Engagement": "usage",
            "Support": "support",
            "Activity": "usage",
        }
        
        signal_type = signal_type_map.get(max_factor.name, None)
        
        # Generate signal description based on factor
        if max_factor.name == "Renewal":
            if max_factor.points >= 25:
                return f"Renewal Critical", "renewal"
            elif max_factor.points >= 12:
                return "Upcoming Renewal", "renewal"
        elif max_factor.name == "Engagement":
            if "decline" in max_factor.detail.lower():
                return "Usage Declining", "usage"
            elif "no activity" in max_factor.detail.lower():
                return "No Recent Activity", "usage"
        elif max_factor.name == "Support":
            if "critical" in max_factor.detail.lower():
                return "Critical Ticket Open", "support"
            elif "high" in max_factor.detail.lower():
                return "High Priority Tickets", "support"
        elif max_factor.name == "Activity":
            if "dropped" in max_factor.detail.lower():
                return "Users Declining", "usage"
        
        return max_factor.detail[:50], signal_type

    def _get_freshdesk_support_metrics(self, account_name: str) -> Optional[dict]:
        """
        Get Freshdesk support metrics for an account.
        Returns open ticket counts and sentiment.
        """
        try:
            with self.get_connection() as conn:
                if conn is None:
                    return None
                
                cursor = conn.cursor()
                
                # Get support metrics
                metrics_query = f"""
                WITH freshdesk_company AS (
                    SELECT id 
                    FROM {DIM_FRESHDESK_CUSTOMERS_TABLE}
                    WHERE name = :account_name
                      AND _fivetran_deleted = false
                ),
                tickets_with_sentiment AS (
                    SELECT 
                        t.priority,
                        t.label_for_customer,
                        cs.net_sentiment_score
                    FROM {FCT_FRESHDESK_TICKETS_TABLE} t
                    LEFT JOIN {DIM_FRESHDESK_CONVERSATION_SUMMARY_TABLE} cs ON t.id = cs.ticket_id
                    WHERE t.company_id IN (SELECT id FROM freshdesk_company)
                )
                SELECT
                    COUNT(CASE WHEN label_for_customer NOT IN ('Closed', 'Resolved') AND priority = 'Urgent' THEN 1 END) as open_critical,
                    COUNT(CASE WHEN label_for_customer NOT IN ('Closed', 'Resolved') AND priority = 'High' THEN 1 END) as open_high,
                    COUNT(CASE WHEN label_for_customer NOT IN ('Closed', 'Resolved') THEN 1 END) as open_total,
                    AVG(COALESCE(net_sentiment_score, 0)) as avg_sentiment
                FROM tickets_with_sentiment
                """
                cursor.execute(metrics_query, {"account_name": account_name})
                row = cursor.fetchone()
                cursor.close()
                
                if row:
                    return {
                        "open_critical": int(row[0] or 0),
                        "open_high": int(row[1] or 0),
                        "open_total": int(row[2] or 0),
                        "avg_sentiment": float(row[3] or 0),
                    }
                return None
        except Exception as e:
            logger.warning(f"_get_freshdesk_support_metrics error: {e}")
            return None

    def _get_health_counts_from_precomputed(
        self,
        conn,
        account_type: Optional[str] = None
    ) -> Optional[Tuple[int, int, int]]:
        """
        Get health category counts from pre-computed table, optionally filtered by account_type.
        Returns (good_count, at_risk_count, critical_count) or None if table not available.
        """
        HEALTH_SCORES_TABLE = "silver.silver_layer.account_health_scores_history"
        
        try:
            cursor = conn.cursor()
            
            type_join = ""
            type_filter = ""
            if account_type:
                safe_type = account_type.replace("'", "''")
                type_join = f"JOIN {DIM_CUSTOMERS_TABLE} c ON h.account_id = c.account_id AND c._fivetran_deleted = false"
                type_filter = f"AND c.account_type = '{safe_type}'"
            
            query = f"""
                SELECT 
                    SUM(CASE WHEN h.health_category = 'Good' THEN 1 ELSE 0 END) as good_count,
                    SUM(CASE WHEN h.health_category = 'At Risk' THEN 1 ELSE 0 END) as at_risk_count,
                    SUM(CASE WHEN h.health_category = 'Critical' THEN 1 ELSE 0 END) as critical_count
                FROM {HEALTH_SCORES_TABLE} h
                {type_join}
                WHERE h.score_date = (SELECT MAX(score_date) FROM {HEALTH_SCORES_TABLE})
                {type_filter}
            """
            
            cursor.execute(query)
            row = cursor.fetchone()
            cursor.close()
            
            if row:
                good = int(row[0]) if row[0] else 0
                at_risk = int(row[1]) if row[1] else 0
                critical = int(row[2]) if row[2] else 0
                logger.info(f"Health counts from precomputed (type={account_type}): good={good}, at_risk={at_risk}, critical={critical}")
                return good, at_risk, critical
            
            return None
            
        except Exception as e:
            logger.warning(f"Failed to get health counts from precomputed table: {e}")
            return None

    def _get_usage_decline_count_from_precomputed(self, conn, account_type: Optional[str] = None) -> int:
        """
        Get count of accounts with significant usage decline (>=20% drop),
        optionally filtered by account_type.
        """
        HEALTH_SCORES_TABLE = "silver.silver_layer.account_health_scores_history"
        
        try:
            cursor = conn.cursor()
            
            type_join = ""
            type_filter = ""
            if account_type:
                safe_type = account_type.replace("'", "''")
                type_join = f"JOIN {DIM_CUSTOMERS_TABLE} c ON h.account_id = c.account_id AND c._fivetran_deleted = false"
                type_filter = f"AND c.account_type = '{safe_type}'"
            
            query = f"""
                SELECT COUNT(*) 
                FROM {HEALTH_SCORES_TABLE} h
                {type_join}
                WHERE h.score_date = (SELECT MAX(score_date) FROM {HEALTH_SCORES_TABLE})
                  AND h.has_pendo = true
                  AND h.previous_visitors > 0
                  AND ((h.current_visitors - h.previous_visitors) * 100.0 / h.previous_visitors) <= -20
                  {type_filter}
            """
            
            cursor.execute(query)
            row = cursor.fetchone()
            cursor.close()
            
            count = int(row[0]) if row and row[0] else 0
            logger.info(f"Usage decline count (>=20% drop, type={account_type}): {count}")
            return count
            
        except Exception as e:
            logger.warning(f"Failed to get usage decline count: {e}")
            return 0

    def get_health_score_history(self, account_id: str) -> list:
        """
        Fetch all historical health scores for a given account.
        Returns list of {score_date, health_score, health_category} sorted by date ascending.
        """
        HEALTH_SCORES_TABLE = "silver.silver_layer.account_health_scores_history"
        
        with self.get_connection() as conn:
            if conn is None:
                logger.warning("No DB connection for health score history")
                return []
            try:
                cursor = conn.cursor()
                
                safe_id = account_id.replace("'", "''")
                query = f"""
                    SELECT score_date, health_score, health_category
                    FROM {HEALTH_SCORES_TABLE}
                    WHERE account_id = '{safe_id}'
                    ORDER BY score_date ASC
                """
                
                cursor.execute(query)
                rows = cursor.fetchall()
                cursor.close()
                
                history = []
                for row in rows:
                    history.append({
                        "score_date": row[0],
                        "health_score": int(row[1]) if row[1] is not None else 0,
                        "health_category": row[2] or "Good",
                    })
                
                logger.info(f"Health score history for {account_id}: {len(history)} data points")
                return history
                
            except Exception as e:
                logger.warning(f"Failed to get health score history for {account_id}: {e}")
                return []

    @staticmethod
    def _explain_health_movement(prev: dict, curr: dict) -> str:
        """Generate a deterministic explanation of why an account's health category changed."""
        reasons = []

        def _renewal_deduction(rd):
            if rd <= 30: return 25
            if rd <= 60: return 18
            if rd <= 90: return 12
            if rd <= 180: return 5
            return 0

        def _pendo_deduction(cv, pv, has_pendo):
            if not has_pendo: return 0
            if pv > 0 and cv == 0: return 40
            if pv > 0:
                pct = ((cv - pv) / pv) * 100
                if pct <= -50: return 35
                if pct <= -30: return 25
                if pct <= -10: return 12
            elif cv == 0 and pv == 0: return 20
            return 0

        def _freshdesk_deduction(crit, high):
            return min(35, (20 if crit > 0 else 0) + min(10, high * 5))

        prev_ren = _renewal_deduction(prev.get("renewal_days", 999))
        curr_ren = _renewal_deduction(curr.get("renewal_days", 999))
        ren_delta = curr_ren - prev_ren

        prev_pendo = _pendo_deduction(prev.get("current_visitors", 0), prev.get("previous_visitors", 0), prev.get("has_pendo", False))
        curr_pendo = _pendo_deduction(curr.get("current_visitors", 0), curr.get("previous_visitors", 0), curr.get("has_pendo", False))
        pendo_delta = curr_pendo - prev_pendo

        prev_fd = _freshdesk_deduction(prev.get("open_critical", 0), prev.get("open_high", 0))
        curr_fd = _freshdesk_deduction(curr.get("open_critical", 0), curr.get("open_high", 0))
        fd_delta = curr_fd - prev_fd

        if ren_delta != 0:
            prev_rd = prev.get("renewal_days", 999)
            curr_rd = curr.get("renewal_days", 999)
            pts = abs(ren_delta)
            if ren_delta < 0:
                if curr_rd > 180:
                    reasons.append(f"Renewal risk cleared — now {curr_rd}d away (+{pts}pts)")
                else:
                    reasons.append(f"Renewal pressure eased — {prev_rd}d → {curr_rd}d (+{pts}pts)")
            else:
                reasons.append(f"Renewal approaching — {prev_rd}d → {curr_rd}d (-{pts}pts)")

        if pendo_delta != 0:
            cv = curr.get("current_visitors", 0)
            pv = curr.get("previous_visitors", 0)
            pts = abs(pendo_delta)
            if pendo_delta < 0:
                reasons.append(f"Usage decline narrowed below penalty threshold — visitors {pv} → {cv} (+{pts}pts)")
            else:
                if cv == 0 and pv > 0:
                    reasons.append(f"Product usage dropped to zero from {pv} visitors (-{pts}pts)")
                else:
                    reasons.append(f"Product usage declined — visitors {pv} → {cv} (-{pts}pts)")

        if fd_delta != 0:
            pc = prev.get("open_critical", 0)
            ph = prev.get("open_high", 0)
            cc = curr.get("open_critical", 0)
            ch = curr.get("open_high", 0)
            pts = abs(fd_delta)
            if fd_delta < 0:
                parts = []
                if pc > cc: parts.append(f"critical {pc}→{cc}")
                if ph > ch: parts.append(f"high {ph}→{ch}")
                reasons.append(f"Support tickets resolved — {', '.join(parts) if parts else f'{pc}c/{ph}h → {cc}c/{ch}h'} (+{pts}pts)")
            else:
                parts = []
                if cc > pc: parts.append(f"{cc} critical opened")
                if ch > ph: parts.append(f"{ch - ph} new high priority")
                reasons.append(f"Support escalation — {', '.join(parts) if parts else f'{cc} critical/{ch} high open'} (-{pts}pts)")

        if not reasons:
            reasons.append("Score factors shifted across category thresholds")

        return ". ".join(reasons)

    def get_health_distribution_changes(self, days: int = 30, account_type: Optional[str] = None) -> dict:
        """Get daily health distribution snapshots and account movements between days."""
        HEALTH_TABLE = "silver.silver_layer.account_health_scores_history"

        with self.get_connection() as conn:
            if conn is None:
                logger.warning("No DB connection for health changes")
                return {"days": [], "today_delta": None}
            try:
                cursor = conn.cursor()

                type_join = ""
                type_filter = ""
                if account_type and account_type.lower() not in ("all", ""):
                    safe_type = self._sql_escape(account_type)
                    type_join = f"JOIN {DIM_CUSTOMERS_TABLE} dc ON h.account_id = dc.account_id AND dc._fivetran_deleted = false"
                    type_filter = f"AND dc.account_type = '{safe_type}'"

                query = f"""
                    SELECT h.score_date, h.account_id, h.account_name,
                           h.health_score, h.health_category,
                           h.renewal_days, h.current_visitors, h.previous_visitors,
                           h.open_critical, h.open_high, h.has_pendo, h.has_freshdesk
                    FROM {HEALTH_TABLE} h
                    {type_join}
                    WHERE h.score_date >= DATE_SUB(CURRENT_DATE(), {int(days) + 1})
                    {type_filter}
                    ORDER BY h.score_date ASC, h.account_name ASC
                """
                cursor.execute(query)
                rows = cursor.fetchall()
                cursor.close()

                from collections import defaultdict
                by_date = defaultdict(dict)
                for row in rows:
                    score_date = row[0]
                    by_date[score_date][row[1]] = {
                        "account_id": row[1],
                        "account_name": row[2],
                        "health_score": int(row[3]) if row[3] is not None else 0,
                        "health_category": row[4] or "Good",
                        "renewal_days": int(row[5]) if row[5] is not None else 999,
                        "current_visitors": int(row[6]) if row[6] is not None else 0,
                        "previous_visitors": int(row[7]) if row[7] is not None else 0,
                        "open_critical": int(row[8]) if row[8] is not None else 0,
                        "open_high": int(row[9]) if row[9] is not None else 0,
                        "has_pendo": bool(row[10]) if row[10] is not None else False,
                        "has_freshdesk": bool(row[11]) if row[11] is not None else False,
                    }

                sorted_dates = sorted(by_date.keys())
                result_days = []

                for i, d in enumerate(sorted_dates):
                    accounts = by_date[d]
                    good = sum(1 for a in accounts.values() if a["health_category"] == "Good")
                    at_risk = sum(1 for a in accounts.values() if a["health_category"] == "At Risk")
                    critical = sum(1 for a in accounts.values() if a["health_category"] == "Critical")

                    improved = []
                    worsened = []

                    if i > 0:
                        prev_d = sorted_dates[i - 1]
                        prev_accounts = by_date[prev_d]
                        cat_rank = {"Good": 0, "At Risk": 1, "Critical": 2}

                        for aid, curr in accounts.items():
                            if aid in prev_accounts:
                                prev = prev_accounts[aid]
                                if curr["health_category"] != prev["health_category"]:
                                    explanation = self._explain_health_movement(prev, curr)
                                    lookback = sorted_dates[max(0, i - 6):i + 1]
                                    recent = [by_date[dd][aid]["health_score"] for dd in lookback if aid in by_date[dd]]
                                    movement = {
                                        "account_id": aid,
                                        "account_name": curr["account_name"],
                                        "prev_score": prev["health_score"],
                                        "curr_score": curr["health_score"],
                                        "prev_category": prev["health_category"],
                                        "curr_category": curr["health_category"],
                                        "explanation": explanation,
                                        "recent_scores": recent,
                                    }
                                    if cat_rank.get(curr["health_category"], 0) < cat_rank.get(prev["health_category"], 0):
                                        improved.append(movement)
                                    else:
                                        worsened.append(movement)

                    result_days.append({
                        "date": d,
                        "prev_date": sorted_dates[i - 1] if i > 0 else None,
                        "good": good,
                        "at_risk": at_risk,
                        "critical": critical,
                        "improved": sorted(improved, key=lambda x: x["curr_score"] - x["prev_score"], reverse=True),
                        "worsened": sorted(worsened, key=lambda x: x["curr_score"] - x["prev_score"]),
                    })

                result_days.reverse()

                today_delta = None
                if len(result_days) >= 2:
                    today_delta = {
                        "good": result_days[0]["good"] - result_days[1]["good"],
                        "at_risk": result_days[0]["at_risk"] - result_days[1]["at_risk"],
                        "critical": result_days[0]["critical"] - result_days[1]["critical"],
                    }

                logger.info(f"Health changes: {len(result_days)} days, {sum(len(d['improved']) + len(d['worsened']) for d in result_days)} total movements")
                return {"days": result_days, "today_delta": today_delta}

            except Exception as e:
                logger.warning(f"Failed to get health distribution changes: {e}")
                return {"days": [], "today_delta": None}

    def get_weekly_summaries(self, account_id: str, limit: int = 12, offset: int = 0) -> dict:
        """Fetch pre-computed weekly summaries for an account."""
        SUMMARIES_TABLE = "silver.silver_layer.account_weekly_summaries"

        with self.get_connection() as conn:
            if conn is None:
                logger.warning("No DB connection for weekly summaries")
                return {"weeks": [], "total_weeks": 0}
            try:
                cursor = conn.cursor()
                safe_id = account_id.replace("'", "''")

                cursor.execute(f"""
                    SELECT COUNT(*) FROM {SUMMARIES_TABLE}
                    WHERE account_id = '{safe_id}'
                """)
                total = cursor.fetchone()[0] or 0

                cursor.execute(f"""
                    SELECT account_id, account_name, week_start, week_end,
                           narrative, gong_summary, generated_at
                    FROM {SUMMARIES_TABLE}
                    WHERE account_id = '{safe_id}'
                    ORDER BY week_start DESC
                    LIMIT {int(limit)} OFFSET {int(offset)}
                """)
                rows = cursor.fetchall()
                cursor.close()

                weeks = []
                for row in rows:
                    weeks.append({
                        "account_id": row[0],
                        "account_name": row[1],
                        "week_start": row[2],
                        "week_end": row[3],
                        "narrative": row[4] or "",
                        "gong_summary": row[5] or None,
                        "generated_at": row[6],
                    })

                logger.info(f"Weekly summaries for {account_id}: {len(weeks)} of {total}")
                return {"weeks": weeks, "total_weeks": total}

            except Exception as e:
                logger.warning(f"Failed to get weekly summaries for {account_id}: {e}")
                return {"weeks": [], "total_weeks": 0}

    def _calculate_at_risk_count_by_health_score(
        self, 
        conn, 
        account_type: Optional[str] = None
    ) -> Tuple[int, int, int]:
        """
        Calculate count of accounts by health score category.
        
        First tries to use pre-computed health scores (fast).
        Falls back to SQL-based approximation if pre-computed not available.
        
        Returns (good_count, at_risk_count, critical_count):
        - Good: health score >= 70
        - At Risk: score 40-69
        - Critical: score < 40
        """
        # Try pre-computed first (much faster)
        precomputed = self._get_health_counts_from_precomputed(conn, account_type)
        if precomputed is not None:
            return precomputed
        
        # Fall back to SQL-based approximation
        logger.info("Falling back to SQL-based health score estimation")
        FCT_TABLE = FCT_CONTRACTS_TABLE
        
        try:
            cursor = conn.cursor()
            
            # Build base conditions
            type_condition = ""
            if account_type:
                safe_type = self._sql_escape(account_type)
                type_condition = f"AND c.account_type = '{safe_type}'"
            
            # Fast SQL query that estimates health score categories
            # Based on renewal days (primary factor) + critical/high Freshdesk tickets
            # FIXED: Removed strict filters to match web app's actual renewal logic
            query = f"""
                WITH account_renewal AS (
                    SELECT 
                        c.account_id,
                        c.account,
                        MIN(
                            CASE 
                                WHEN fct.RENEWAL_NOT_YET_CONTRACTED = 'Y' 
                                    AND fct.rev_rec_end_date IS NOT NULL
                                THEN DATEDIFF(fct.REV_REC_END_DATE, CURRENT_DATE())
                                ELSE 9999
                            END
                        ) as renewal_days
                    FROM {DIM_CUSTOMERS_TABLE} c
                    LEFT JOIN {FCT_TABLE} fct ON c.account_id = fct.account_id
                    WHERE c._fivetran_deleted = false
                    AND COALESCE(c.account_type, '') != 'Churn'
                    {type_condition}
                    AND c.account_id NOT IN (
                        SELECT fc.account_id
                        FROM {FCT_TABLE} fc
                        WHERE fc.account_id IS NOT NULL
                        GROUP BY fc.account_id
                        HAVING COUNT(*) = SUM(
                            CASE WHEN fc.renewal_not_yet_contracted = 'Y'
                                  AND fc.churn_expected_occurred = 'Y'
                            THEN 1 ELSE 0 END
                        )
                    )
                    GROUP BY c.account_id, c.account
                ),
                account_tickets AS (
                    SELECT 
                        fc.name as account_name,
                        SUM(CASE WHEN t.priority = 'Urgent' AND t.label_for_customer NOT IN ('Closed', 'Resolved') THEN 1 ELSE 0 END) as critical_tickets,
                        SUM(CASE WHEN t.priority = 'High' AND t.label_for_customer NOT IN ('Closed', 'Resolved') THEN 1 ELSE 0 END) as high_tickets
                    FROM {DIM_FRESHDESK_CUSTOMERS_TABLE} fc
                    LEFT JOIN {FCT_FRESHDESK_TICKETS_TABLE} t ON t.company_id = fc.id
                    GROUP BY fc.name
                ),
                health_estimate AS (
                    SELECT 
                        ar.account_id,
                        ar.account,
                        ar.renewal_days,
                        COALESCE(at.critical_tickets, 0) as critical_tickets,
                        COALESCE(at.high_tickets, 0) as high_tickets,
                        -- Calculate estimated deduction
                        (
                            -- Renewal deduction
                            CASE 
                                WHEN ar.renewal_days <= 30 THEN 25
                                WHEN ar.renewal_days <= 60 THEN 18
                                WHEN ar.renewal_days <= 90 THEN 12
                                WHEN ar.renewal_days <= 180 THEN 5
                                ELSE 0
                            END
                            +
                            -- Support deduction (simplified)
                            CASE WHEN COALESCE(at.critical_tickets, 0) > 0 THEN 15 ELSE 0 END
                            +
                            LEAST(10, COALESCE(at.high_tickets, 0) * 5)
                        ) as total_deduction
                    FROM account_renewal ar
                    LEFT JOIN account_tickets at ON at.account_name = ar.account
                )
                SELECT 
                    SUM(CASE WHEN (100 - total_deduction) >= 70 THEN 1 ELSE 0 END) as good_count,
                    SUM(CASE WHEN (100 - total_deduction) >= 40 AND (100 - total_deduction) < 70 THEN 1 ELSE 0 END) as at_risk_count,
                    SUM(CASE WHEN (100 - total_deduction) < 40 THEN 1 ELSE 0 END) as critical_count
                FROM health_estimate
            """
            
            cursor.execute(query)
            row = cursor.fetchone()
            cursor.close()
            
            good_count = int(row[0]) if row and row[0] else 0
            at_risk_count = int(row[1]) if row and row[1] else 0
            critical_count = int(row[2]) if row and row[2] else 0
            
            logger.info(f"Health score counts (SQL estimate): good={good_count}, at_risk={at_risk_count}, critical={critical_count}")
            return good_count, at_risk_count, critical_count
            
        except Exception as e:
            logger.warning(f"Error calculating health score counts: {e}")
            import traceback
            logger.warning(f"Traceback: {traceback.format_exc()}")
            # Return zeros on error
            return 0, 0, 0

    def _get_health_scores_sql(
        self, 
        conn, 
        account_type: Optional[str] = None,
        owner_filter: Optional[str] = None,
        search: Optional[str] = None,
    ) -> dict:
        """
        Get health scores for ALL accounts using a single fast SQL query.
        Returns dict[account_id -> {score, category, renewal_days, has_freshdesk_issues}]
        
        This is MUCH faster than calculating in Python for each account.
        Uses the same scoring logic as calculate_health_score_detail but in SQL.
        """
        FCT_TABLE = FCT_CONTRACTS_TABLE
        PENDO_ACCOUNTS = "silver.silver_layer.dim_pendo_account_customers"
        PENDO_ACCOUNT_DAILY = "silver.silver_layer.fct_pendo_account_daily_metrics"
        
        result = {}
        
        try:
            cursor = conn.cursor()
            
            # Build filter conditions
            where_conditions = [
                "c._fivetran_deleted = false",
                "COALESCE(c.account_type, '') != 'Churn'"
            ]
            
            if account_type:
                safe_type = self._sql_escape(account_type)
                where_conditions.append(f"c.account_type = '{safe_type}'")
            
            if owner_filter:
                safe_owner = self._sql_escape(owner_filter)
                where_conditions.append(f"csm_user.name = '{safe_owner}'")
            
            if search:
                safe_search = self._sql_escape(search)
                where_conditions.append(f"LOWER(c.account) LIKE LOWER('%{safe_search}%')")
            
            # Exclude fully churned accounts
            where_conditions.append(f"""
                c.account_id NOT IN (
                    SELECT fc.account_id
                    FROM {FCT_TABLE} fc
                    WHERE fc.account_id IS NOT NULL
                    GROUP BY fc.account_id
                    HAVING COUNT(*) = SUM(
                        CASE WHEN fc.renewal_not_yet_contracted = 'Y'
                              AND fc.churn_expected_occurred = 'Y'
                        THEN 1 ELSE 0 END
                    )
                )
            """)
            
            where_clause = " AND ".join(where_conditions)
            
            # Single comprehensive SQL query for health scores
            query = f"""
                WITH account_base AS (
                    SELECT 
                        c.account_id,
                        c.account as name,
                        c.industry,
                        csm_user.name AS csm_name,
                        c.parent_id,
                        c.parent_name,
                        c.account_executive
                    FROM {DIM_CUSTOMERS_TABLE} c
                    LEFT JOIN {DIM_USERS_TABLE} csm_user ON c.csm_c = csm_user.id
                    WHERE {where_clause}
                ),
                account_renewal AS (
                    SELECT 
                        ab.account_id,
                        ab.name,
                        ab.industry,
                        ab.csm_name,
                        ab.parent_id,
                        ab.parent_name,
                        ab.account_executive,
                        MIN(
                            CASE 
                                WHEN fct.RENEWAL_NOT_YET_CONTRACTED = 'Y' 
                                    AND fct.revenue_type NOT IN ('Services', 'Perpetual')
                                    AND (fct.churn_expected_occurred IS NULL OR fct.churn_expected_occurred = 'nan')
                                    AND fct.rev_rec_end_date > CURRENT_DATE()
                                THEN DATEDIFF(fct.REV_REC_END_DATE, CURRENT_DATE())
                                ELSE 9999
                            END
                        ) as renewal_days,
                        MIN(fct.REV_REC_END_DATE) as renewal_date
                    FROM account_base ab
                    LEFT JOIN {FCT_TABLE} fct ON ab.account_id = fct.account_id
                        AND fct.RENEWAL_NOT_YET_CONTRACTED = 'Y'
                        AND fct.revenue_type NOT IN ('Services', 'Perpetual')
                        AND (fct.churn_expected_occurred IS NULL OR fct.churn_expected_occurred = 'nan')
                        AND fct.rev_rec_end_date > CURRENT_DATE()
                    GROUP BY ab.account_id, ab.name, ab.industry, ab.csm_name, ab.parent_id, ab.parent_name, ab.account_executive
                ),
                account_tickets AS (
                    SELECT 
                        LOWER(TRIM(fc.name)) as account_name_lower,
                        SUM(CASE WHEN t.label_for_customer NOT IN ('Closed', 'Resolved') AND t.priority = 'Urgent' THEN 1 ELSE 0 END) as critical_tickets,
                        SUM(CASE WHEN t.label_for_customer NOT IN ('Closed', 'Resolved') AND t.priority = 'High' THEN 1 ELSE 0 END) as high_tickets,
                        COUNT(CASE WHEN t.label_for_customer NOT IN ('Closed', 'Resolved') THEN 1 END) as open_tickets
                    FROM {DIM_FRESHDESK_CUSTOMERS_TABLE} fc
                    LEFT JOIN {FCT_FRESHDESK_TICKETS_TABLE} t ON t.company_id = fc.id
                    WHERE fc._fivetran_deleted = false
                    GROUP BY LOWER(TRIM(fc.name))
                ),
                account_pendo AS (
                    SELECT 
                        LOWER(TRIM(pc.name)) as account_name_lower,
                        SUM(CASE WHEN p.date_day >= DATE_SUB(CURRENT_DATE(), 30) THEN COALESCE(p.count_active_visitors, 0) ELSE 0 END) as current_visitors,
                        SUM(CASE WHEN p.date_day >= DATE_SUB(CURRENT_DATE(), 60) AND p.date_day < DATE_SUB(CURRENT_DATE(), 30) THEN COALESCE(p.count_active_visitors, 0) ELSE 0 END) as previous_visitors
                    FROM {PENDO_ACCOUNTS} pc
                    LEFT JOIN {PENDO_ACCOUNT_DAILY} p ON p.account_id = pc.id AND p.date_day >= DATE_SUB(CURRENT_DATE(), 60)
                    GROUP BY LOWER(TRIM(pc.name))
                ),
                health_calc AS (
                    SELECT 
                        ar.*,
                        COALESCE(at.critical_tickets, 0) as critical_tickets,
                        COALESCE(at.high_tickets, 0) as high_tickets,
                        COALESCE(at.open_tickets, 0) as open_tickets,
                        COALESCE(ap.current_visitors, 0) as pendo_current,
                        COALESCE(ap.previous_visitors, 0) as pendo_previous,
                        -- Renewal deduction (max 25)
                        CASE 
                            WHEN ar.renewal_days <= 30 THEN 25
                            WHEN ar.renewal_days <= 60 THEN 18
                            WHEN ar.renewal_days <= 90 THEN 12
                            WHEN ar.renewal_days <= 180 THEN 5
                            ELSE 0
                        END as renewal_deduction,
                        -- Pendo deduction (max 40)
                        CASE
                            WHEN ap.previous_visitors > 0 AND ap.current_visitors = 0 THEN 40
                            WHEN ap.previous_visitors > 0 AND ((ap.current_visitors - ap.previous_visitors) * 100.0 / ap.previous_visitors) <= -50 THEN 35
                            WHEN ap.previous_visitors > 0 AND ((ap.current_visitors - ap.previous_visitors) * 100.0 / ap.previous_visitors) <= -30 THEN 25
                            WHEN ap.previous_visitors > 0 AND ((ap.current_visitors - ap.previous_visitors) * 100.0 / ap.previous_visitors) <= -10 THEN 12
                            WHEN ap.current_visitors = 0 AND ap.previous_visitors = 0 AND ap.account_name_lower IS NOT NULL THEN 20
                            ELSE 0
                        END as pendo_deduction,
                        -- Freshdesk deduction (max 35)
                        LEAST(35,
                            CASE WHEN COALESCE(at.critical_tickets, 0) > 0 THEN 20 ELSE 0 END
                            + LEAST(10, COALESCE(at.high_tickets, 0) * 5)
                        ) as freshdesk_deduction,
                        -- Has data flags
                        CASE WHEN ap.account_name_lower IS NOT NULL THEN true ELSE false END as has_pendo,
                        CASE WHEN at.account_name_lower IS NOT NULL THEN true ELSE false END as has_freshdesk
                    FROM account_renewal ar
                    LEFT JOIN account_tickets at ON LOWER(TRIM(ar.name)) = at.account_name_lower
                    LEFT JOIN account_pendo ap ON LOWER(TRIM(ar.name)) = ap.account_name_lower
                )
                SELECT 
                    account_id,
                    name,
                    industry,
                    csm_name,
                    parent_id,
                    parent_name,
                    account_executive,
                    renewal_days,
                    renewal_date,
                    critical_tickets,
                    high_tickets,
                    open_tickets,
                    pendo_current,
                    pendo_previous,
                    has_pendo,
                    has_freshdesk,
                    renewal_deduction,
                    pendo_deduction,
                    freshdesk_deduction,
                    (100 - renewal_deduction - pendo_deduction - freshdesk_deduction) as health_score,
                    CASE 
                        WHEN (100 - renewal_deduction - pendo_deduction - freshdesk_deduction) >= 70 THEN 'Good'
                        WHEN (100 - renewal_deduction - pendo_deduction - freshdesk_deduction) >= 40 THEN 'At Risk'
                        ELSE 'Critical'
                    END as health_category
                FROM health_calc
                ORDER BY name ASC
            """
            
            cursor.execute(query)
            rows = cursor.fetchall()
            cursor.close()
            
            for row in rows:
                account_id = row[0]
                if account_id:
                    result[account_id] = {
                        "account_id": account_id,
                        "name": row[1],
                        "industry": row[2],
                        "csm_name": row[3],
                        "parent_id": row[4],
                        "parent_name": row[5],
                        "account_executive": row[6],
                        "renewal_days": int(row[7]) if row[7] is not None else 999,
                        "renewal_date": row[8],
                        "critical_tickets": int(row[9] or 0),
                        "high_tickets": int(row[10] or 0),
                        "open_tickets": int(row[11] or 0),
                        "pendo_current": int(row[12] or 0),
                        "pendo_previous": int(row[13] or 0),
                        "has_pendo": bool(row[14]),
                        "has_freshdesk": bool(row[15]),
                        "renewal_deduction": int(row[16] or 0),
                        "pendo_deduction": int(row[17] or 0),
                        "freshdesk_deduction": int(row[18] or 0),
                        "health_score": int(row[19]) if row[19] is not None else 100,
                        "health_category": row[20] or "Good",
                    }
            
            logger.info(f"SQL health scores returned for {len(result)} accounts")
            return result
            
        except Exception as e:
            logger.warning(f"Error getting SQL health scores: {e}")
            import traceback
            logger.warning(f"Traceback: {traceback.format_exc()}")
            return result

    def get_metrics_summary(self, account_type: Optional[str] = None, renewal_period: int = 90) -> MetricsSummary:
        """Get dashboard KPI metrics from dim_customers + fct_contracts for renewals."""
        logger.info(f"get_metrics_summary called with account_type={account_type}, renewal_period={renewal_period}")
        FCT_TABLE = FCT_CONTRACTS_TABLE
        with self.get_connection() as conn:
            if conn is None:
                logger.error("Connection is None - cannot fetch metrics")
                raise Exception("Database connection failed")

            try:
                where_conditions = ["_fivetran_deleted = false", "COALESCE(account_type, '') != 'Churn'"]
                if account_type:
                    safe_type = self._sql_escape(account_type)
                    where_conditions.append(f"account_type = '{safe_type}'")

                # Exclude fully churned accounts
                churned_subquery = f"""
                    account_id NOT IN (
                        SELECT c.account_id
                        FROM {FCT_TABLE} c
                        WHERE c.account_id IS NOT NULL
                        GROUP BY c.account_id
                        HAVING COUNT(*) = SUM(
                            CASE WHEN c.renewal_not_yet_contracted = 'Y'
                                  AND c.churn_expected_occurred = 'Y'
                            THEN 1 ELSE 0 END
                        )
                    )
                """
                where_conditions.append(churned_subquery)
                where_clause = " AND ".join(where_conditions)

                cursor = conn.cursor()

                # Main dim_customers query - just count accounts (engagement/renewal columns removed from schema)
                cursor.execute(f"""
                    SELECT COUNT(*) as total_accounts
                    FROM {DIM_CUSTOMERS_TABLE}
                    WHERE {where_clause}
                """)
                row = cursor.fetchone()
                logger.info(f"dim_customers query returned: {row}")

                total = row[0] or 0
                
                # Health distribution based on actual health scores (considers renewal, Pendo, Freshdesk)
                good, at_risk, critical = self._calculate_at_risk_count_by_health_score(conn, account_type)

                # Renewal KPI from fct_contracts (EUR, dynamic period)
                fct_where = f"""
                    c.RENEWAL_NOT_YET_CONTRACTED = 'Y'
                    AND c.revenue_type NOT IN ('Services', 'Perpetual')
                    AND (c.churn_expected_occurred IS NULL OR c.churn_expected_occurred = 'nan')
                    AND c.rev_rec_end_date > CURRENT_DATE()
                    AND c.rev_rec_end_date <= DATE_ADD(CURRENT_DATE(), {int(renewal_period)})
                """
                if account_type:
                    fct_where += f"\n                    AND dc.account_type = '{safe_type}'"

                renewal_query = f"""
                    SELECT
                        COALESCE(ROUND(SUM(c.ARR_EUR), 0), 0) AS renewals_arr_eur,
                        COUNT(DISTINCT c.account_id) AS renewals_count
                    FROM {FCT_TABLE} c
                    LEFT JOIN {DIM_CUSTOMERS_TABLE} dc
                        ON c.account_id = dc.account_id
                    WHERE {fct_where}
                        AND dc._fivetran_deleted = false
                """
                logger.info(f"Executing renewal query with period={renewal_period}")
                cursor.execute(renewal_query)
                ren_row = cursor.fetchone()
                logger.info(f"fct_contracts renewal query returned: {ren_row}")

                renewals_arr = float(ren_row[0]) if ren_row and ren_row[0] else 0.0
                renewals_count = int(ren_row[1]) if ren_row and ren_row[1] else 0

                # Portfolio ARR from fct_contracts (EUR, current calendar year)
                arr_where = f"""
                    c.RENEWAL_NOT_YET_CONTRACTED = 'Y'
                    AND c.revenue_type NOT IN ('Services', 'Perpetual')
                    AND (c.churn_expected_occurred IS NULL OR c.churn_expected_occurred = 'nan')
                    AND c.rev_rec_end_date > CURRENT_DATE()
                    AND YEAR(c.rev_rec_end_date) = YEAR(CURRENT_DATE())
                """
                if account_type:
                    arr_where += f"\n                    AND dc.account_type = '{safe_type}'"

                arr_query = f"""
                    SELECT COALESCE(ROUND(SUM(c.ARR_EUR), 0), 0) AS portfolio_arr_eur
                    FROM {FCT_TABLE} c
                    LEFT JOIN {DIM_CUSTOMERS_TABLE} dc
                        ON c.account_id = dc.account_id
                    WHERE {arr_where}
                        AND dc._fivetran_deleted = false
                """
                logger.info("Executing portfolio ARR query (EUR, current year)")
                cursor.execute(arr_query)
                arr_row = cursor.fetchone()
                portfolio_arr_eur = float(arr_row[0]) if arr_row and arr_row[0] else 0.0
                logger.info(f"Portfolio ARR EUR (CY): {portfolio_arr_eur}")

                cursor.close()

                # Get usage decline count from pre-computed table
                usage_decline = self._get_usage_decline_count_from_precomputed(conn, account_type)

                # Day-over-day deltas from pre-computed health scores
                at_risk_delta = None
                usage_decline_delta = None
                try:
                    HEALTH_T = "silver.silver_layer.account_health_scores_history"
                    type_join_d = ""
                    type_filter_d = ""
                    if account_type and account_type.lower() not in ("all", ""):
                        type_join_d = f"JOIN {DIM_CUSTOMERS_TABLE} dc2 ON h2.account_id = dc2.account_id AND dc2._fivetran_deleted = false"
                        type_filter_d = f"AND dc2.account_type = '{safe_type}'"

                    delta_cursor = conn.cursor()
                    delta_cursor.execute(f"""
                        WITH dates AS (
                            SELECT MAX(score_date) AS today,
                                   (SELECT MAX(score_date) FROM {HEALTH_T} WHERE score_date < (SELECT MAX(score_date) FROM {HEALTH_T})) AS yesterday
                            FROM {HEALTH_T}
                        ),
                        today_counts AS (
                            SELECT
                                SUM(CASE WHEN h2.health_category IN ('At Risk', 'Critical') THEN 1 ELSE 0 END) AS at_risk,
                                SUM(CASE WHEN h2.has_pendo = true AND h2.previous_visitors > 0
                                         AND ((h2.current_visitors - h2.previous_visitors) * 100.0 / h2.previous_visitors) <= -20
                                    THEN 1 ELSE 0 END) AS usage_decline
                            FROM {HEALTH_T} h2
                            {type_join_d}
                            CROSS JOIN dates d
                            WHERE h2.score_date = d.today {type_filter_d}
                        ),
                        yesterday_counts AS (
                            SELECT
                                SUM(CASE WHEN h2.health_category IN ('At Risk', 'Critical') THEN 1 ELSE 0 END) AS at_risk,
                                SUM(CASE WHEN h2.has_pendo = true AND h2.previous_visitors > 0
                                         AND ((h2.current_visitors - h2.previous_visitors) * 100.0 / h2.previous_visitors) <= -20
                                    THEN 1 ELSE 0 END) AS usage_decline
                            FROM {HEALTH_T} h2
                            {type_join_d}
                            CROSS JOIN dates d
                            WHERE h2.score_date = d.yesterday {type_filter_d}
                        )
                        SELECT
                            COALESCE(t.at_risk, 0) - COALESCE(y.at_risk, 0),
                            COALESCE(t.usage_decline, 0) - COALESCE(y.usage_decline, 0)
                        FROM today_counts t, yesterday_counts y
                    """)
                    delta_row = delta_cursor.fetchone()
                    delta_cursor.close()
                    if delta_row:
                        at_risk_delta = int(delta_row[0]) if delta_row[0] is not None else None
                        usage_decline_delta = int(delta_row[1]) if delta_row[1] is not None else None
                    logger.info(f"KPI deltas: at_risk={at_risk_delta}, usage_decline={usage_decline_delta}")
                except Exception as de:
                    logger.warning(f"Failed to calculate KPI deltas: {de}")

                metrics = MetricsSummary(
                    total_accounts=total,
                    total_arr=portfolio_arr_eur,
                    renewals_arr=renewals_arr,
                    renewals_count=renewals_count,
                    health_distribution=HealthDistribution(
                        good=max(0, good),
                        at_risk=at_risk,
                        critical=critical,
                    ),
                    at_risk_count=critical + at_risk,
                    renewals_90_days=renewals_count,
                    usage_decline_count=usage_decline,
                    expansion_signals=0,
                    at_risk_delta=at_risk_delta,
                    usage_decline_delta=usage_decline_delta,
                )
                logger.info(f"Returning metrics: renewals_arr={renewals_arr}, renewals_count={renewals_count}")
                return metrics
            except Exception as e:
                logger.error(f"Error fetching metrics: {type(e).__name__}: {e}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
                raise Exception(f"Failed to fetch metrics: {str(e)}")

    def get_account_type_counts(self) -> dict:
        """Get count of accounts per account type from dim_customers."""
        logger.info("get_account_type_counts called")
        with self.get_connection() as conn:
            if conn is None:
                raise Exception("Database connection failed")
            try:
                cursor = conn.cursor()
                query = f"""
                    SELECT 
                        COALESCE(account_type, 'Unknown') as account_type,
                        COUNT(*) as cnt
                    FROM silver.silver_layer.dim_customers
                    WHERE _fivetran_deleted = false
                    AND COALESCE(account_type, '') != 'Churn'
                    AND account_id NOT IN (
                        SELECT fc.account_id
                        FROM {FCT_CONTRACTS_TABLE} fc
                        WHERE fc.account_id IS NOT NULL
                        GROUP BY fc.account_id
                        HAVING COUNT(*) = SUM(
                            CASE WHEN fc.renewal_not_yet_contracted = 'Y'
                                  AND fc.churn_expected_occurred = 'Y'
                            THEN 1 ELSE 0 END
                        )
                    )
                    GROUP BY account_type
                    ORDER BY cnt DESC
                """
                logger.info(f"Executing account type counts query: {query}")
                cursor.execute(query)
                rows = cursor.fetchall()
                counts = {}
                total = 0
                for row in rows:
                    acct_type = row[0] if row[0] else 'Unknown'
                    count = int(row[1])
                    counts[acct_type] = count
                    total += count
                counts['all'] = total
                logger.info(f"Account type counts: {counts}")
                return counts
            except Exception as e:
                logger.error(f"Error fetching account type counts: {e}")
                raise Exception(f"Failed to fetch account type counts: {str(e)}")

    def get_customer_growth(self, account_type: Optional[str] = None) -> CustomerGrowthResponse:
        """Get customer growth data: YoY, monthly series, events from new_customer_date."""
        logger.info(f"get_customer_growth called with account_type={account_type}")

        today = date.today()

        # Subquery to identify fully churned accounts
        churned_exclusion = f"""
            account_id NOT IN (
                SELECT fc.account_id
                FROM {FCT_CONTRACTS_TABLE} fc
                WHERE fc.account_id IS NOT NULL
                GROUP BY fc.account_id
                HAVING COUNT(*) = SUM(
                    CASE WHEN fc.renewal_not_yet_contracted = 'Y'
                          AND fc.churn_expected_occurred = 'Y'
                    THEN 1 ELSE 0 END
                )
            )
        """

        # Build filter for customers WITH a date
        dated_conditions = ["_fivetran_deleted = false", "COALESCE(account_type, '') != 'Churn'", "new_customer_date IS NOT NULL", churned_exclusion]
        if account_type:
            dated_conditions.append(f"account_type = '{account_type}'")
        dated_where = " AND ".join(dated_conditions)

        # Total count (includes those without new_customer_date)
        total_conditions = ["_fivetran_deleted = false", "COALESCE(account_type, '') != 'Churn'", churned_exclusion]
        if account_type:
            total_conditions.append(f"account_type = '{account_type}'")
        total_where = " AND ".join(total_conditions)

        with self.get_connection() as conn:
            if conn is None:
                raise Exception("Database connection failed")
            try:
                cursor = conn.cursor()

                # 1. Monthly series: count of new customers per month
                #    Use TRY_CAST to safely handle any malformed dates
                q1 = f"""
                    SELECT
                        YEAR(TRY_CAST(new_customer_date AS DATE)) AS yr,
                        MONTH(TRY_CAST(new_customer_date AS DATE)) AS mo,
                        COUNT(*) AS new_count
                    FROM {DIM_CUSTOMERS_TABLE}
                    WHERE {dated_where}
                      AND TRY_CAST(new_customer_date AS DATE) IS NOT NULL
                    GROUP BY YEAR(TRY_CAST(new_customer_date AS DATE)),
                             MONTH(TRY_CAST(new_customer_date AS DATE))
                    ORDER BY yr, mo
                """
                logger.info(f"Customer growth query 1: {q1}")
                cursor.execute(q1)
                monthly_raw = cursor.fetchall()
                logger.info(f"Monthly raw rows: {len(monthly_raw)}, sample: {monthly_raw[:3] if monthly_raw else 'empty'}")

                # 2. Total active customers now
                q2 = f"SELECT COUNT(*) FROM {DIM_CUSTOMERS_TABLE} WHERE {total_where}"
                cursor.execute(q2)
                total_now = cursor.fetchone()[0] or 0
                logger.info(f"Total customers now: {total_now}")

                # 3. Individual events (new customers) for the timeline
                FCT_TABLE = FCT_CONTRACTS_TABLE
                q3 = f"""
                    SELECT
                        c.account_id,
                        c.account AS name,
                        CAST(c.new_customer_date AS STRING) AS cust_date,
                        c.industry,
                        c.region
                    FROM {DIM_CUSTOMERS_TABLE} c
                    WHERE {dated_where.replace('_fivetran_deleted', 'c._fivetran_deleted').replace('new_customer_date', 'c.new_customer_date').replace('account_type', 'c.account_type')}
                      AND TRY_CAST(c.new_customer_date AS DATE) IS NOT NULL
                    ORDER BY c.new_customer_date DESC
                """
                cursor.execute(q3)
                event_rows = cursor.fetchall()
                cursor.close()
                logger.info(f"Event rows: {len(event_rows)}")

                # Process monthly series
                month_names = [
                    "", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
                ]

                monthly_map = {}
                for row in monthly_raw:
                    try:
                        yr_val = int(row[0])
                        mo_val = int(row[1])
                        cnt_val = int(row[2])
                        if 1 <= mo_val <= 12 and 1900 <= yr_val <= 2100:
                            monthly_map[(yr_val, mo_val)] = cnt_val
                    except (TypeError, ValueError) as parse_err:
                        logger.warning(f"Skipping bad monthly row {row}: {parse_err}")
                        continue

                logger.info(f"Monthly map: {monthly_map}")

                # Date range: always go back at least 24 months for YoY
                forced_min_yr = today.year - 2
                forced_min_mo = today.month

                if monthly_map:
                    data_min = min(monthly_map.keys())
                    data_max = max(monthly_map.keys())
                    min_yr, min_mo = min(data_min, (forced_min_yr, forced_min_mo))
                    max_yr, max_mo = max(data_max, (today.year, today.month))
                else:
                    min_yr, min_mo = forced_min_yr, forced_min_mo
                    max_yr, max_mo = today.year, today.month

                # Build full monthly series (fill gaps with 0)
                monthly_series = []
                loop_yr, loop_mo = min_yr, min_mo

                # One fake churn event in Dec 2025
                fake_churn_month = (2025, 12)
                fake_churn_count = 1

                # Running cumulative: customers without dates are the "base"
                total_new_all = sum(monthly_map.values())
                cumulative = total_now - total_new_all + fake_churn_count

                safety_limit = 600  # Max ~50 years to prevent infinite loop
                iteration = 0
                while (loop_yr, loop_mo) <= (max_yr, max_mo) and iteration < safety_limit:
                    iteration += 1
                    new_count = monthly_map.get((loop_yr, loop_mo), 0)
                    churn_count = fake_churn_count if (loop_yr, loop_mo) == fake_churn_month else 0
                    net = new_count - churn_count
                    cumulative += net

                    monthly_series.append(MonthlyGrowthPoint(
                        year=loop_yr,
                        month=loop_mo,
                        label=f"{month_names[loop_mo]} {loop_yr}",
                        new_count=new_count,
                        churn_count=churn_count,
                        net_change=net,
                        cumulative_total=cumulative,
                    ))

                    loop_mo += 1
                    if loop_mo > 12:
                        loop_mo = 1
                        loop_yr += 1

                logger.info(f"Monthly series built: {len(monthly_series)} points")

                # Calculate YoY by comparing total customer COUNTS
                # total_now = all active customers (including those with null dates)
                # last_year_count = total_now - (new in last 12m) + (churned in last 12m)
                new_last_12m = 0
                new_prior_12m = 0
                churn_last_12m = 0
                for pt in monthly_series:
                    months_ago = (today.year - pt.year) * 12 + (today.month - pt.month)
                    if 0 <= months_ago < 12:
                        new_last_12m += pt.new_count
                        churn_last_12m += pt.churn_count
                    elif 12 <= months_ago < 24:
                        new_prior_12m += pt.new_count

                # Total customers 12 months ago = current total minus net change
                last_year_count = total_now - new_last_12m + churn_last_12m

                yoy_pct = 0.0
                if last_year_count > 0:
                    yoy_pct = round(((total_now - last_year_count) / last_year_count) * 100, 1)
                elif total_now > 0:
                    yoy_pct = 100.0

                net_12m = new_last_12m - churn_last_12m
                avg_per_month = round(new_last_12m / 12, 1) if new_last_12m else 0.0

                logger.info(f"YoY calc: total_now={total_now}, last_year_count={last_year_count}, new_last_12m={new_last_12m}, churn_last_12m={churn_last_12m}, yoy_pct={yoy_pct}")

                summary = CustomerGrowthSummary(
                    new_last_12m=new_last_12m,
                    new_prior_12m=last_year_count,  # Total customers 12 months ago
                    yoy_growth_pct=yoy_pct,
                    total_customers_now=total_now,
                    net_change_12m=net_12m,
                    churn_last_12m=churn_last_12m,
                    avg_per_month=avg_per_month,
                )

                # Build events list
                events = []
                for row in event_rows:
                    events.append(CustomerEvent(
                        account_id=str(row[0]) if row[0] else "",
                        account_name=str(row[1]) if row[1] else "Unknown",
                        date=str(row[2]).strip() if row[2] else "",
                        event_type="new",
                        industry=str(row[3]) if row[3] and str(row[3]).lower() != 'none' else None,
                        region=str(row[4]) if row[4] and str(row[4]).lower() != 'none' else None,
                    ))

                # Add fake churn event
                events.append(CustomerEvent(
                    account_id="fake-churn-001",
                    account_name="Example Churned Corp",
                    date="2025-12-03",
                    event_type="churned",
                    industry=None,
                    region=None,
                ))
                events.sort(key=lambda e: e.date, reverse=True)

                logger.info(f"Customer growth: {summary}, {len(monthly_series)} months, {len(events)} events")
                return CustomerGrowthResponse(
                    summary=summary,
                    monthly_series=monthly_series,
                    events=events,
                )

            except Exception as e:
                logger.error(f"Error fetching customer growth: {e}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
                raise Exception(f"Failed to fetch customer growth: {str(e)}")

    def get_customer_growth_breakdown(
        self,
        dimension: str = "industry",
        account_type: Optional[str] = None,
    ):
        """Get customer growth broken down by a dimension (industry or region).
        Returns per-group cumulative totals over time."""
        from ..models.schemas import (
            GroupMonthlyPoint, GroupSeries, CustomerGrowthBreakdownResponse,
        )

        logger.info(f"get_customer_growth_breakdown: dimension={dimension}, account_type={account_type}")
        today = date.today()

        # Determine the column to group by
        FCT_TABLE = FCT_CONTRACTS_TABLE
        use_join = False
        if dimension == "region":
            # region is on fct_contracts, need a subquery/join
            group_col = "region"
            use_join = True
        else:
            # industry is on dim_customers directly
            group_col = "industry"

        churned_exclusion = f"""
            c.account_id NOT IN (
                SELECT fc.account_id
                FROM {FCT_CONTRACTS_TABLE} fc
                WHERE fc.account_id IS NOT NULL
                GROUP BY fc.account_id
                HAVING COUNT(*) = SUM(
                    CASE WHEN fc.renewal_not_yet_contracted = 'Y'
                          AND fc.churn_expected_occurred = 'Y'
                    THEN 1 ELSE 0 END
                )
            )
        """
        base_conditions = ["c._fivetran_deleted = false", "COALESCE(c.account_type, '') != 'Churn'", churned_exclusion]
        if account_type:
            base_conditions.append(f"c.account_type = '{account_type}'")
        base_where = " AND ".join(base_conditions)

        with self.get_connection() as conn:
            if conn is None:
                raise Exception("Database connection failed")
            try:
                cursor = conn.cursor()

                if use_join:
                    # For region: use region directly from dim_customers
                    q = f"""
                        SELECT
                            COALESCE(c.region, 'Unknown') AS grp,
                            YEAR(TRY_CAST(c.new_customer_date AS DATE)) AS yr,
                            MONTH(TRY_CAST(c.new_customer_date AS DATE)) AS mo,
                            COUNT(*) AS cnt
                        FROM {DIM_CUSTOMERS_TABLE} c
                        WHERE {base_where}
                          AND TRY_CAST(c.new_customer_date AS DATE) IS NOT NULL
                        GROUP BY COALESCE(c.region, 'Unknown'),
                                 YEAR(TRY_CAST(c.new_customer_date AS DATE)),
                                 MONTH(TRY_CAST(c.new_customer_date AS DATE))
                        ORDER BY grp, yr, mo
                    """

                    # Also get total counts per group (including those without dates)
                    q_totals = f"""
                        SELECT COALESCE(c.region, 'Unknown') AS grp, COUNT(*) AS total
                        FROM {DIM_CUSTOMERS_TABLE} c
                        WHERE {base_where}
                        GROUP BY COALESCE(c.region, 'Unknown')
                    """
                else:
                    # For industry: directly on dim_customers
                    q = f"""
                        SELECT
                            COALESCE({group_col}, 'Unknown') AS grp,
                            YEAR(TRY_CAST(new_customer_date AS DATE)) AS yr,
                            MONTH(TRY_CAST(new_customer_date AS DATE)) AS mo,
                            COUNT(*) AS cnt
                        FROM {DIM_CUSTOMERS_TABLE} c
                        WHERE {base_where}
                          AND TRY_CAST(new_customer_date AS DATE) IS NOT NULL
                        GROUP BY COALESCE({group_col}, 'Unknown'),
                                 YEAR(TRY_CAST(new_customer_date AS DATE)),
                                 MONTH(TRY_CAST(new_customer_date AS DATE))
                        ORDER BY grp, yr, mo
                    """

                    q_totals = f"""
                        SELECT COALESCE({group_col}, 'Unknown') AS grp, COUNT(*) AS total
                        FROM {DIM_CUSTOMERS_TABLE} c
                        WHERE {base_where}
                        GROUP BY COALESCE({group_col}, 'Unknown')
                    """

                logger.info(f"Breakdown query: {q[:200]}...")
                cursor.execute(q)
                rows = cursor.fetchall()
                logger.info(f"Breakdown rows: {len(rows)}")

                cursor.execute(q_totals)
                total_rows = cursor.fetchall()
                cursor.close()

                # Parse totals per group
                group_totals = {}
                for row in total_rows:
                    grp = str(row[0]) if row[0] else "Unknown"
                    group_totals[grp] = int(row[1])

                # Parse monthly data per group
                # {group_name: {(year, month): count_of_new}}
                group_monthly: dict = {}
                for row in rows:
                    try:
                        grp = str(row[0]) if row[0] else "Unknown"
                        yr = int(row[1])
                        mo = int(row[2])
                        cnt = int(row[3])
                        if grp not in group_monthly:
                            group_monthly[grp] = {}
                        group_monthly[grp][(yr, mo)] = cnt
                    except (TypeError, ValueError):
                        continue

                # Build date range (same logic as main chart)
                month_names = [
                    "", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
                ]

                all_keys = set()
                for gm in group_monthly.values():
                    all_keys.update(gm.keys())

                forced_min = (today.year - 2, today.month)
                if all_keys:
                    min_key = min(min(all_keys), forced_min)
                    max_key = max(max(all_keys), (today.year, today.month))
                else:
                    min_key = forced_min
                    max_key = (today.year, today.month)

                # Build series per group
                all_groups = set(group_monthly.keys()) | set(group_totals.keys())
                # Filter to top N groups by total, collapse rest to "Other"
                sorted_groups = sorted(all_groups, key=lambda g: group_totals.get(g, 0), reverse=True)
                TOP_N = 10
                top_groups = sorted_groups[:TOP_N]
                other_groups = sorted_groups[TOP_N:]

                result_groups = []
                for grp in top_groups:
                    monthly_data = group_monthly.get(grp, {})
                    total_now = group_totals.get(grp, 0)
                    total_new = sum(monthly_data.values())
                    cumulative = total_now - total_new  # base (customers without dates)

                    series_pts = []
                    yr, mo = min_key
                    while (yr, mo) <= max_key:
                        new_this_month = monthly_data.get((yr, mo), 0)
                        cumulative += new_this_month
                        series_pts.append(GroupMonthlyPoint(
                            year=yr,
                            month=mo,
                            label=f"{month_names[mo]} {yr}",
                            cumulative_total=cumulative,
                        ))
                        mo += 1
                        if mo > 12:
                            mo = 1
                            yr += 1

                    result_groups.append(GroupSeries(
                        group_name=grp,
                        series=series_pts,
                    ))

                # Collapse "Other" if needed
                if other_groups:
                    other_monthly: dict = {}
                    other_total = 0
                    for grp in other_groups:
                        other_total += group_totals.get(grp, 0)
                        for k, v in group_monthly.get(grp, {}).items():
                            other_monthly[k] = other_monthly.get(k, 0) + v

                    other_new = sum(other_monthly.values())
                    cumulative = other_total - other_new
                    series_pts = []
                    yr, mo = min_key
                    while (yr, mo) <= max_key:
                        cumulative += other_monthly.get((yr, mo), 0)
                        series_pts.append(GroupMonthlyPoint(
                            year=yr, month=mo,
                            label=f"{month_names[mo]} {yr}",
                            cumulative_total=cumulative,
                        ))
                        mo += 1
                        if mo > 12:
                            mo = 1
                            yr += 1
                    result_groups.append(GroupSeries(
                        group_name=f"Other ({len(other_groups)})",
                        series=series_pts,
                    ))

                # Sort by latest cumulative total descending
                result_groups.sort(
                    key=lambda g: g.series[-1].cumulative_total if g.series else 0,
                    reverse=True,
                )

                logger.info(f"Breakdown: {dimension}, {len(result_groups)} groups")
                return CustomerGrowthBreakdownResponse(
                    dimension=dimension,
                    groups=result_groups,
                )

            except Exception as e:
                logger.error(f"Error in customer growth breakdown: {e}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
                raise Exception(f"Failed to fetch growth breakdown: {str(e)}")

    def get_accounts(
        self,
        page: int = 1,
        page_size: int = 10,
        health_filter: Optional[str] = None,
        status_filter: Optional[str] = None,
        owner_filter: Optional[str] = None,
        search: Optional[str] = None,
        sort_by: str = "attention_first",
        kpi_filter: Optional[str] = None,
        account_type: Optional[str] = None,
    ) -> Tuple[List[Account], int]:
        """Get paginated list of accounts from dim_customers."""
        logger.info(f"get_accounts called: page={page}, page_size={page_size}, search={search}, kpi_filter={kpi_filter}, account_type={account_type}")
        with self.get_connection() as conn:
            if conn is None:
                raise Exception("Database connection failed — cannot load accounts")

            try:
                logger.info(f"Executing accounts query on table: {DIM_CUSTOMERS_TABLE}")
                cursor = conn.cursor()

                # Base query - include parent account info; renewals fetched in bulk later
                FCT_TABLE = FCT_CONTRACTS_TABLE
                base_query = f"""
                    SELECT 
                        c.account_id,
                        c.account AS name,
                        c.industry,
                        csm_user.name AS csm_name,
                        c.parent_id,
                        c.parent_name,
                        c.account_executive
                    FROM {DIM_CUSTOMERS_TABLE} c
                    LEFT JOIN {DIM_USERS_TABLE} csm_user ON c.csm_c = csm_user.id
                    WHERE c._fivetran_deleted = false
                    AND COALESCE(c.account_type, '') != 'Churn'
                    AND c.account_id NOT IN (
                        SELECT fc.account_id
                        FROM {FCT_TABLE} fc
                        WHERE fc.account_id IS NOT NULL
                        GROUP BY fc.account_id
                        HAVING COUNT(*) = SUM(
                            CASE WHEN fc.renewal_not_yet_contracted = 'Y'
                                  AND fc.churn_expected_occurred = 'Y'
                            THEN 1 ELSE 0 END
                        )
                    )
                """

                # Build WHERE clauses
                where_clauses = []
                params = []

                # Account type filter
                if account_type:
                    where_clauses.append("c.account_type = ?")
                    params.append(account_type)

                if search:
                    where_clauses.append("LOWER(c.account) LIKE LOWER(?)")
                    params.append(f"%{search}%")

                # KPI filters
                # Note: at_risk filter is applied after health score calculation (in Python)
                # renewals filter uses SQL for performance
                if kpi_filter:
                    if kpi_filter == "renewals":
                        where_clauses.append(f"""
                            c.account_id IN (
                                SELECT ACCOUNT_ID FROM {FCT_TABLE}
                                WHERE RENEWAL_NOT_YET_CONTRACTED = 'Y'
                                  AND revenue_type NOT IN ('Services', 'Perpetual')
                                  AND (churn_expected_occurred IS NULL OR churn_expected_occurred = 'nan')
                                  AND rev_rec_end_date > CURRENT_DATE()
                                  AND rev_rec_end_date <= DATE_ADD(CURRENT_DATE(), 90)
                                GROUP BY ACCOUNT_ID
                            )
                        """)

                if where_clauses:
                    base_query += " AND " + " AND ".join(where_clauses)

                # Ordering - use simple ordering (correlated subqueries not supported well in Databricks)
                base_query += " ORDER BY c.account ASC"

                # When using Python-side filters (at_risk, health_filter, status_filter),
                # we need to fetch ALL accounts to filter properly, then paginate in Python
                # Always fetch all accounts to get accurate at_risk count
                # Then paginate in Python
                cursor.execute(base_query, params)
                rows = cursor.fetchall()
                cursor.close()

                # Collect account IDs for bulk renewal query
                from ..models.schemas import RenewalInfo
                raw_accounts = []
                account_ids_for_renewals = []
                for row in rows:
                    # Query returns: account_id, name, industry, csm_name, parent_id, parent_name
                    account_id = row[0]
                    raw_accounts.append(row)
                    if account_id:
                        account_ids_for_renewals.append(account_id)

                # Bulk-fetch ALL contract renewals for these accounts
                renewals_by_account: dict = {}
                if account_ids_for_renewals:
                    try:
                        ids_in = ", ".join([f"'{self._sql_escape(aid)}'" for aid in account_ids_for_renewals])
                        renewal_cursor = conn.cursor()
                        renewal_query = f"""
                            SELECT
                                ACCOUNT_ID,
                                REVENUE_TYPE,
                                CONTRACT_GROUP,
                                REV_REC_END_DATE,
                                DATEDIFF(REV_REC_END_DATE, CURRENT_DATE()) AS renewal_days,
                                COALESCE(ARR_CAD, 0) AS arr_cad
                            FROM {FCT_TABLE}
                            WHERE RENEWAL_NOT_YET_CONTRACTED = 'Y'
                              AND ACCOUNT_ID IN ({ids_in})
                            ORDER BY ACCOUNT_ID, REV_REC_END_DATE ASC
                        """
                        renewal_cursor.execute(renewal_query)
                        renewal_rows = renewal_cursor.fetchall()
                        renewal_cursor.close()

                        def _parse_date_safe(val):
                            if val:
                                try:
                                    from datetime import datetime as dt
                                    return dt.strptime(str(val)[:10], "%Y-%m-%d").date()
                                except (ValueError, TypeError):
                                    pass
                            return None

                        for rr in renewal_rows:
                            aid = rr[0]
                            rev_type = str(rr[1]) if rr[1] else "Other"
                            cg = str(rr[2]) if rr[2] else None
                            end_date = _parse_date_safe(rr[3])
                            r_days = int(rr[4]) if rr[4] is not None else None
                            arr = float(rr[5]) if rr[5] is not None else None
                            if aid not in renewals_by_account:
                                renewals_by_account[aid] = []
                            renewals_by_account[aid].append(RenewalInfo(
                                revenue_type=rev_type,
                                renewal_date=end_date,
                                renewal_days=r_days,
                                contract_group=cg,
                                arr_cad=arr,
                            ))
                        logger.info(f"Bulk renewal query returned {len(renewal_rows)} rows for {len(renewals_by_account)} accounts")
                    except Exception as re:
                        logger.warning(f"Bulk renewal fetch failed: {re}")

                # Process rows into Account objects
                def _parse_date(val):
                    if val:
                        try:
                            from datetime import datetime as dt
                            return dt.strptime(str(val)[:10], "%Y-%m-%d").date()
                        except (ValueError, TypeError):
                            pass
                    return None

                # ══════════════════════════════════════════════════════════════
                # FETCH PRE-COMPUTED HEALTH SCORES (1 fast query instead of N*2)
                # Falls back to real-time calculation if pre-computed not available
                # ══════════════════════════════════════════════════════════════
                logger.info(f"Fetching pre-computed health scores for {len(raw_accounts)} accounts")
                precomputed_scores = self._get_precomputed_health_scores(conn)
                
                # Only fetch Pendo/Freshdesk batch if pre-computed scores not available
                pendo_metrics_batch = {}
                freshdesk_metrics_batch = {}
                if not precomputed_scores:
                    logger.warning("Pre-computed scores not available, falling back to real-time calculation")
                    all_account_names = [row[1] for row in raw_accounts if row[1]]
                    pendo_metrics_batch = self._get_all_pendo_metrics_batch(conn, all_account_names)
                    freshdesk_metrics_batch = self._get_all_freshdesk_metrics_batch(conn, all_account_names)
                
                accounts = []
                at_risk_count = 0  # Count of ALL at-risk accounts (score < 70)
                fallback_count = 0  # Track how many accounts needed fallback
                
                for row in raw_accounts:
                    # Query returns: account_id, name, industry, csm_name, parent_id, parent_name, account_executive
                    account_id = row[0]
                    name = row[1]
                    industry = row[2]
                    csm_name = row[3]
                    parent_id = row[4]
                    parent_name = row[5]
                    ae_name = row[6]

                    renewals = renewals_by_account.get(account_id, [])

                    # Determine nearest renewal from fct_contracts
                    nearest_days = None
                    nearest_date = None
                    for r in renewals:
                        if r.renewal_days is not None:
                            if nearest_days is None or r.renewal_days < nearest_days:
                                nearest_days = r.renewal_days
                                nearest_date = r.renewal_date

                    renewal_days = nearest_days if nearest_days is not None else 999
                    parsed_renewal_date = nearest_date if nearest_date else date.today() + timedelta(days=999)

                    # Use pre-computed health score if available, otherwise fall back to real-time
                    if account_id and account_id in precomputed_scores:
                        health_detail = precomputed_scores[account_id]
                    else:
                        # Fallback: calculate in real-time (slower)
                        fallback_count += 1
                        if not pendo_metrics_batch and not freshdesk_metrics_batch:
                            # Lazy load batch data if needed for fallback
                            all_account_names = [row[1] for row in raw_accounts if row[1]]
                            pendo_metrics_batch = self._get_all_pendo_metrics_batch(conn, all_account_names)
                            freshdesk_metrics_batch = self._get_all_freshdesk_metrics_batch(conn, all_account_names)
                        health_detail = self.calculate_health_score_detail(
                            account_name=name or "Unknown",
                            renewal_days=renewal_days,
                            pendo_data=pendo_metrics_batch.get(name),
                            freshdesk_data=freshdesk_metrics_batch.get(name),
                            account_id=account_id,
                        )
                    health = health_detail.category
                    
                    # Count at-risk accounts BEFORE any filtering
                    if health_detail.score < 70:
                        at_risk_count += 1

                    # Apply health filter if specified
                    if health_filter and health.value != health_filter:
                        continue

                    # Derive primary signal from health factors
                    signal_desc, signal_type = self._derive_primary_signal_from_health(health_detail)

                    # Map status based on health score (higher = healthier)
                    if health_detail.score >= 70:
                        status = AccountStatus.STABLE
                    elif health_detail.score >= 40:
                        status = AccountStatus.IN_PROGRESS
                    else:
                        status = AccountStatus.NEEDS_ATTENTION

                    # Apply status filter if specified
                    if status_filter and status.value != status_filter:
                        continue

                    # Apply KPI filter for at_risk (health score < 70)
                    if kpi_filter == "at_risk" and health_detail.score >= 70:
                        continue

                    accounts.append(
                        Account(
                            id=account_id or "",
                            name=name or "Unknown",
                            health=health,
                            health_score_detail=health_detail,
                            primary_signal=signal_desc,
                            primary_signal_type=signal_type,
                            renewal_days=renewal_days,
                            renewal_date=parsed_renewal_date,
                            owner_id="",
                            owner_name="Unassigned",
                            owner_avatar=None,
                            status=status,
                            csm_name=csm_name if csm_name else None,
                            ae_name=ae_name if ae_name else None,
                            parent_id=parent_id,
                            parent_name=parent_name,
                            renewals=renewals,
                        )
                    )

                # Total is the count of all accounts after filtering
                total = len(accounts)
                
                # Apply pagination in Python
                offset = (page - 1) * page_size
                paginated_accounts = accounts[offset:offset + page_size]
                
                precomputed_used = len(raw_accounts) - fallback_count
                logger.info(f"Returning {len(paginated_accounts)} accounts (page {page}, total: {total}, at_risk: {at_risk_count}, precomputed: {precomputed_used}, fallback: {fallback_count})")
                return paginated_accounts, total, at_risk_count

            except Exception as e:
                logger.error(f"Error fetching accounts: {e}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
                raise Exception(f"Failed to fetch accounts: {str(e)}")

    def get_health_score_detail_for_account(
        self,
        account_name: str,
        renewal_days: Optional[int],
        account_id: Optional[str] = None,
    ) -> HealthScoreDetail:
        """
        Get detailed health score with factor breakdown for a single account.
        
        This is called on-demand when user clicks on health badge.
        Uses the full Python calculation (slower but complete details).
        """
        logger.info(f"get_health_score_detail_for_account: {account_name}")
        
        with self.get_connection() as conn:
            if conn is None:
                # Return minimal detail on connection failure
                return HealthScoreDetail(
                    score=100,
                    category=HealthScore.GOOD,
                    factors=[],
                    has_pendo=False,
                    has_freshdesk=False,
                    scoring_version="error"
                )
            
            # Fetch Pendo and Freshdesk data for this single account
            pendo_data = self._get_all_pendo_metrics_batch(conn, [account_name]).get(account_name)
            freshdesk_data = self._get_all_freshdesk_metrics_batch(conn, [account_name]).get(account_name)
            
            # Fetch Gong sentiment data for this account
            gong_activity = self._fetch_gong_activity(account_id) if account_id else None

            # Calculate full health score detail
            return self.calculate_health_score_detail(
                account_name=account_name,
                renewal_days=renewal_days,
                pendo_data=pendo_data,
                freshdesk_data=freshdesk_data,
                account_id=account_id,
                gong_data=gong_activity,
            )

    def get_account_by_id(self, account_id: str) -> Optional[AccountDetail]:
        """Get detailed account information."""
        FCT_TABLE = FCT_CONTRACTS_TABLE
        with self.get_connection() as conn:
            if conn is None:
                raise Exception("Database connection failed — cannot load account detail")

            try:
                cursor = conn.cursor()
                # Query dim_customers with available columns, join fct_contracts for ARR and renewal
                cursor.execute(f"""
                    SELECT 
                        c.account_id,
                        c.account AS name,
                        c.industry,
                        COALESCE(c.csm_c, 'Unassigned') as owner_name,
                        COALESCE(arr_data.total_arr, 0) as arr,
                        arr_data.nearest_renewal_date,
                        arr_data.renewal_days
                    FROM {DIM_CUSTOMERS_TABLE} c
                    LEFT JOIN (
                        SELECT 
                            account_id,
                            SUM(ARR_EUR) as total_arr,
                            MIN(rev_rec_end_date) as nearest_renewal_date,
                            MIN(DATEDIFF(rev_rec_end_date, CURRENT_DATE())) as renewal_days
                        FROM {FCT_TABLE}
                        WHERE RENEWAL_NOT_YET_CONTRACTED = 'Y'
                          AND revenue_type NOT IN ('Services', 'Perpetual')
                          AND (churn_expected_occurred IS NULL OR churn_expected_occurred = 'nan')
                          AND rev_rec_end_date > CURRENT_DATE()
                        GROUP BY account_id
                    ) arr_data ON c.account_id = arr_data.account_id
                    WHERE c.account_id = ?
                    AND c._fivetran_deleted = false
                """, [account_id])
                
                row = cursor.fetchone()
                cursor.close()

                if not row:
                    return None

                # row[0]: account_id, row[1]: name, row[2]: industry, row[3]: owner_name
                # row[4]: arr, row[5]: nearest_renewal_date, row[6]: renewal_days
                renewal_days = row[6] if row[6] is not None else 999

                # Derive health score (engagement columns removed from schema)
                health = self._derive_health_score(renewal_days, None, None, None)
                signal_desc, signal_type = self._derive_primary_signal(renewal_days, None, None, None)
                status = self._map_account_status(None)

                arr = float(row[4]) if row[4] else 0.0

                # Parse renewal date safely
                parsed_renewal_date = None
                if row[5]:
                    try:
                        from datetime import datetime as dt
                        parsed_renewal_date = dt.strptime(str(row[5])[:10], "%Y-%m-%d").date()
                    except (ValueError, TypeError):
                        pass

                return AccountDetail(
                    id=row[0] or "",
                    name=row[1] or "Unknown",
                    health=health,
                    primary_signal=signal_desc,
                    primary_signal_type=signal_type,
                    renewal_days=renewal_days,
                    renewal_date=parsed_renewal_date if parsed_renewal_date else date.today() + timedelta(days=999),
                    owner_id="",
                    owner_name=row[3] or "Unassigned",
                    owner_avatar=None,
                    status=status,
                    arr=arr,
                    mrr=arr / 12 if arr else 0,
                    contract_start=date.today(),
                    contract_end=parsed_renewal_date if parsed_renewal_date else date.today() + timedelta(days=365),
                    industry=row[2],
                    employees=None,
                    signals=[],
                    notes=None,
                )

            except Exception as e:
                logger.error(f"Error fetching account detail: {e}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
                raise Exception(f"Failed to fetch account detail: {str(e)}")

    def create_task(self, task: TaskCreate, user_id: str) -> Task:
        """Create a new task for an account (mock implementation)."""
        # Note: dim_customers doesn't have a tasks table
        # This would need a separate tasks table in your Databricks
        return Task(
            id=f"task-{datetime.now().timestamp()}",
            account_id=task.account_id,
            title=task.title,
            description=task.description,
            due_date=task.due_date,
            priority=task.priority,
            status="open",
            created_at=datetime.now(),
            created_by=user_id,
        )

    def update_account_status(self, account_id: str, status: AccountStatus) -> bool:
        """Update account status (mock implementation)."""
        # Note: This would require write access to update account_status_c
        logger.info(f"Would update account {account_id} status to {status}")
        return True

    # ==================== Mock Data Methods ====================

    def _get_mock_metrics(self) -> MetricsSummary:
        """Return mock metrics when Databricks is unavailable."""
        return MetricsSummary(
            total_accounts=132,
            total_arr=146170000.0,  # $146.17M
            renewals_arr=24500000.0,  # $24.5M renewing in 90 days
            health_distribution=HealthDistribution(
                good=86,  # ~65%
                at_risk=28,  # ~21%
                critical=18,  # ~14%
            ),
            at_risk_count=18,
            renewals_90_days=22,
            usage_decline_count=14,
            expansion_signals=9,
        )

    def _get_mock_accounts_filtered(
        self,
        page: int,
        page_size: int,
        health_filter: Optional[str],
        status_filter: Optional[str],
        owner_filter: Optional[str],
        search: Optional[str],
        sort_by: str,
        kpi_filter: Optional[str] = None,
    ) -> Tuple[List[Account], int]:
        """Return filtered mock accounts with search ranking."""
        accounts = self._get_mock_accounts()

        # Apply KPI filter
        if kpi_filter:
            if kpi_filter == "at_risk":
                accounts = [a for a in accounts if a.health.value in ["Critical", "At Risk"]]
            elif kpi_filter == "renewals":
                accounts = [a for a in accounts if a.renewal_days <= 90 and a.renewal_days > 0]
            elif kpi_filter == "usage_decline":
                accounts = [a for a in accounts if a.primary_signal_type == "usage"]
            elif kpi_filter == "expansion":
                accounts = [a for a in accounts if a.primary_signal_type == "expansion"]

        if health_filter:
            accounts = [a for a in accounts if a.health.value == health_filter]
        if status_filter:
            accounts = [a for a in accounts if a.status.value == status_filter]
        if owner_filter:
            accounts = [a for a in accounts if a.owner_id == owner_filter]
        
        # Apply search with scoring
        if search:
            # Filter accounts that match and calculate their scores
            scored_accounts = []
            for account in accounts:
                if matches_search(account.name, search):
                    score = calculate_search_score(account.name, search)
                    # Create new account with search_score
                    scored_account = Account(
                        id=account.id,
                        name=account.name,
                        health=account.health,
                        primary_signal=account.primary_signal,
                        primary_signal_type=account.primary_signal_type,
                        renewal_days=account.renewal_days,
                        renewal_date=account.renewal_date,
                        owner_id=account.owner_id,
                        owner_name=account.owner_name,
                        owner_avatar=account.owner_avatar,
                        status=account.status,
                        search_score=score,
                    )
                    scored_accounts.append(scored_account)
            
            # Sort by search score (highest first), then by attention
            scored_accounts.sort(key=lambda a: (-a.search_score if a.search_score else 0, a.renewal_days))
            accounts = scored_accounts
        else:
            # Apply regular sorting when not searching
            if sort_by == "attention_first":
                health_order = {HealthScore.CRITICAL: 0, HealthScore.AT_RISK: 1, HealthScore.GOOD: 2}
                accounts.sort(key=lambda a: (health_order.get(a.health, 99), a.renewal_days))
            elif sort_by == "renewal_date":
                accounts.sort(key=lambda a: a.renewal_days)
            elif sort_by == "name":
                accounts.sort(key=lambda a: a.name)

        total = len(accounts)
        start = (page - 1) * page_size
        end = start + page_size
        return accounts[start:end], total

    def _get_mock_accounts(self) -> List[Account]:
        """Generate mock account data with parent-child relationships."""
        today = date.today()
        return [
            Account(
                id="acc-1",
                name="Acme Corp",
                health=HealthScore.CRITICAL,
                primary_signal="Renewal in 25 days",
                primary_signal_type="renewal",
                renewal_days=25,
                renewal_date=today + timedelta(days=25),
                owner_id="user-1",
                owner_name="Anna Perez",
                owner_avatar=None,
                status=AccountStatus.NEEDS_ATTENTION,
                parent_id=None,
                parent_name=None,
            ),
            Account(
                id="acc-2",
                name="Beacon Systems",
                health=HealthScore.CRITICAL,
                primary_signal="Usage Down 35%",
                primary_signal_type="usage",
                renewal_days=56,
                renewal_date=today + timedelta(days=56),
                owner_id="user-2",
                owner_name="Thomas Nguyen",
                owner_avatar=None,
                status=AccountStatus.NEEDS_ATTENTION,
                parent_id=None,
                parent_name=None,
            ),
            Account(
                id="acc-3",
                name="Delta Industries",
                health=HealthScore.AT_RISK,
                primary_signal="Upcoming Renewal",
                primary_signal_type="renewal",
                renewal_days=45,
                renewal_date=today + timedelta(days=45),
                owner_id="user-3",
                owner_name="Priya Patel",
                owner_avatar=None,
                status=AccountStatus.IN_PROGRESS,
                parent_id=None,
                parent_name=None,
            ),
            Account(
                id="acc-4",
                name="FinservX",
                health=HealthScore.AT_RISK,
                primary_signal="Upcoming Renewal",
                primary_signal_type="renewal",
                renewal_days=65,
                renewal_date=today + timedelta(days=65),
                owner_id="user-4",
                owner_name="Alex Chen",
                owner_avatar=None,
                status=AccountStatus.IN_PROGRESS,
                parent_id=None,
                parent_name=None,
            ),
            Account(
                id="acc-5",
                name="Tyler Technologies",
                health=HealthScore.GOOD,
                primary_signal="Expansion Opportunity",
                primary_signal_type="expansion",
                renewal_days=188,
                renewal_date=today + timedelta(days=188),
                owner_id="user-5",
                owner_name="Sarah Robinson",
                owner_avatar=None,
                status=AccountStatus.STABLE,
                parent_id=None,
                parent_name=None,
            ),
            Account(
                id="acc-6",
                name="Infinisoft",
                health=HealthScore.GOOD,
                primary_signal="Stable",
                primary_signal_type=None,
                renewal_days=262,
                renewal_date=today + timedelta(days=262),
                owner_id="user-6",
                owner_name="Michael Smith",
                owner_avatar=None,
                status=AccountStatus.STABLE,
                parent_id=None,
                parent_name=None,
            ),
            # Duke Energy - Parent account (no parent_id)
            Account(
                id="acc-duke",
                name="Duke Energy",
                health=HealthScore.GOOD,
                primary_signal="Expansion Opportunity",
                primary_signal_type="expansion",
                renewal_days=145,
                renewal_date=today + timedelta(days=145),
                owner_id="user-1",
                owner_name="Anna Perez",
                owner_avatar=None,
                status=AccountStatus.STABLE,
                parent_id=None,
                parent_name=None,
            ),
            # Duke Energy child accounts
            Account(
                id="acc-duke-1",
                name="Duke Energy - Carolinas",
                health=HealthScore.GOOD,
                primary_signal="Stable",
                primary_signal_type=None,
                renewal_days=145,
                renewal_date=today + timedelta(days=145),
                owner_id="user-1",
                owner_name="Anna Perez",
                owner_avatar=None,
                status=AccountStatus.STABLE,
                parent_id="acc-duke",
                parent_name="Duke Energy",
            ),
            Account(
                id="acc-duke-2",
                name="Duke Energy - Florida",
                health=HealthScore.AT_RISK,
                primary_signal="Upcoming Renewal",
                primary_signal_type="renewal",
                renewal_days=60,
                renewal_date=today + timedelta(days=60),
                owner_id="user-1",
                owner_name="Anna Perez",
                owner_avatar=None,
                status=AccountStatus.IN_PROGRESS,
                parent_id="acc-duke",
                parent_name="Duke Energy",
            ),
            Account(
                id="acc-duke-3",
                name="Duke Energy - Indiana",
                health=HealthScore.CRITICAL,
                primary_signal="Renewal in 28 days",
                primary_signal_type="renewal",
                renewal_days=28,
                renewal_date=today + timedelta(days=28),
                owner_id="user-2",
                owner_name="Thomas Nguyen",
                owner_avatar=None,
                status=AccountStatus.NEEDS_ATTENTION,
                parent_id="acc-duke",
                parent_name="Duke Energy",
            ),
            Account(
                id="acc-duke-4",
                name="Duke Energy - Ohio",
                health=HealthScore.GOOD,
                primary_signal="Expansion Opportunity",
                primary_signal_type="expansion",
                renewal_days=200,
                renewal_date=today + timedelta(days=200),
                owner_id="user-3",
                owner_name="Priya Patel",
                owner_avatar=None,
                status=AccountStatus.STABLE,
                parent_id="acc-duke",
                parent_name="Duke Energy",
            ),
            Account(
                id="acc-8",
                name="BC Hydro",
                health=HealthScore.AT_RISK,
                primary_signal="Upcoming Renewal",
                primary_signal_type="renewal",
                renewal_days=78,
                renewal_date=today + timedelta(days=78),
                owner_id="user-2",
                owner_name="Thomas Nguyen",
                owner_avatar=None,
                status=AccountStatus.IN_PROGRESS,
                parent_id=None,
                parent_name=None,
            ),
            Account(
                id="acc-9",
                name="Hydro One",
                health=HealthScore.GOOD,
                primary_signal="Stable",
                primary_signal_type=None,
                renewal_days=320,
                renewal_date=today + timedelta(days=320),
                owner_id="user-3",
                owner_name="Priya Patel",
                owner_avatar=None,
                status=AccountStatus.STABLE,
                parent_id=None,
                parent_name=None,
            ),
            Account(
                id="acc-10",
                name="Acme Technologies",
                health=HealthScore.GOOD,
                primary_signal="Stable",
                primary_signal_type=None,
                renewal_days=210,
                renewal_date=today + timedelta(days=210),
                owner_id="user-4",
                owner_name="Alex Chen",
                owner_avatar=None,
                status=AccountStatus.STABLE,
                parent_id=None,
                parent_name=None,
            ),
            Account(
                id="acc-11",
                name="Global Acme Solutions",
                health=HealthScore.AT_RISK,
                primary_signal="Upcoming Renewal",
                primary_signal_type="renewal",
                renewal_days=55,
                renewal_date=today + timedelta(days=55),
                owner_id="user-5",
                owner_name="Sarah Robinson",
                owner_avatar=None,
                status=AccountStatus.IN_PROGRESS,
                parent_id=None,
                parent_name=None,
            ),
            Account(
                id="acc-12",
                name="BC Power Corp",
                health=HealthScore.CRITICAL,
                primary_signal="Renewal in 20 days",
                primary_signal_type="renewal",
                renewal_days=20,
                renewal_date=today + timedelta(days=20),
                owner_id="user-6",
                owner_name="Michael Smith",
                owner_avatar=None,
                status=AccountStatus.NEEDS_ATTENTION,
                parent_id=None,
                parent_name=None,
            ),
        ]

    def _get_mock_account_detail(self, account_id: str) -> Optional[AccountDetail]:
        """Return mock account detail."""
        accounts = self._get_mock_accounts()
        for acc in accounts:
            if acc.id == account_id:
                return AccountDetail(
                    **acc.model_dump(),
                    arr=125000.0,
                    mrr=10416.67,
                    contract_start=date.today() - timedelta(days=365),
                    contract_end=date.today() + timedelta(days=acc.renewal_days),
                    industry="Technology",
                    employees=150,
                    signals=[
                        Signal(
                            id="sig-1",
                            type="usage_decline",
                            description="Engagement dropped 25% vs last quarter",
                            severity="medium",
                            detected_at=datetime.now() - timedelta(days=5),
                        )
                    ],
                    notes="Key stakeholder changed last month.",
                )
        return None

    def get_account_full_detail(self, account_id: str) -> Optional[AccountFullDetail]:
        """Get comprehensive account detail with all use case data."""
        logger.info(f"get_account_full_detail called for account_id={account_id}")

        # Fetch real account header data from Databricks
        account_detail = self.get_account_by_id(account_id)
        if not account_detail:
            return None

        # Build the full detail with real header + mock widget data for now
        return self._build_full_detail_with_real_account(account_detail)

    def _fetch_contract_context(self, account_detail: AccountDetail) -> ContractContext:
        """Fetch real contract data from fct_contracts for this account."""
        FCT_TABLE = FCT_CONTRACTS_TABLE
        account_id = account_detail.id
        logger.info(f"_fetch_contract_context: account_id={account_id}")

        try:
            with self.get_connection() as conn:
                if conn is None:
                    logger.warning("_fetch_contract_context: no connection, returning empty")
                    return self._empty_contract_context(account_detail)

                safe_id = self._sql_escape(account_id)
                cursor = conn.cursor()
                query = f"""
                    SELECT
                        c.CONTRACT_GROUP,
                        c.REVENUE_TYPE,
                        c.CURRENCY,
                        COALESCE(SUM(c.ARR_CONTRACT_CURRENCY), 0) AS arr_native,
                        COALESCE(SUM(c.ARR_CAD), 0) AS arr_cad,
                        COALESCE(SUM(c.BOOKING_TCV_ALLOCATED_CONTRACT_CURRENCY), 0) AS tcv_native,
                        COALESCE(SUM(c.BOOKING_TCV_CAD), 0) AS tcv_cad,
                        MIN(c.REV_REC_START_DATE) AS contract_start,
                        MAX(c.REV_REC_END_DATE) AS contract_end,
                        MAX(CASE WHEN c.RENEWAL_NOT_YET_CONTRACTED = 'Y' THEN 1 ELSE 0 END) AS is_active_renewal
                    FROM {FCT_TABLE} c
                    WHERE c.ACCOUNT_ID = '{safe_id}'
                    GROUP BY c.CONTRACT_GROUP, c.REVENUE_TYPE, c.CURRENCY
                    ORDER BY contract_end DESC
                """
                logger.info(f"_fetch_contract_context: running query")
                cursor.execute(query)
                rows = cursor.fetchall()
                cursor.close()

                if not rows:
                    logger.info(f"_fetch_contract_context: no contracts found for {account_id}")
                    return self._empty_contract_context(account_detail)

                contract_groups = []
                total_arr_cad = 0.0
                total_tcv_cad = 0.0
                rev_types_set = set()
                nearest_end = None
                today = date.today()

                for row in rows:
                    cg_name = row[0] or "Unknown"
                    rev_type = row[1] or "Unknown"
                    currency = row[2] or "CAD"
                    arr_native = float(row[3]) if row[3] else 0.0
                    arr_cad = float(row[4]) if row[4] else 0.0
                    tcv_native = float(row[5]) if row[5] else 0.0
                    tcv_cad = float(row[6]) if row[6] else 0.0
                    c_start = str(row[7]) if row[7] else None
                    c_end = str(row[8]) if row[8] else None
                    is_active = bool(row[9]) if row[9] else False

                    days_left = None
                    if c_end:
                        try:
                            end_dt = date.fromisoformat(str(c_end)[:10])
                            days_left = (end_dt - today).days
                            if is_active and (nearest_end is None or end_dt < nearest_end):
                                nearest_end = end_dt
                        except (ValueError, TypeError):
                            pass

                    contract_groups.append(ContractGroup(
                        contract_group=cg_name,
                        revenue_type=rev_type,
                        currency=currency,
                        arr=arr_native,
                        arr_cad=arr_cad,
                        tcv=tcv_native,
                        tcv_cad=tcv_cad,
                        contract_start=c_start,
                        contract_end=c_end,
                        days_until_end=days_left,
                        renewal_not_yet_contracted=is_active,
                    ))
                    if is_active:
                        total_arr_cad += arr_cad
                        total_tcv_cad += tcv_cad
                    rev_types_set.add(rev_type)

                days_until = (nearest_end - today).days if nearest_end else 0

                luminance_docs = self._fetch_luminance_documents(account_detail.name)

                return ContractContext(
                    total_arr_cad=total_arr_cad,
                    total_tcv_cad=total_tcv_cad,
                    nearest_renewal_date=nearest_end,
                    days_until_renewal=days_until,
                    contract_count=len(contract_groups),
                    revenue_types=sorted(rev_types_set),
                    contracts=contract_groups,
                    luminance_documents=luminance_docs,
                    # Legacy fields
                    contract_type=", ".join(sorted(rev_types_set)),
                    start_date=account_detail.contract_start if hasattr(account_detail, 'contract_start') else None,
                    end_date=nearest_end,
                    renewal_date=nearest_end,
                    contract_value=total_arr_cad,
                    arr=total_arr_cad,
                    mrr=total_arr_cad / 12 if total_arr_cad else 0.0,
                    payment_terms="N/A",
                    auto_renewal=False,
                    contract_history=[],
                )
        except Exception as e:
            logger.error(f"_fetch_contract_context error: {e}", exc_info=True)
            return self._empty_contract_context(account_detail)

    def _empty_contract_context(self, account_detail: AccountDetail) -> ContractContext:
        """Return empty contract context as fallback."""
        return ContractContext(
            contract_type="N/A",
            start_date=getattr(account_detail, 'contract_start', None),
            end_date=getattr(account_detail, 'contract_end', None),
            renewal_date=getattr(account_detail, 'renewal_date', None),
            days_until_renewal=getattr(account_detail, 'renewal_days', 0),
            contract_value=0, arr=0, mrr=0,
            payment_terms="N/A", auto_renewal=False,
            contract_history=[], contracts=[],
        )

    def _fetch_luminance_documents(self, account_name: str) -> list[LuminanceDocument]:
        """Fetch Luminance contract documents linked to this account."""
        LUMINANCE_TABLE = "silver.silver_layer.dim_luminance_account"
        DIM_CUSTOMERS = "silver.silver_layer.dim_customers"
        try:
            with self.get_connection() as conn:
                if conn is None:
                    return []
                safe_name = self._sql_escape(account_name)
                cursor = conn.cursor()
                cursor.execute(f"""
                    SELECT
                        dla.id,
                        dla.title,
                        dla.url,
                        dla.state,
                        dla.document_type
                    FROM {LUMINANCE_TABLE} dla
                    INNER JOIN {DIM_CUSTOMERS} dc
                        ON dc.account = dla.matched_account_name
                    WHERE dc.account = '{safe_name}'
                      AND dla.url IS NOT NULL
                      AND dla.state = 'import_complete'
                    ORDER BY dla.title
                """)
                rows = cursor.fetchall()
                cursor.close()
                return [
                    LuminanceDocument(
                        document_id=str(row[0] or ""),
                        title=str(row[1] or "Untitled"),
                        url=str(row[2]),
                        state=str(row[3] or "import_complete"),
                        document_type=str(row[4]) if row[4] else None,
                    )
                    for row in rows
                ]
        except Exception as e:
            logger.error(f"_fetch_luminance_documents error: {e}")
            return []

    def _fetch_pendo_usage(self, account_name: str) -> UsageAnalysis:
        """Fetch Pendo product usage data for an account from all 4 tables."""
        PENDO_ACCOUNTS = "silver.silver_layer.dim_pendo_account_customers"
        PENDO_ACCOUNT_DAILY = "silver.silver_layer.fct_pendo_account_daily_metrics"
        PENDO_FEATURE_DAILY = "silver.silver_layer.stg_pendo_feature_daily_metrics"
        PENDO_VISITOR_DAILY = "silver.silver_layer.fct_pendo_visitor_daily_metrics"
        PENDO_PAGE_DAILY = "silver.silver_layer.stg_pendo_page_daily_metrics"
        logger.info(f"_fetch_pendo_usage: account_name={account_name}")

        try:
            with self.get_connection() as conn:
                if conn is None:
                    logger.warning("_fetch_pendo_usage: no connection")
                    return self._empty_usage()

                safe_name = self._sql_escape(account_name)
                cursor = conn.cursor()

                # Step 1: Find pendo account IDs for this customer
                id_query = f"""
                    SELECT pc.id
                    FROM {PENDO_ACCOUNTS} pc
                    WHERE pc.name = '{safe_name}'
                """
                cursor.execute(id_query)
                pendo_rows = cursor.fetchall()
                pendo_ids = [r[0] for r in pendo_rows if r[0]]

                if not pendo_ids:
                    logger.info(f"_fetch_pendo_usage: no pendo accounts found for '{account_name}'")
                    cursor.close()
                    return self._empty_usage()

                logger.info(f"_fetch_pendo_usage: found {len(pendo_ids)} pendo account(s)")
                ids_in = ", ".join([f"'{self._sql_escape(pid)}'" for pid in pendo_ids])

                # ── Tab 1: Account-level daily metrics ──
                daily_query = f"""
                    SELECT
                        date_day,
                        SUM(COALESCE(count_active_visitors, 0)) AS active_visitors,
                        SUM(COALESCE(sum_minutes, 0)) AS sum_minutes,
                        SUM(COALESCE(sum_events, 0)) AS sum_events,
                        SUM(COALESCE(count_pages_viewed, 0)) AS pages_viewed,
                        SUM(COALESCE(count_features_clicked, 0)) AS features_clicked,
                        SUM(COALESCE(count_page_viewing_visitors, 0)) AS page_viewing_visitors,
                        SUM(COALESCE(count_feature_clicking_visitors, 0)) AS feature_clicking_visitors
                    FROM {PENDO_ACCOUNT_DAILY}
                    WHERE account_id IN ({ids_in})
                      AND date_day >= DATE_SUB(CURRENT_DATE(), 365)
                    GROUP BY date_day
                    ORDER BY date_day ASC
                """
                cursor.execute(daily_query)
                daily_rows = cursor.fetchall()

                daily_metrics = []
                for row in daily_rows:
                    active = int(row[1]) if row[1] else 0
                    mins = float(row[2]) if row[2] else 0.0
                    daily_metrics.append(PendoDailyMetric(
                        date_day=str(row[0])[:10] if row[0] else "",
                        active_visitors=active,
                        sum_minutes=mins,
                        sum_events=int(row[3]) if row[3] else 0,
                        pages_viewed=int(row[4]) if row[4] else 0,
                        features_clicked=int(row[5]) if row[5] else 0,
                        page_viewing_visitors=int(row[6]) if row[6] else 0,
                        feature_clicking_visitors=int(row[7]) if row[7] else 0,
                        avg_minutes_per_user=round(mins / active, 1) if active > 0 else 0.0,
                    ))

                # ── Tab 2: Feature-level daily metrics (join through dim_pendo_visitor) ──
                pendo_features = PendoTabData()
                try:
                    feat_query = f"""
                        SELECT
                            f.date_day,
                            f.feature_id,
                            COALESCE(f.feature_name, f.feature_id) AS feature_name,
                            SUM(COALESCE(f.sum_clicks, 0)) AS clicks,
                            SUM(COALESCE(f.count_visitors, 0)) AS unique_visitors
                        FROM {PENDO_FEATURE_DAILY} f
                        WHERE f.group_id IN ({ids_in})
                          AND f.date_day >= DATE_SUB(CURRENT_DATE(), 365)
                        GROUP BY f.date_day, f.feature_id, f.feature_name
                        ORDER BY f.date_day ASC
                    """
                    cursor.execute(feat_query)
                    feat_rows = cursor.fetchall()

                    feat_daily = []
                    feat_totals = {}
                    for row in feat_rows:
                        fname = str(row[2]) if row[2] else str(row[1])
                        clicks = int(row[3]) if row[3] else 0
                        uvs = int(row[4]) if row[4] else 0
                        feat_daily.append({
                            "date_day": str(row[0])[:10],
                            "feature_name": fname,
                            "clicks": clicks,
                            "unique_visitors": uvs,
                        })
                        if fname not in feat_totals:
                            feat_totals[fname] = {"clicks": 0, "visitors": 0}
                        feat_totals[fname]["clicks"] += clicks
                        feat_totals[fname]["visitors"] = max(feat_totals[fname]["visitors"], uvs)

                    top_features = sorted(feat_totals.items(), key=lambda x: x[1]["clicks"], reverse=True)[:20]
                    pendo_features = PendoTabData(
                        daily=feat_daily,
                        top_items=[{"name": k, **v} for k, v in top_features],
                        total_data_days=len(set(d["date_day"] for d in feat_daily)),
                    )
                    logger.info(f"_fetch_pendo_usage: features — {len(feat_daily)} rows, {len(top_features)} unique features")
                except Exception as e:
                    logger.warning(f"_fetch_pendo_usage: feature query failed: {e}")

                # ── Tab 3: Visitor daily metrics (join through dim_pendo_visitor) ──
                pendo_visitors = PendoTabData()
                try:
                    vis_query = f"""
                        SELECT
                            vdm.date_day,
                            COUNT(DISTINCT vdm.visitor_id) AS unique_visitors,
                            SUM(COALESCE(vdm.sum_minutes, 0)) AS sum_minutes,
                            SUM(COALESCE(vdm.sum_events, 0)) AS sum_events,
                            SUM(COALESCE(vdm.count_pages_viewed, 0)) AS pages_viewed,
                            SUM(COALESCE(vdm.count_features_clicked, 0)) AS features_clicked
                        FROM {PENDO_VISITOR_DAILY} vdm
                        JOIN silver.silver_layer.dim_pendo_visitor dv
                            ON vdm.visitor_id = dv.visitor_id
                        WHERE dv.account_id IN ({ids_in})
                          AND vdm.date_day >= DATE_SUB(CURRENT_DATE(), 365)
                          AND vdm.visitor_id IS NOT NULL
                        GROUP BY vdm.date_day
                        ORDER BY vdm.date_day ASC
                    """
                    cursor.execute(vis_query)
                    vis_rows = cursor.fetchall()

                    vis_daily = []
                    for row in vis_rows:
                        vis_daily.append({
                            "date_day": str(row[0])[:10],
                            "unique_visitors": int(row[1]) if row[1] else 0,
                            "sum_minutes": float(row[2]) if row[2] else 0.0,
                            "sum_events": int(row[3]) if row[3] else 0,
                            "pages_viewed": int(row[4]) if row[4] else 0,
                            "features_clicked": int(row[5]) if row[5] else 0,
                        })

                    # Visitor-level breakdown (top visitors by engagement)
                    vis_top_query = f"""
                        SELECT
                            vdm.visitor_id,
                            MAX(vdm.date_day) AS last_active,
                            ROUND(SUM(COALESCE(vdm.sum_minutes, 0)), 1) AS total_minutes,
                            COUNT(DISTINCT vdm.date_day) AS active_days,
                            SUM(COALESCE(vdm.count_pages_viewed, 0)) AS total_pages,
                            SUM(COALESCE(vdm.count_features_clicked, 0)) AS total_features
                        FROM {PENDO_VISITOR_DAILY} vdm
                        JOIN silver.silver_layer.dim_pendo_visitor dv
                            ON vdm.visitor_id = dv.visitor_id
                        WHERE dv.account_id IN ({ids_in})
                          AND vdm.date_day >= DATE_SUB(CURRENT_DATE(), 365)
                          AND vdm.visitor_id IS NOT NULL
                        GROUP BY vdm.visitor_id
                        ORDER BY total_minutes DESC
                        LIMIT 30
                    """
                    cursor.execute(vis_top_query)
                    vis_top_rows = cursor.fetchall()
                    vis_top_items = []
                    for vr in vis_top_rows:
                        vis_top_items.append({
                            "visitor_id": str(vr[0]) if vr[0] else "",
                            "last_active": str(vr[1])[:10] if vr[1] else "",
                            "total_minutes": float(vr[2]) if vr[2] else 0.0,
                            "active_days": int(vr[3]) if vr[3] else 0,
                            "total_pages": int(vr[4]) if vr[4] else 0,
                            "total_features": int(vr[5]) if vr[5] else 0,
                        })
                    logger.info(f"_fetch_pendo_usage: top visitors — {len(vis_top_items)}")

                    pendo_visitors = PendoTabData(
                        daily=vis_daily,
                        top_items=vis_top_items,
                        total_data_days=len(vis_daily),
                    )
                    logger.info(f"_fetch_pendo_usage: visitors — {len(vis_daily)} rows, {len(vis_top_items)} top visitors")
                except Exception as e:
                    logger.warning(f"_fetch_pendo_usage: visitor query failed: {e}")

                # ── Tab 4: Page daily metrics ──
                pendo_pages = PendoTabData()
                try:
                    page_query = f"""
                        SELECT
                            p.date_day,
                            p.page_id,
                            COALESCE(p.page_name, p.page_id) AS page_name,
                            SUM(COALESCE(p.sum_pageviews, 0)) AS views,
                            SUM(COALESCE(p.count_visitors, 0)) AS unique_visitors
                        FROM {PENDO_PAGE_DAILY} p
                        WHERE p.group_id IN ({ids_in})
                          AND p.date_day >= DATE_SUB(CURRENT_DATE(), 365)
                        GROUP BY p.date_day, p.page_id, p.page_name
                        ORDER BY p.date_day ASC
                    """
                    cursor.execute(page_query)
                    page_rows = cursor.fetchall()

                    page_daily = []
                    page_totals = {}
                    for row in page_rows:
                        pname = str(row[2]) if row[2] else str(row[1])
                        views = int(row[3]) if row[3] else 0
                        uvs = int(row[4]) if row[4] else 0
                        page_daily.append({
                            "date_day": str(row[0])[:10],
                            "page_name": pname,
                            "views": views,
                            "unique_visitors": uvs,
                        })
                        if pname not in page_totals:
                            page_totals[pname] = {"views": 0, "visitors": 0}
                        page_totals[pname]["views"] += views
                        page_totals[pname]["visitors"] = max(page_totals[pname]["visitors"], uvs)

                    top_pages = sorted(page_totals.items(), key=lambda x: x[1]["views"], reverse=True)[:20]
                    pendo_pages = PendoTabData(
                        daily=page_daily,
                        top_items=[{"name": k, **v} for k, v in top_pages],
                        total_data_days=len(set(d["date_day"] for d in page_daily)),
                    )
                    logger.info(f"_fetch_pendo_usage: pages — {len(page_daily)} rows, {len(top_pages)} unique pages")
                except Exception as e:
                    logger.warning(f"_fetch_pendo_usage: page query failed: {e}")

                cursor.close()

                # ── Compute account-level summary ──
                from datetime import datetime, timedelta
                today = datetime.now().date()
                d30 = today - timedelta(days=30)
                d60 = today - timedelta(days=60)

                cur_visitors, prev_visitors = 0.0, 0.0
                cur_minutes, prev_minutes = 0.0, 0.0
                cur_events, prev_events = 0.0, 0.0

                for m in daily_metrics:
                    try:
                        md = datetime.strptime(m.date_day, "%Y-%m-%d").date()
                    except ValueError:
                        continue
                    if md >= d30:
                        cur_visitors += m.active_visitors
                        cur_minutes += m.sum_minutes
                        cur_events += m.sum_events
                    elif md >= d60:
                        prev_visitors += m.active_visitors
                        prev_minutes += m.sum_minutes
                        prev_events += m.sum_events

                def pct_change(cur, prev):
                    if prev == 0:
                        return 100.0 if cur > 0 else 0.0
                    return ((cur - prev) / prev) * 100.0

                summary = PendoUsageSummary(
                    current_active_visitors=cur_visitors,
                    previous_active_visitors=prev_visitors,
                    visitors_change_pct=round(pct_change(cur_visitors, prev_visitors), 1),
                    current_minutes=cur_minutes,
                    previous_minutes=prev_minutes,
                    minutes_change_pct=round(pct_change(cur_minutes, prev_minutes), 1),
                    current_events=cur_events,
                    previous_events=prev_events,
                    events_change_pct=round(pct_change(cur_events, prev_events), 1),
                    total_data_days=len(daily_metrics),
                    pendo_account_ids=pendo_ids,
                )

                usage_history = [
                    UsageTrend(date=m.date_day, value=float(m.active_visitors), metric="active_visitors")
                    for m in daily_metrics
                ]

                trend = "stable"
                if summary.visitors_change_pct > 5:
                    trend = "increasing"
                elif summary.visitors_change_pct < -5:
                    trend = "declining"

                return UsageAnalysis(
                    current_usage=cur_visitors,
                    previous_usage=prev_visitors,
                    change_percent=summary.visitors_change_pct,
                    trend=trend,
                    usage_history=usage_history,
                    features_adopted=[],
                    pendo_summary=summary,
                    pendo_daily=daily_metrics,
                    pendo_features=pendo_features,
                    pendo_visitors=pendo_visitors,
                    pendo_pages=pendo_pages,
                    has_pendo_data=True,
                )

        except Exception as e:
            logger.error(f"_fetch_pendo_usage error: {e}", exc_info=True)
            return self._empty_usage()

    def _empty_usage(self, pendo_ids: list = None) -> UsageAnalysis:
        """Return empty usage analysis."""
        return UsageAnalysis(
            current_usage=0, previous_usage=0, change_percent=0,
            trend="stable", usage_history=[], features_adopted=[],
            pendo_summary=PendoUsageSummary(pendo_account_ids=pendo_ids or []),
            pendo_daily=[], has_pendo_data=False,
        )

    def get_support_analysis(self, account_name: str) -> SupportAnalysis:
        """Fetch comprehensive support analysis from Freshdesk data for an account."""
        logger.info(f"get_support_analysis called for account_name={account_name}")

        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # Query 1: Get ticket metrics with sentiment data
                metrics_query = f"""
                WITH freshdesk_company AS (
                    SELECT id 
                    FROM {DIM_FRESHDESK_CUSTOMERS_TABLE}
                    WHERE name = :account_name
                      AND _fivetran_deleted = false
                ),
                tickets_with_sentiment AS (
                    SELECT 
                        t.*,
                        cs.net_sentiment_score,
                        cs.total_messages,
                        cs.customer_messages,
                        cs.support_messages,
                        cs.positive_messages,
                        cs.negative_messages,
                        cs.neutral_messages
                    FROM {FCT_FRESHDESK_TICKETS_TABLE} t
                    LEFT JOIN {DIM_FRESHDESK_CONVERSATION_SUMMARY_TABLE} cs ON t.id = cs.ticket_id
                    WHERE t.company_id IN (SELECT id FROM freshdesk_company)
                )
                SELECT
                    COUNT(*) as total_tickets,
                    COUNT(CASE WHEN label_for_customer NOT IN ('Closed', 'Resolved') THEN 1 END) as open_tickets,
                    COUNT(CASE WHEN priority = 'Urgent' THEN 1 END) as critical_tickets,
                    COUNT(CASE WHEN priority = 'High' THEN 1 END) as high_tickets,
                    AVG(CASE 
                        WHEN stats_closed_at IS NOT NULL AND CAST(stats_closed_at AS STRING) != '-'
                        THEN TIMESTAMPDIFF(HOUR, created_at, stats_closed_at)
                    END) as avg_resolution_hours,
                    COUNT(CASE WHEN created_at >= DATE_SUB(CURRENT_DATE(), 30) THEN 1 END) as tickets_last_30,
                    COUNT(CASE WHEN created_at >= DATE_SUB(CURRENT_DATE(), 60) AND created_at < DATE_SUB(CURRENT_DATE(), 30) THEN 1 END) as tickets_prev_30,
                    AVG(COALESCE(net_sentiment_score, 0)) as avg_sentiment,
                    SUM(COALESCE(customer_messages, 0)) as total_customer_messages,
                    SUM(COALESCE(support_messages, 0)) as total_support_messages,
                    COUNT(CASE WHEN net_sentiment_score > 0 THEN 1 END) as positive_ticket_count,
                    COUNT(CASE WHEN net_sentiment_score < 0 THEN 1 END) as negative_ticket_count,
                    COUNT(CASE WHEN net_sentiment_score = 0 OR net_sentiment_score IS NULL THEN 1 END) as neutral_ticket_count
                FROM tickets_with_sentiment
                """

                cursor.execute(metrics_query, {"account_name": account_name})
                metrics_row = cursor.fetchone()

                if not metrics_row:
                    return self._empty_support_analysis()

                total_tickets = int(metrics_row[0] or 0)
                open_tickets = int(metrics_row[1] or 0)
                critical_tickets = int(metrics_row[2] or 0)
                high_tickets = int(metrics_row[3] or 0)
                avg_resolution = float(metrics_row[4] or 0)
                tickets_last_30 = int(metrics_row[5] or 0)
                tickets_prev_30 = int(metrics_row[6] or 0)
                avg_sentiment = float(metrics_row[7] or 0)
                total_customer_messages = int(metrics_row[8] or 0)
                total_support_messages = int(metrics_row[9] or 0)
                positive_ticket_count = int(metrics_row[10] or 0)
                negative_ticket_count = int(metrics_row[11] or 0)
                neutral_ticket_count = int(metrics_row[12] or 0)

                # Determine trend
                if tickets_prev_30 == 0:
                    trend = "stable"
                elif tickets_last_30 > tickets_prev_30 * 1.1:
                    trend = "increasing"
                elif tickets_last_30 < tickets_prev_30 * 0.9:
                    trend = "decreasing"
                else:
                    trend = "stable"

                # Query 2: Get themes (grouped by type)
                themes_query = f"""
                WITH freshdesk_company AS (
                    SELECT id 
                    FROM {DIM_FRESHDESK_CUSTOMERS_TABLE}
                    WHERE name = :account_name
                      AND _fivetran_deleted = false
                )
                SELECT 
                    COALESCE(t.type, 'Other') as theme_name,
                    COUNT(*) as ticket_count,
                    MAX(t.priority) as max_priority
                FROM {FCT_FRESHDESK_TICKETS_TABLE} t
                WHERE t.company_id IN (SELECT id FROM freshdesk_company)
                  AND t.type IS NOT NULL
                GROUP BY t.type
                ORDER BY ticket_count DESC
                LIMIT 5
                """

                cursor.execute(themes_query, {"account_name": account_name})
                themes_rows = cursor.fetchall()

                themes = []
                for row in themes_rows:
                    theme_name = row[0] or "Other"
                    count = int(row[1] or 0)
                    max_priority = row[2] or "Low"
                    severity = self._map_priority_to_severity(max_priority)
                    themes.append(TicketTheme(name=theme_name, count=count, severity=severity))

                # Query 3: Get recent tickets WITH conversation summary data
                recent_query = f"""
                WITH freshdesk_company AS (
                    SELECT id 
                    FROM {DIM_FRESHDESK_CUSTOMERS_TABLE}
                    WHERE name = :account_name
                      AND _fivetran_deleted = false
                )
                SELECT 
                    t.id,
                    t.subject,
                    t.priority,
                    t.label_for_customer,
                    t.created_at,
                    t.type,
                    cs.ticket_summary,
                    cs.net_sentiment_score,
                    cs.total_messages,
                    cs.customer_messages,
                    cs.support_messages,
                    cs.positive_messages,
                    cs.negative_messages,
                    cs.neutral_messages,
                    cs.last_message_at
                FROM {FCT_FRESHDESK_TICKETS_TABLE} t
                LEFT JOIN {DIM_FRESHDESK_CONVERSATION_SUMMARY_TABLE} cs ON t.id = cs.ticket_id
                WHERE t.company_id IN (SELECT id FROM freshdesk_company)
                ORDER BY t.created_at DESC
                LIMIT 50
                """

                cursor.execute(recent_query, {"account_name": account_name})
                recent_rows = cursor.fetchall()

                recent_tickets = []
                for row in recent_rows:
                    ticket_id = str(row[0])
                    title = row[1] or "No subject"
                    priority = row[2] or "Low"
                    label = row[3] or "Open"
                    created_at = row[4]
                    ticket_type = row[5]
                    summary = row[6]
                    net_sentiment = int(row[7] or 0)
                    total_messages = int(row[8] or 0)
                    customer_messages = int(row[9] or 0)
                    support_messages = int(row[10] or 0)
                    positive_messages = int(row[11] or 0)
                    negative_messages = int(row[12] or 0)
                    neutral_messages = int(row[13] or 0)
                    last_message_at = row[14]

                    severity = self._map_priority_to_severity(priority)
                    status = self._map_label_to_status(label)

                    if isinstance(created_at, str):
                        try:
                            created_at = datetime.fromisoformat(created_at.replace('Z', '+00:00').replace('+00:00', ''))
                        except:
                            created_at = datetime.now()

                    if isinstance(last_message_at, str):
                        try:
                            last_message_at = datetime.fromisoformat(last_message_at.replace('Z', '+00:00').replace('+00:00', ''))
                        except:
                            last_message_at = None

                    recent_tickets.append(SupportTicket(
                        id=ticket_id,
                        title=title[:150],
                        severity=severity,
                        status=status,
                        created_at=created_at,
                        updated_at=created_at,
                        summary=summary[:500] if summary else None,
                        net_sentiment=net_sentiment,
                        total_messages=total_messages,
                        customer_messages=customer_messages,
                        support_messages=support_messages,
                        positive_messages=positive_messages,
                        negative_messages=negative_messages,
                        neutral_messages=neutral_messages,
                        last_message_at=last_message_at,
                        ticket_type=ticket_type,
                    ))

                # Query 4: Resolution time distribution stats
                resolution_stats_query = f"""
                WITH freshdesk_company AS (
                    SELECT id 
                    FROM {DIM_FRESHDESK_CUSTOMERS_TABLE}
                    WHERE name = :account_name
                      AND _fivetran_deleted = false
                ),
                resolved_tickets AS (
                    SELECT 
                        t.id,
                        t.created_at,
                        t.stats_closed_at,
                        CASE 
                            WHEN t.stats_closed_at IS NOT NULL AND CAST(t.stats_closed_at AS STRING) != '-'
                            THEN TIMESTAMPDIFF(HOUR, t.created_at, t.stats_closed_at) / 24.0
                            ELSE NULL
                        END as resolution_days
                    FROM {FCT_FRESHDESK_TICKETS_TABLE} t
                    WHERE t.company_id IN (SELECT id FROM freshdesk_company)
                      AND t.label_for_customer IN ('Closed', 'Resolved')
                      AND t.stats_closed_at IS NOT NULL 
                      AND CAST(t.stats_closed_at AS STRING) != '-'
                ),
                stats AS (
                    SELECT
                        AVG(resolution_days) as mean_days,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY resolution_days) as median_days,
                        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY resolution_days) as p25_days,
                        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY resolution_days) as p75_days,
                        PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY resolution_days) as p90_days,
                        MIN(resolution_days) as min_days,
                        MAX(resolution_days) as max_days,
                        COUNT(*) as total_resolved,
                        COUNT(CASE WHEN resolution_days < 1 THEN 1 END) as bucket_lt_1d,
                        COUNT(CASE WHEN resolution_days >= 1 AND resolution_days < 3 THEN 1 END) as bucket_1_3d,
                        COUNT(CASE WHEN resolution_days >= 3 AND resolution_days < 7 THEN 1 END) as bucket_3_7d,
                        COUNT(CASE WHEN resolution_days >= 7 AND resolution_days < 14 THEN 1 END) as bucket_7_14d,
                        COUNT(CASE WHEN resolution_days >= 14 AND resolution_days < 30 THEN 1 END) as bucket_14_30d,
                        COUNT(CASE WHEN resolution_days >= 30 THEN 1 END) as bucket_30plus
                    FROM resolved_tickets
                    WHERE resolution_days IS NOT NULL AND resolution_days >= 0
                )
                SELECT * FROM stats
                """

                resolution_stats = None
                try:
                    cursor.execute(resolution_stats_query, {"account_name": account_name})
                    res_row = cursor.fetchone()
                    if res_row and res_row[7] and res_row[7] > 0:  # total_resolved > 0
                        total_resolved = int(res_row[7])
                        distribution = []
                        buckets = [
                            ("< 1 day", 0, 1, int(res_row[8] or 0)),
                            ("1-3 days", 1, 3, int(res_row[9] or 0)),
                            ("3-7 days", 3, 7, int(res_row[10] or 0)),
                            ("7-14 days", 7, 14, int(res_row[11] or 0)),
                            ("14-30 days", 14, 30, int(res_row[12] or 0)),
                            ("30+ days", 30, 9999, int(res_row[13] or 0)),
                        ]
                        for label, min_d, max_d, count in buckets:
                            pct = round((count / total_resolved) * 100, 1) if total_resolved > 0 else 0
                            distribution.append(ResolutionBucket(
                                label=label,
                                min_days=min_d,
                                max_days=max_d,
                                count=count,
                                percentage=pct
                            ))
                        resolution_stats = ResolutionStats(
                            mean_days=round(float(res_row[0] or 0), 2),
                            median_days=round(float(res_row[1] or 0), 2),
                            p25_days=round(float(res_row[2] or 0), 2),
                            p75_days=round(float(res_row[3] or 0), 2),
                            p90_days=round(float(res_row[4] or 0), 2),
                            min_days=round(float(res_row[5] or 0), 2),
                            max_days=round(float(res_row[6] or 0), 2),
                            total_resolved=total_resolved,
                            distribution=distribution
                        )
                except Exception as e:
                    logger.warning(f"Failed to get resolution stats: {e}")
                    resolution_stats = None

                return SupportAnalysis(
                    open_tickets=open_tickets,
                    critical_tickets=critical_tickets,
                    high_tickets=high_tickets,
                    avg_resolution_hours=round(avg_resolution, 1),
                    ticket_trend=trend,
                    themes=themes,
                    recent_tickets=recent_tickets,
                    avg_sentiment=round(avg_sentiment, 2),
                    total_tickets=total_tickets,
                    total_customer_messages=total_customer_messages,
                    total_support_messages=total_support_messages,
                    positive_ticket_count=positive_ticket_count,
                    negative_ticket_count=negative_ticket_count,
                    neutral_ticket_count=neutral_ticket_count,
                    resolution_stats=resolution_stats,
                )

        except Exception as e:
            logger.error(f"get_support_analysis error: {e}", exc_info=True)
            return self._empty_support_analysis()

    def _map_priority_to_severity(self, priority: str) -> str:
        """Map Freshdesk priority to widget severity."""
        mapping = {
            "Urgent": "critical",
            "High": "high",
            "Medium": "medium",
            "Low": "low",
        }
        return mapping.get(priority, "low")

    def _map_label_to_status(self, label: str) -> str:
        """Map Freshdesk label_for_customer to widget status."""
        if label in ("Closed", "Resolved"):
            return "resolved"
        elif label == "Open":
            return "open"
        else:
            return "in_progress"

    def _empty_support_analysis(self) -> SupportAnalysis:
        """Return empty support analysis."""
        return SupportAnalysis(
            open_tickets=0, critical_tickets=0, high_tickets=0,
            avg_resolution_hours=0, ticket_trend="stable",
            themes=[], recent_tickets=[]
        )

    def get_support_tickets_paginated(
        self, 
        account_name: str, 
        page: int = 1, 
        page_size: int = 25,
        status_filter: Optional[str] = None,
        severity_filter: Optional[str] = None,
    ) -> Tuple[List[SupportTicket], int]:
        """Get paginated support tickets for an account."""
        logger.info(f"get_support_tickets_paginated: account={account_name}, page={page}, size={page_size}")
        
        try:
            with self.get_connection() as conn:
                if conn is None:
                    return [], 0
                
                cursor = conn.cursor()
                offset = (page - 1) * page_size
                
                # Build filter conditions
                status_conditions = ""
                severity_conditions = ""
                
                if status_filter:
                    if status_filter == "open":
                        status_conditions = "AND t.label_for_customer = 'Open'"
                    elif status_filter == "in_progress":
                        status_conditions = "AND t.label_for_customer NOT IN ('Open', 'Closed', 'Resolved')"
                    elif status_filter == "resolved":
                        status_conditions = "AND t.label_for_customer IN ('Closed', 'Resolved')"
                
                if severity_filter:
                    priority_map = {"critical": "Urgent", "high": "High", "medium": "Medium", "low": "Low"}
                    freshdesk_priority = priority_map.get(severity_filter)
                    if freshdesk_priority:
                        severity_conditions = f"AND t.priority = '{freshdesk_priority}'"
                
                # Count total
                count_query = f"""
                WITH freshdesk_company AS (
                    SELECT id 
                    FROM {DIM_FRESHDESK_CUSTOMERS_TABLE}
                    WHERE name = :account_name
                      AND _fivetran_deleted = false
                )
                SELECT COUNT(*) 
                FROM {FCT_FRESHDESK_TICKETS_TABLE} t
                WHERE t.company_id IN (SELECT id FROM freshdesk_company)
                {status_conditions}
                {severity_conditions}
                """
                cursor.execute(count_query, {"account_name": account_name})
                total = cursor.fetchone()[0] or 0
                
                # Get paginated tickets
                tickets_query = f"""
                WITH freshdesk_company AS (
                    SELECT id 
                    FROM {DIM_FRESHDESK_CUSTOMERS_TABLE}
                    WHERE name = :account_name
                      AND _fivetran_deleted = false
                )
                SELECT 
                    t.id,
                    t.subject,
                    t.priority,
                    t.label_for_customer,
                    t.created_at,
                    t.type,
                    cs.ticket_summary,
                    cs.net_sentiment_score,
                    cs.total_messages,
                    cs.customer_messages,
                    cs.support_messages,
                    cs.positive_messages,
                    cs.negative_messages,
                    cs.neutral_messages,
                    cs.last_message_at
                FROM {FCT_FRESHDESK_TICKETS_TABLE} t
                LEFT JOIN {DIM_FRESHDESK_CONVERSATION_SUMMARY_TABLE} cs ON t.id = cs.ticket_id
                WHERE t.company_id IN (SELECT id FROM freshdesk_company)
                {status_conditions}
                {severity_conditions}
                ORDER BY t.created_at DESC
                LIMIT {page_size} OFFSET {offset}
                """
                
                cursor.execute(tickets_query, {"account_name": account_name})
                rows = cursor.fetchall()
                cursor.close()
                
                tickets = []
                for row in rows:
                    ticket_id = str(row[0])
                    title = row[1] or "No subject"
                    priority = row[2] or "Low"
                    label = row[3] or "Open"
                    created_at = row[4]
                    ticket_type = row[5]
                    summary = row[6]
                    net_sentiment = int(row[7] or 0)
                    total_messages = int(row[8] or 0)
                    customer_messages = int(row[9] or 0)
                    support_messages = int(row[10] or 0)
                    positive_messages = int(row[11] or 0)
                    negative_messages = int(row[12] or 0)
                    neutral_messages = int(row[13] or 0)
                    last_message_at = row[14]
                    
                    severity = self._map_priority_to_severity(priority)
                    status = self._map_label_to_status(label)
                    
                    if isinstance(created_at, str):
                        try:
                            created_at = datetime.fromisoformat(created_at.replace('Z', '+00:00').replace('+00:00', ''))
                        except:
                            created_at = datetime.now()
                    
                    if isinstance(last_message_at, str):
                        try:
                            last_message_at = datetime.fromisoformat(last_message_at.replace('Z', '+00:00').replace('+00:00', ''))
                        except:
                            last_message_at = None
                    
                    tickets.append(SupportTicket(
                        id=ticket_id,
                        title=title[:150],
                        severity=severity,
                        status=status,
                        created_at=created_at,
                        updated_at=created_at,
                        summary=summary[:500] if summary else None,
                        net_sentiment=net_sentiment,
                        total_messages=total_messages,
                        customer_messages=customer_messages,
                        support_messages=support_messages,
                        positive_messages=positive_messages,
                        negative_messages=negative_messages,
                        neutral_messages=neutral_messages,
                        last_message_at=last_message_at,
                        ticket_type=ticket_type,
                    ))
                
                return tickets, total
        
        except Exception as e:
            logger.error(f"get_support_tickets_paginated error: {e}", exc_info=True)
            return [], 0

    def get_csm_support_tickets_recent(
        self,
        csm_id: str,
        limit: int = 40,
        account_type: Optional[str] = None,
    ) -> List[SupportTicket]:
        """Tickets for Freshdesk companies linked to Navigate accounts owned by csm_c."""
        limit = max(1, min(int(limit), 100))
        type_cond = ""
        params: dict = {"csm_id": csm_id}
        if account_type and str(account_type).strip().lower() != "all":
            type_cond = " AND c.account_type = :account_type"
            params["account_type"] = account_type

        logger.info(
            f"get_csm_support_tickets_recent: csm_id={csm_id}, limit={limit}, account_type={account_type}"
        )

        try:
            with self.get_connection() as conn:
                if conn is None:
                    return []

                cursor = conn.cursor()
                tickets_query = f"""
                WITH csm_accounts AS (
                    SELECT DISTINCT c.account AS account_name
                    FROM {DIM_CUSTOMERS_TABLE} c
                    WHERE c.csm_c = :csm_id AND c._fivetran_deleted = false{type_cond}
                ),
                freshdesk_company AS (
                    SELECT fc.id AS company_id, ca.account_name AS nav_account_name
                    FROM {DIM_FRESHDESK_CUSTOMERS_TABLE} fc
                    INNER JOIN csm_accounts ca ON fc.name = ca.account_name
                    WHERE fc._fivetran_deleted = false
                )
                SELECT
                    t.id,
                    t.subject,
                    t.priority,
                    t.label_for_customer,
                    t.created_at,
                    t.type,
                    cs.ticket_summary,
                    cs.net_sentiment_score,
                    cs.total_messages,
                    cs.customer_messages,
                    cs.support_messages,
                    cs.positive_messages,
                    cs.negative_messages,
                    cs.neutral_messages,
                    cs.last_message_at,
                    fdc.nav_account_name
                FROM {FCT_FRESHDESK_TICKETS_TABLE} t
                INNER JOIN freshdesk_company fdc ON t.company_id = fdc.company_id
                LEFT JOIN {DIM_FRESHDESK_CONVERSATION_SUMMARY_TABLE} cs ON t.id = cs.ticket_id
                ORDER BY t.created_at DESC
                LIMIT {limit}
                """
                cursor.execute(tickets_query, params)
                rows = cursor.fetchall()
                cursor.close()

                tickets: List[SupportTicket] = []
                for row in rows:
                    ticket_id = str(row[0])
                    title = row[1] or "No subject"
                    priority = row[2] or "Low"
                    label = row[3] or "Open"
                    created_at = row[4]
                    ticket_type = row[5]
                    summary = row[6]
                    net_sentiment = int(row[7] or 0)
                    total_messages = int(row[8] or 0)
                    customer_messages = int(row[9] or 0)
                    support_messages = int(row[10] or 0)
                    positive_messages = int(row[11] or 0)
                    negative_messages = int(row[12] or 0)
                    neutral_messages = int(row[13] or 0)
                    last_message_at = row[14]
                    nav_account = row[15]
                    nav_account_str = str(nav_account).strip() if nav_account is not None else None
                    if nav_account_str in ("", "None", "nan"):
                        nav_account_str = None

                    severity = self._map_priority_to_severity(priority)
                    status = self._map_label_to_status(label)

                    if isinstance(created_at, str):
                        try:
                            created_at = datetime.fromisoformat(
                                created_at.replace("Z", "+00:00").replace("+00:00", "")
                            )
                        except Exception:
                            created_at = datetime.now()

                    if isinstance(last_message_at, str):
                        try:
                            last_message_at = datetime.fromisoformat(
                                last_message_at.replace("Z", "+00:00").replace("+00:00", "")
                            )
                        except Exception:
                            last_message_at = None

                    tickets.append(
                        SupportTicket(
                            id=ticket_id,
                            title=title[:150],
                            severity=severity,
                            status=status,
                            created_at=created_at,
                            updated_at=created_at,
                            summary=summary[:500] if summary else None,
                            net_sentiment=net_sentiment,
                            total_messages=total_messages,
                            customer_messages=customer_messages,
                            support_messages=support_messages,
                            positive_messages=positive_messages,
                            negative_messages=negative_messages,
                            neutral_messages=neutral_messages,
                            last_message_at=last_message_at,
                            ticket_type=ticket_type,
                            account_name=nav_account_str,
                        )
                    )

                return tickets

        except Exception as e:
            logger.error(f"get_csm_support_tickets_recent error: {e}", exc_info=True)
            return []

    def get_confluence_implementation_context(self, account_id: str) -> ConfluenceImplementationResponse:
        """Load Confluence client implementation page text joined to dim_customers by account id / name."""
        logger.info(f"get_confluence_implementation_context: account_id={account_id}")
        try:
            with self.get_connection() as conn:
                if conn is None:
                    return ConfluenceImplementationResponse(has_content=False)

                cursor = conn.cursor()
                query = f"""
                SELECT
                    kb.page_title,
                    kb.page_text,
                    kb.page_id,
                    kb.space_id,
                    kb.root_page_name
                FROM {KB_CONFLUENCE_CUSTOMER_CONTEXT_TABLE} kb
                INNER JOIN {DIM_CUSTOMERS_TABLE} dc
                  ON dc.account_id = :account_id
                  AND dc._fivetran_deleted = false
                WHERE kb.depth >= 1
                  AND (
                    kb.customer_id = dc.account_id
                    OR TRIM(COALESCE(kb.customer_account, '')) = TRIM(COALESCE(dc.account, ''))
                  )
                ORDER BY
                  CASE WHEN kb.customer_id IS NOT NULL AND kb.customer_id = dc.account_id THEN 0 ELSE 1 END,
                  kb.depth DESC,
                  LENGTH(COALESCE(CAST(kb.page_text AS STRING), '')) DESC
                LIMIT 1
                """
                cursor.execute(query, {"account_id": account_id})
                row = cursor.fetchone()
                cursor.close()

                if not row:
                    return ConfluenceImplementationResponse(has_content=False)

                title, text, page_id, space_id, root_name = row[0], row[1], row[2], row[3], row[4]
                text_str = str(text).strip() if text is not None else ""
                if not text_str:
                    return ConfluenceImplementationResponse(has_content=False)
                return ConfluenceImplementationResponse(
                    has_content=True,
                    page_title=str(title).strip() if title is not None else None,
                    page_text=text_str,
                    page_id=str(page_id) if page_id is not None else None,
                    space_id=str(space_id) if space_id is not None else None,
                    root_page_name=str(root_name).strip() if root_name is not None else None,
                )

        except Exception as e:
            logger.error(f"get_confluence_implementation_context error: {e}", exc_info=True)
            return ConfluenceImplementationResponse(has_content=False)

    def _fetch_gong_activity(self, sf_account_id: str) -> Optional[GongActivityAnalysis]:
        """Fetch Gong call activity for an account using its Salesforce account ID."""
        try:
            safe_id = self._sql_escape(sf_account_id)
            with self.get_connection() as conn:
                if conn is None:
                    return None
                cursor = conn.cursor()

                # Query 1: recent calls (DISTINCT to avoid bridge fan-out)
                cursor.execute(f"""
                    SELECT DISTINCT
                        g.call_id,
                        g.call_title,
                        g.call_started_ts,
                        g.call_duration_seconds,
                        g.call_brief
                    FROM {GONG_CALL_TABLE} g
                    JOIN {GONG_BRIDGE_TABLE} b ON g.call_id = b.call_id
                    WHERE b.object_type = 'account'
                      AND b.object_id = '{safe_id}'
                    ORDER BY g.call_started_ts DESC
                    LIMIT 10
                """)
                call_rows = cursor.fetchall()

                if not call_rows:
                    return None

                call_ids = [str(r[0]) for r in call_rows]
                now = datetime.now()

                # Cadence counts
                meetings_30d = sum(
                    1 for r in call_rows
                    if r[2] and (now - r[2].replace(tzinfo=None)).days <= 30
                )
                meetings_90d = sum(
                    1 for r in call_rows
                    if r[2] and (now - r[2].replace(tzinfo=None)).days <= 90
                )
                last_ts = call_rows[0][2]
                days_since = int((now - last_ts.replace(tzinfo=None)).days) if last_ts else None

                # Query 2: tracker signals (last 90 days) — call_id is BIGINT, needs CAST
                cursor.execute(f"""
                    SELECT
                        t.tracker_name,
                        COUNT(DISTINCT t.call_id) AS call_count,
                        COALESCE(SUM(t.phrase_count), 0) AS mention_count
                    FROM {GONG_TRACKER_HIT_TABLE} t
                    JOIN {GONG_BRIDGE_TABLE} b ON CAST(t.call_id AS STRING) = b.call_id
                    JOIN {GONG_CALL_TABLE} g ON CAST(t.call_id AS STRING) = g.call_id
                    WHERE b.object_type = 'account'
                      AND b.object_id = '{safe_id}'
                      AND g.call_started_ts >= DATEADD(day, -90, CURRENT_TIMESTAMP())
                    GROUP BY t.tracker_name
                    ORDER BY call_count DESC
                """)
                tracker_rows = cursor.fetchall()

                tracker_signals = []
                risk_signal_calls = 0
                engagement_signal_calls = 0
                for row in tracker_rows:
                    name, cnt, mentions = row[0], int(row[1]), int(row[2])
                    if name in GONG_RISK_TRACKERS:
                        category = "risk"
                        risk_signal_calls += cnt
                    elif name in GONG_ENGAGEMENT_TRACKERS:
                        category = "engagement"
                        engagement_signal_calls += cnt
                    else:
                        category = "general"
                    tracker_signals.append(GongTrackerSignal(
                        tracker_name=name,
                        call_count=cnt,
                        mention_count=mentions,
                        category=category,
                    ))

                # Query 3: key points for most recent call
                most_recent_id = call_ids[0]
                cursor.execute(f"""
                    SELECT key_point_text
                    FROM {GONG_KEY_POINT_TABLE}
                    WHERE call_id = '{self._sql_escape(most_recent_id)}'
                    ORDER BY key_point_index
                    LIMIT 5
                """)
                key_points = [r[0] for r in cursor.fetchall() if r[0]]

                # Query 4: participants for the 5 most recent calls
                recent_ids = call_ids[:5]
                ids_sql = ", ".join(f"'{self._sql_escape(cid)}'" for cid in recent_ids)
                cursor.execute(f"""
                    SELECT DISTINCT p.call_id, p.name, p.affiliation
                    FROM {GONG_PARTICIPANT_TABLE} p
                    WHERE p.call_id IN ({ids_sql})
                      AND p.affiliation IN ('External', 'Internal')
                      AND p.name IS NOT NULL
                """)
                participants_by_call: dict = {}
                for row in cursor.fetchall():
                    cid, name, aff = str(row[0]), row[1], row[2]
                    participants_by_call.setdefault(cid, {"External": [], "Internal": []})
                    participants_by_call[cid][aff].append(name)

                # Build GongCallSummary list
                recent_calls = []
                for row in call_rows:
                    cid = str(row[0])
                    brief = row[4] or ""
                    excerpt = brief[:200].rstrip() if brief else None
                    pts = participants_by_call.get(cid, {})
                    duration_sec = row[3] or 0
                    recent_calls.append(GongCallSummary(
                        call_id=cid,
                        title=row[1] or "Untitled",
                        started_at=row[2],
                        duration_minutes=max(1, duration_sec // 60),
                        brief_excerpt=excerpt,
                        customer_attendees=pts.get("External", []),
                        csm_attendees=pts.get("Internal", []),
                    ))

                # Derive engagement score and labels
                rate_30d = meetings_30d / 1.0
                rate_90d = meetings_90d / 3.0  # monthly equivalent
                if meetings_90d == 0:
                    engagement_score = 20
                    engagement_label = "No recent meetings"
                    engagement_trend = "declining"
                elif days_since is not None and days_since > 60:
                    engagement_score = 35
                    engagement_label = "No meetings in 60+ days"
                    engagement_trend = "declining"
                elif days_since is not None and days_since > 30:
                    engagement_score = 50
                    engagement_label = "Low cadence"
                    engagement_trend = "stable"
                elif risk_signal_calls > engagement_signal_calls:
                    engagement_score = 55
                    engagement_label = "Risk signals present"
                    engagement_trend = "declining" if rate_30d < rate_90d else "stable"
                elif engagement_signal_calls > risk_signal_calls:
                    engagement_score = 85
                    engagement_label = "Healthy engagement"
                    engagement_trend = "improving" if rate_30d >= rate_90d else "stable"
                else:
                    engagement_score = 70
                    engagement_label = "Neutral"
                    engagement_trend = "improving" if rate_30d > rate_90d else "stable"

                return GongActivityAnalysis(
                    meetings_30d=meetings_30d,
                    meetings_90d=meetings_90d,
                    last_meeting_date=last_ts,
                    days_since_last_meeting=days_since,
                    tracker_signals=tracker_signals,
                    risk_signal_calls=risk_signal_calls,
                    engagement_signal_calls=engagement_signal_calls,
                    engagement_label=engagement_label,
                    engagement_trend=engagement_trend,
                    engagement_score=engagement_score,
                    recent_calls=recent_calls,
                    latest_key_points=key_points,
                )
        except Exception as e:
            logger.warning(f"_fetch_gong_activity error for {sf_account_id}: {e}")
            return None

    def _build_full_detail_with_real_account(self, account_detail: AccountDetail) -> AccountFullDetail:
        """Build full detail response using real account header + placeholder widget data."""
        today = date.today()
        now = datetime.now()

        gong_activity = self._fetch_gong_activity(account_detail.id)

        health_breakdown = HealthBreakdown(
            overall_score=74,
            usage_score=68,
            support_score=82,
            engagement_score=gong_activity.engagement_score if gong_activity else 50,
            renewal_score=75,
            trend="stable",
            contributing_factors=[
                ContributingFactor(name="Usage Trend", impact="neutral", description="Usage data not yet connected"),
                ContributingFactor(name="Support Health", impact="neutral", description="Support data not yet connected"),
                ContributingFactor(name="Stakeholder Engagement", impact="neutral", description="Engagement data not yet connected"),
            ]
        )

        support_analysis = self.get_support_analysis(account_detail.name)

        usage_analysis = self._fetch_pendo_usage(account_detail.name)

        whitespace = WhitespaceAnalysis(
            total_licenses=0, used_licenses=0, utilization_percent=0,
            products=[], expansion_opportunities=[]
        )

        contract = self._fetch_contract_context(account_detail)

        risk_assessment = RiskAssessment(
            churn_risk_score=0, renewal_risk_score=0,
            risk_level="unknown", risk_factors=[], recommended_actions=[]
        )

        sentiment = SentimentAnalysis(
            overall_sentiment=0, sentiment_label="neutral",
            trend="stable", sources=[], recent_interactions=[]
        )

        benchmark = BenchmarkData(peer_group="N/A", metrics=[])

        meeting_brief = MeetingBrief(
            generated_at=now,
            snapshot_id=f"snapshot-{account_detail.id}-{now.strftime('%Y%m%d')}",
            summary=f"{account_detail.name} — widget data not yet connected to live sources.",
            key_points=[], talking_points=[],
            risks_to_address=[], opportunities=[],
            recent_activity_summary="", recommended_topics=[]
        )

        value_realization = ValueRealization(
            goals=[], overall_realization_percent=0,
            time_to_value_days=0, adoption_score=0
        )

        return AccountFullDetail(
            account=account_detail,
            health_breakdown=health_breakdown,
            support_analysis=support_analysis,
            usage_analysis=usage_analysis,
            whitespace=whitespace,
            contract=contract,
            changes_since_last_touch=[],
            risk_assessment=risk_assessment,
            sentiment=sentiment,
            benchmark=benchmark,
            alerts=[],
            signals=[],
            notes=[],
            meeting_brief=meeting_brief,
            value_realization=value_realization,
            gong_activity=gong_activity,
            last_updated=now,
            last_touch_date=now,
        )

    def _get_mock_account_full_detail(self, account_id: str) -> Optional[AccountFullDetail]:
        """Generate comprehensive mock data for account detail page."""
        # Get base account detail
        account_detail = self._get_mock_account_detail(account_id)
        if not account_detail:
            return None

        today = date.today()
        now = datetime.now()

        # Health Breakdown
        health_breakdown = HealthBreakdown(
            overall_score=74,
            usage_score=68,
            support_score=82,
            engagement_score=71,
            renewal_score=75,
            trend="stable",
            contributing_factors=[
                ContributingFactor(
                    name="Usage Trend",
                    impact="negative",
                    description="Usage decreased 10% compared to last quarter"
                ),
                ContributingFactor(
                    name="Support Health",
                    impact="positive",
                    description="No critical support tickets in the last 30 days"
                ),
                ContributingFactor(
                    name="Stakeholder Engagement",
                    impact="neutral",
                    description="Regular engagement with primary contact"
                ),
                ContributingFactor(
                    name="Feature Adoption",
                    impact="positive",
                    description="Adopted 3 new features this quarter"
                ),
            ]
        )

        # Support Analysis
        support_analysis = SupportAnalysis(
            open_tickets=34,
            critical_tickets=2,
            high_tickets=5,
            avg_resolution_hours=18.5,
            ticket_trend="stable",
            themes=[
                TicketTheme(name="Performance Issues", count=12, severity="high"),
                TicketTheme(name="Integration Questions", count=8, severity="medium"),
                TicketTheme(name="Feature Requests", count=7, severity="low"),
                TicketTheme(name="Login/Auth", count=4, severity="medium"),
                TicketTheme(name="Billing", count=3, severity="low"),
            ],
            recent_tickets=[
                SupportTicket(
                    id="tkt-001",
                    title="API response times degraded",
                    severity="critical",
                    status="open",
                    created_at=now - timedelta(hours=4),
                    updated_at=now - timedelta(hours=1),
                ),
                SupportTicket(
                    id="tkt-002",
                    title="SSO integration failing intermittently",
                    severity="high",
                    status="in_progress",
                    created_at=now - timedelta(days=2),
                    updated_at=now - timedelta(hours=6),
                ),
                SupportTicket(
                    id="tkt-003",
                    title="Dashboard loading slowly",
                    severity="medium",
                    status="open",
                    created_at=now - timedelta(days=3),
                    updated_at=now - timedelta(days=1),
                ),
            ]
        )

        # Usage Analysis
        usage_history = [
            UsageTrend(date=today - timedelta(days=i*7), value=1000 + (i % 5) * 100 - i * 10, metric="active_users")
            for i in range(12, -1, -1)
        ]
        usage_analysis = UsageAnalysis(
            current_usage=1461,
            previous_usage=1650,
            change_percent=-11.5,
            trend="declining",
            usage_history=usage_history,
            features_adopted=[
                FeatureAdoption(name="Analytics Dashboard", adoption_percent=85, trend="stable"),
                FeatureAdoption(name="Custom Reports", adoption_percent=62, trend="increasing"),
                FeatureAdoption(name="API Integration", adoption_percent=78, trend="stable"),
                FeatureAdoption(name="Mobile App", adoption_percent=34, trend="increasing"),
                FeatureAdoption(name="Automation Rules", adoption_percent=45, trend="declining"),
            ]
        )

        # Whitespace Analysis
        whitespace = WhitespaceAnalysis(
            total_licenses=500,
            used_licenses=387,
            utilization_percent=77.4,
            products=[
                ProductWhitespace(name="Core Platform", licensed=300, used=275, utilization_percent=91.7),
                ProductWhitespace(name="Analytics Add-on", licensed=100, used=85, utilization_percent=85.0),
                ProductWhitespace(name="API Access", licensed=50, used=20, utilization_percent=40.0),
                ProductWhitespace(name="Mobile Users", licensed=50, used=7, utilization_percent=14.0),
            ],
            expansion_opportunities=[
                ExpansionOpportunity(
                    product="Advanced Analytics",
                    potential_value=45000,
                    reason="High engagement with basic analytics; ready for advanced features",
                    confidence="high"
                ),
                ExpansionOpportunity(
                    product="Additional Licenses",
                    potential_value=25000,
                    reason="Core platform utilization at 91.7%, approaching limit",
                    confidence="medium"
                ),
            ]
        )

        # Contract Context
        contract = ContractContext(
            contract_type="Enterprise Annual",
            start_date=today - timedelta(days=280),
            end_date=today + timedelta(days=account_detail.renewal_days),
            renewal_date=today + timedelta(days=account_detail.renewal_days),
            days_until_renewal=account_detail.renewal_days,
            contract_value=150000,
            arr=account_detail.arr,
            mrr=account_detail.mrr,
            payment_terms="Net 30",
            auto_renewal=True,
            contract_history=[
                ContractEvent(
                    date=today - timedelta(days=280),
                    type="new",
                    description="Initial enterprise contract signed",
                    value_change=125000
                ),
                ContractEvent(
                    date=today - timedelta(days=180),
                    type="expansion",
                    description="Added Analytics Add-on",
                    value_change=25000
                ),
            ]
        )

        # Changes Since Last Touch
        changes_since_last_touch = [
            ChangeEvent(
                id="chg-1",
                date=now - timedelta(days=2),
                type="support",
                source="Zendesk",
                title="Critical ticket opened",
                description="API performance degradation reported",
                importance="high"
            ),
            ChangeEvent(
                id="chg-2",
                date=now - timedelta(days=5),
                type="usage",
                source="Product Analytics",
                title="Usage spike detected",
                description="30% increase in API calls over 24 hours",
                importance="medium"
            ),
            ChangeEvent(
                id="chg-3",
                date=now - timedelta(days=7),
                type="stakeholder",
                source="Salesforce",
                title="New contact added",
                description="VP of Engineering added as contact",
                importance="high"
            ),
            ChangeEvent(
                id="chg-4",
                date=now - timedelta(days=10),
                type="email",
                source="Email",
                title="Feature request received",
                description="Request for custom reporting capabilities",
                importance="medium"
            ),
            ChangeEvent(
                id="chg-5",
                date=now - timedelta(days=14),
                type="meeting",
                source="Calendar",
                title="QBR completed",
                description="Quarterly business review meeting with stakeholders",
                importance="high"
            ),
        ]

        # Risk Assessment
        risk_assessment = RiskAssessment(
            churn_risk_score=35,
            renewal_risk_score=42,
            risk_level="medium",
            risk_factors=[
                RiskFactor(
                    name="Usage Decline",
                    impact=65,
                    description="Usage decreased 11.5% compared to last quarter",
                    trend="stable"
                ),
                RiskFactor(
                    name="Support Issues",
                    impact=45,
                    description="2 critical tickets currently open",
                    trend="worsening"
                ),
                RiskFactor(
                    name="Stakeholder Changes",
                    impact=30,
                    description="Key champion left 2 months ago",
                    trend="improving"
                ),
            ],
            recommended_actions=[
                RecommendedAction(
                    title="Executive Business Review",
                    description="Schedule EBR with new VP of Engineering to rebuild executive alignment",
                    priority="high",
                    action_type="meeting"
                ),
                RecommendedAction(
                    title="Support Escalation",
                    description="Escalate critical API ticket to engineering leadership",
                    priority="urgent",
                    action_type="support"
                ),
                RecommendedAction(
                    title="Usage Workshop",
                    description="Conduct adoption workshop to address declining usage",
                    priority="medium",
                    action_type="enablement"
                ),
            ]
        )

        # Sentiment Analysis
        sentiment = SentimentAnalysis(
            overall_sentiment=15,
            sentiment_label="neutral",
            trend="stable",
            sources=[
                SentimentSource(type="email", sentiment=25, count=12),
                SentimentSource(type="meeting", sentiment=35, count=4),
                SentimentSource(type="support", sentiment=-15, count=34),
                SentimentSource(type="survey", sentiment=42, count=1),
            ],
            recent_interactions=[
                SentimentInteraction(
                    date=now - timedelta(days=2),
                    type="support",
                    sentiment=-30,
                    summary="Frustrated with API performance issues"
                ),
                SentimentInteraction(
                    date=now - timedelta(days=7),
                    type="email",
                    sentiment=50,
                    summary="Positive feedback on new features"
                ),
                SentimentInteraction(
                    date=now - timedelta(days=14),
                    type="meeting",
                    sentiment=35,
                    summary="Constructive QBR discussion"
                ),
            ]
        )

        # Benchmark Data
        benchmark = BenchmarkData(
            peer_group="Enterprise Technology (500-1000 employees)",
            metrics=[
                BenchmarkMetric(
                    name="Health Score",
                    account_value=74,
                    peer_average=72,
                    peer_median=75,
                    percentile=52
                ),
                BenchmarkMetric(
                    name="License Utilization",
                    account_value=77.4,
                    peer_average=68.5,
                    peer_median=70,
                    percentile=72
                ),
                BenchmarkMetric(
                    name="Support Tickets (monthly)",
                    account_value=34,
                    peer_average=28,
                    peer_median=25,
                    percentile=35
                ),
                BenchmarkMetric(
                    name="Feature Adoption",
                    account_value=62,
                    peer_average=58,
                    peer_median=60,
                    percentile=58
                ),
            ]
        )

        # Action Alerts
        alerts = [
            ActionAlert(
                id="alert-1",
                type="renewal_risk",
                title="Renewal Risk Alert",
                description=f"Contract expires in {account_detail.renewal_days} days and health score has decreased by 15 points over the last month.",
                severity="high",
                timestamp=now - timedelta(hours=10),
                suggested_action="Start Renewal Plan",
                action_url="/actions/renewal-plan",
                is_read=False
            ),
            ActionAlert(
                id="alert-2",
                type="upsell",
                title="Upsell Opportunity Identified",
                description="Usage is 125% of current license. Schedule a call to discuss with their VP, IT.",
                severity="medium",
                timestamp=now - timedelta(hours=30),
                suggested_action="View Details",
                is_read=False
            ),
            ActionAlert(
                id="alert-3",
                type="churn_risk",
                title="Churn Risk Detected",
                description="Multiple churn indicators - usage down 40%, 5 high severity tickets open for 20+ days.",
                severity="critical",
                timestamp=now - timedelta(hours=2),
                suggested_action="Escalate Issue",
                is_read=False
            ),
        ]

        # Signals
        signals = [
            SignalDetail(
                id="sig-1",
                type="usage",
                title="Usage Decline",
                description="Ticket volume up 28% over last 2 weeks",
                severity="high",
                timestamp=now - timedelta(days=1),
                source="Product Analytics",
                is_read=True
            ),
            SignalDetail(
                id="sig-2",
                type="contract",
                title="Trial Period Ending",
                description="Trial period ending next week, no usage last 5 days",
                severity="medium",
                timestamp=now - timedelta(days=3),
                source="Contract System",
                is_read=False
            ),
            SignalDetail(
                id="sig-3",
                type="stakeholder",
                title="New IT Contact",
                description="New IT contact attached recently",
                severity="low",
                timestamp=now - timedelta(days=5),
                source="CRM",
                is_read=True
            ),
        ]

        # Human Notes
        notes = [
            HumanNote(
                id="note-1",
                author="Sarah Robinson",
                author_email="sarah.robinson@company.com",
                content="Met with VP of Engineering. They're concerned about API performance but excited about the roadmap. Need to follow up on custom reporting ask.",
                created_at=now - timedelta(days=7),
                updated_at=now - timedelta(days=7),
                tags=["executive", "roadmap", "feature-request"]
            ),
            HumanNote(
                id="note-2",
                author="Thomas Nguyen",
                author_email="thomas.nguyen@company.com",
                content="Champion (former CTO) left the company. New VP seems supportive but we need to rebuild the relationship.",
                created_at=now - timedelta(days=45),
                updated_at=now - timedelta(days=30),
                tags=["stakeholder-change", "risk"]
            ),
        ]

        # Meeting Brief
        meeting_brief = MeetingBrief(
            generated_at=now,
            snapshot_id=f"snapshot-{account_id}-{now.strftime('%Y%m%d')}",
            summary=f"{account_detail.name} is a {account_detail.industry} company with {account_detail.employees or 'N/A'} employees. The account health is currently at 74/100 with a renewal coming up in {account_detail.renewal_days} days. Key concerns include declining usage (-11.5%) and open critical support tickets.",
            key_points=[
                "Health score: 74/100 (stable trend)",
                f"Renewal in {account_detail.renewal_days} days - $150K contract",
                "Usage declined 11.5% QoQ",
                "2 critical support tickets open",
                "New VP of Engineering recently joined",
            ],
            talking_points=[
                "Acknowledge API performance concerns and share remediation timeline",
                "Discuss custom reporting feature request and roadmap alignment",
                "Review adoption of new features released this quarter",
                "Explore expansion opportunity for Advanced Analytics",
            ],
            risks_to_address=[
                "API performance issues impacting user satisfaction",
                "Key champion departure - relationship rebuilding needed",
                "Usage trend declining - need to understand root cause",
            ],
            opportunities=[
                "Advanced Analytics upsell ($45K potential)",
                "Additional license expansion as utilization approaches limit",
                "New stakeholder (VP Engineering) is a potential new champion",
            ],
            recent_activity_summary="Last QBR was 14 days ago. Since then: 1 critical support ticket opened, new VP of Engineering contact added, positive email feedback on new features received.",
            recommended_topics=[
                "API performance remediation plan",
                "Custom reporting capabilities",
                "Advanced Analytics demo",
                "Renewal timeline and terms",
            ]
        )

        # Value Realization
        value_realization = ValueRealization(
            goals=[
                ValueGoal(
                    id="goal-1",
                    name="Reduce Support Ticket Volume",
                    target=20,
                    current=34,
                    unit="tickets/month",
                    status="behind",
                    due_date=today + timedelta(days=60)
                ),
                ValueGoal(
                    id="goal-2",
                    name="Increase User Adoption",
                    target=90,
                    current=77.4,
                    unit="%",
                    status="at_risk",
                    due_date=today + timedelta(days=90)
                ),
                ValueGoal(
                    id="goal-3",
                    name="API Response Time",
                    target=200,
                    current=185,
                    unit="ms",
                    status="on_track",
                    due_date=today + timedelta(days=30)
                ),
            ],
            overall_realization_percent=72,
            time_to_value_days=45,
            adoption_score=68
        )

        return AccountFullDetail(
            account=account_detail,
            health_breakdown=health_breakdown,
            support_analysis=support_analysis,
            usage_analysis=usage_analysis,
            whitespace=whitespace,
            contract=contract,
            changes_since_last_touch=changes_since_last_touch,
            risk_assessment=risk_assessment,
            sentiment=sentiment,
            benchmark=benchmark,
            alerts=alerts,
            signals=signals,
            notes=notes,
            meeting_brief=meeting_brief,
            value_realization=value_realization,
            last_updated=now,
            last_touch_date=now - timedelta(days=14),
        )


    # ==================== User Preferences Methods ====================

    PREFERENCES_TABLE = "silver.silver_layer.csm_app_user_preferences"

    @staticmethod
    def _sql_escape(value: str) -> str:
        """Escape a string for safe inline SQL (replace single quotes with two single quotes)."""
        return value.replace("'", "''")

    def get_user_preference(self, user_email: str, preference_key: str) -> Optional[dict]:
        """Get a user preference by key. Returns {value, updated_at} or None."""
        logger.info(f"get_user_preference: user={user_email}, key={preference_key}")
        with self.get_connection() as conn:
            if conn is None:
                logger.error("get_user_preference: connection is None")
                return None
            try:
                safe_email = self._sql_escape(user_email)
                safe_key = self._sql_escape(preference_key)
                cursor = conn.cursor()
                query = f"""
                    SELECT preference_value, updated_at
                    FROM {self.PREFERENCES_TABLE}
                    WHERE user_email = '{safe_email}' AND preference_key = '{safe_key}'
                """
                logger.info(f"get_user_preference query: {query.strip()}")
                cursor.execute(query)
                row = cursor.fetchone()
                cursor.close()
                if not row:
                    logger.info(f"get_user_preference: no entry found")
                    return None
                logger.info(f"get_user_preference: found entry, updated_at={row[1]}")
                return {
                    "value": row[0],
                    "updated_at": str(row[1]) if row[1] else None,
                }
            except Exception as e:
                logger.error(f"Error fetching user preference: {e}", exc_info=True)
                return None

    def save_user_preference(self, user_email: str, preference_key: str, preference_value: str) -> bool:
        """Save a user preference (upsert via check-then-insert/update). Returns True on success."""
        logger.info(f"save_user_preference: user={user_email}, key={preference_key}, value_len={len(preference_value)}")
        with self.get_connection() as conn:
            if conn is None:
                logger.error("save_user_preference: connection is None")
                return False
            try:
                safe_email = self._sql_escape(user_email)
                safe_key = self._sql_escape(preference_key)
                safe_value = self._sql_escape(preference_value)
                cursor = conn.cursor()

                # Step 1: Check if a row already exists
                check_query = f"""
                    SELECT 1 FROM {self.PREFERENCES_TABLE}
                    WHERE user_email = '{safe_email}' AND preference_key = '{safe_key}'
                """
                cursor.execute(check_query)
                exists = cursor.fetchone() is not None
                logger.info(f"save_user_preference: exists={exists}")

                if exists:
                    # Step 2a: UPDATE existing row
                    update_query = f"""
                        UPDATE {self.PREFERENCES_TABLE}
                        SET preference_value = '{safe_value}',
                            updated_at = CURRENT_TIMESTAMP()
                        WHERE user_email = '{safe_email}' AND preference_key = '{safe_key}'
                    """
                    logger.info(f"save_user_preference: running UPDATE")
                    cursor.execute(update_query)
                else:
                    # Step 2b: INSERT new row (first-time user)
                    insert_query = f"""
                        INSERT INTO {self.PREFERENCES_TABLE}
                            (user_email, preference_key, preference_value, updated_at)
                        VALUES ('{safe_email}', '{safe_key}', '{safe_value}', CURRENT_TIMESTAMP())
                    """
                    logger.info(f"save_user_preference: running INSERT for first-time user")
                    cursor.execute(insert_query)

                cursor.close()
                logger.info(f"save_user_preference: success")
                return True
            except Exception as e:
                logger.error(f"Error saving user preference: {e}", exc_info=True)
                return False


    # ==================== CSM Type Tags ====================

    CSM_TYPE_TAGS_TABLE = "silver.silver_layer.csm_type_tags"

    def get_csm_types(self) -> dict[str, str]:
        """Return all CSM type tags as {csm_id: csm_type}.

        CSMs without a row default to 'full-time' on the frontend.
        Rows are created on first tag change via update_csm_type.
        """
        try:
            with self.get_connection() as conn:
                if conn is None:
                    return {}
                cursor = conn.cursor()
                cursor.execute(f"""
                    SELECT csm_id, csm_type
                    FROM {self.CSM_TYPE_TAGS_TABLE}
                """)
                rows = cursor.fetchall()
                cursor.close()
                return {str(row[0]): str(row[1]) for row in rows}
        except Exception as e:
            logger.error(f"get_csm_types error: {e}", exc_info=True)
            return {}

    def update_csm_type(self, csm_id: str, csm_type: str) -> bool:
        """Upsert a single CSM's type tag (check-then-insert/update)."""
        try:
            with self.get_connection() as conn:
                if conn is None:
                    return False
                safe_id = self._sql_escape(csm_id)
                safe_type = self._sql_escape(csm_type)
                cursor = conn.cursor()

                cursor.execute(f"""
                    SELECT 1 FROM {self.CSM_TYPE_TAGS_TABLE}
                    WHERE csm_id = '{safe_id}'
                """)
                exists = cursor.fetchone() is not None

                if exists:
                    cursor.execute(f"""
                        UPDATE {self.CSM_TYPE_TAGS_TABLE}
                        SET csm_type = '{safe_type}',
                            updated_at = CURRENT_TIMESTAMP()
                        WHERE csm_id = '{safe_id}'
                    """)
                else:
                    cursor.execute(f"""
                        INSERT INTO {self.CSM_TYPE_TAGS_TABLE}
                            (csm_id, csm_type, updated_at)
                        VALUES ('{safe_id}', '{safe_type}', CURRENT_TIMESTAMP())
                    """)

                cursor.close()
                return True
        except Exception as e:
            logger.error(f"update_csm_type error: {e}", exc_info=True)
            return False

    # ==================== CSM Management Methods ====================

    def get_csm_stats(self, account_type: Optional[str] = None) -> CSMStats:
        """Get CSM management dashboard statistics."""
        logger.info(f"get_csm_stats called, account_type={account_type}")
        FCT_TABLE = FCT_CONTRACTS_TABLE
        with self.get_connection() as conn:
            if conn is None:
                logger.warning("Connection is None, returning empty CSM stats")
                return CSMStats(
                    active_csms=0,
                    avg_accounts_per_csm=0.0,
                    unassigned_accounts=0,
                    total_arr_managed=0.0,
                )

            try:
                cursor = conn.cursor()
                acct_filter = ""
                if account_type and account_type != "all":
                    safe_type = self._sql_escape(account_type)
                    acct_filter = f" AND c.account_type = '{safe_type}'"
                cursor.execute(f"""
                    SELECT 
                        COUNT(DISTINCT u.id) as active_csms,
                        COUNT(CASE WHEN c.csm_c IS NOT NULL AND c.csm_c != '' THEN 1 END) as assigned_accounts,
                        SUM(CASE WHEN c.csm_c IS NULL OR c.csm_c = '' THEN 1 ELSE 0 END) as unassigned_accounts
                    FROM {DIM_CUSTOMERS_TABLE} c
                    LEFT JOIN {DIM_USERS_TABLE} u ON c.csm_c = u.id
                    WHERE c._fivetran_deleted = false{acct_filter}
                """)
                row = cursor.fetchone()

                active_csms = row[0] or 0
                assigned_accounts = row[1] or 0
                unassigned_accounts = row[2] or 0

                # Get total ARR from fct_contracts
                cursor.execute(f"""
                    SELECT COALESCE(SUM(f.ARR_EUR), 0) as total_arr
                    FROM {FCT_TABLE} f
                    INNER JOIN {DIM_CUSTOMERS_TABLE} c ON f.account_id = c.account_id
                    WHERE c._fivetran_deleted = false
                      AND f.RENEWAL_NOT_YET_CONTRACTED = 'Y'
                      AND f.revenue_type NOT IN ('Services', 'Perpetual')
                      AND (f.churn_expected_occurred IS NULL OR f.churn_expected_occurred = 'nan')
                      AND f.rev_rec_end_date > CURRENT_DATE()
                """)
                arr_row = cursor.fetchone()
                total_arr = float(arr_row[0]) if arr_row and arr_row[0] else 0.0
                cursor.close()

                avg_accounts_per_csm = assigned_accounts / active_csms if active_csms > 0 else 0

                return CSMStats(
                    active_csms=active_csms,
                    avg_accounts_per_csm=round(avg_accounts_per_csm, 1),
                    unassigned_accounts=unassigned_accounts,
                    total_arr_managed=total_arr,
                )
            except Exception as e:
                logger.error(f"Error fetching CSM stats: {e}")
                return CSMStats(
                    active_csms=0,
                    avg_accounts_per_csm=0.0,
                    unassigned_accounts=0,
                    total_arr_managed=0.0,
                )

    def get_csm_assignment_history(self, csm_id: str) -> list[dict]:
        """Fetch assignment history for a CSM from the assignment history table.

        Rows stored as status 'Current' are reconciled in Python with dim_customers: if
        this CSM is not the active csm_c for that account_name, status is returned as
        'Handed Off' (stale LEAD()-based flags in the history table). SQL uses two simple
        queries so Databricks/connector dialect issues do not drop the whole result set.
        """
        HISTORY_TABLE = "silver.silver_layer.csm_assignment_history"
        safe_id = self._sql_escape(csm_id)
        with self.get_connection() as conn:
            if conn is None:
                return []
            try:
                cursor = conn.cursor()
                cursor.execute(f"""
                    SELECT
                        account_name,
                        csm_name,
                        csm_id,
                        assigned_from,
                        assigned_until,
                        days_held,
                        handed_off_from,
                        handed_off_from_id,
                        changed_by,
                        status
                    FROM {HISTORY_TABLE}
                    WHERE csm_id = '{safe_id}'
                    ORDER BY assigned_from DESC
                """)
                rows = cursor.fetchall()
                cursor.close()

                live_names: Optional[set[str]] = None
                try:
                    cur2 = conn.cursor()
                    cur2.execute(f"""
                        SELECT DISTINCT account
                        FROM {DIM_CUSTOMERS_TABLE}
                        WHERE csm_c = '{safe_id}' AND _fivetran_deleted = false
                    """)
                    live_names = set()
                    for r in cur2.fetchall():
                        if r[0] is not None:
                            live_names.add(str(r[0]).strip())
                    cur2.close()
                except Exception as e2:
                    logger.warning(f"Live CSM account lookup skipped (reconciliation off): {e2}")

                cols = [
                    "account_name", "csm_name", "csm_id", "assigned_from",
                    "assigned_until", "days_held", "handed_off_from",
                    "handed_off_from_id", "changed_by", "status",
                ]
                results = []
                for row in rows:
                    item = {}
                    for i, col in enumerate(cols):
                        val = row[i]
                        if val is not None and hasattr(val, 'isoformat'):
                            val = val.isoformat()
                        elif val is not None:
                            val = str(val) if col not in ("days_held",) else val
                        item[col] = val
                    if item.get("days_held") is not None:
                        try:
                            item["days_held"] = int(item["days_held"])
                        except (ValueError, TypeError):
                            item["days_held"] = 0
                    if live_names is not None:
                        raw_status = item.get("status")
                        st = str(raw_status).strip() if raw_status is not None else ""
                        if st == "Current":
                            acc = (item.get("account_name") or "")
                            acc = acc.strip() if isinstance(acc, str) else str(acc).strip()
                            if acc and acc not in live_names:
                                item["status"] = "Handed Off"
                    results.append(item)
                return results
            except Exception as e:
                logger.error(f"Error fetching CSM assignment history: {e}")
                return []

    def get_distinct_csm_names_from_assignment_history(self) -> list[str]:
        """Distinct CSM display names from csm_name and handed_off_from in assignment history."""
        HISTORY_TABLE = "silver.silver_layer.csm_assignment_history"
        with self.get_connection() as conn:
            if conn is None:
                return []
            try:
                cursor = conn.cursor()
                cursor.execute(f"""
                    SELECT DISTINCT TRIM(n) AS n FROM (
                        SELECT csm_name AS n FROM {HISTORY_TABLE}
                        WHERE csm_name IS NOT NULL AND TRIM(csm_name) != ''
                        UNION ALL
                        SELECT handed_off_from AS n FROM {HISTORY_TABLE}
                        WHERE handed_off_from IS NOT NULL AND TRIM(handed_off_from) != ''
                    ) q
                    WHERE TRIM(n) != ''
                    ORDER BY 1
                """)
                rows = cursor.fetchall()
                cursor.close()
                return [str(r[0]).strip() for r in rows if r and r[0]]
            except Exception as e:
                logger.error(f"get_distinct_csm_names_from_assignment_history: {e}", exc_info=True)
                return []

    @staticmethod
    def _parse_iso_to_sql_timestamp(iso: str) -> Optional[str]:
        """Return 'YYYY-MM-DD HH:mm:ss' for SQL TIMESTAMP literal, or None."""
        if not iso or str(iso).strip() in ("", "-", "null", "None"):
            return None
        try:
            s = str(iso).strip().replace("Z", "+00:00")
            d = datetime.fromisoformat(s)
            return d.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return None

    def delete_csm_assignment_history_record(
        self,
        csm_id: str,
        account_name: str,
        assigned_from: str,
    ) -> bool:
        """Delete one row from csm_assignment_history by natural key."""
        HISTORY_TABLE = "silver.silver_layer.csm_assignment_history"
        safe_csm = self._sql_escape(csm_id)
        safe_acc = self._sql_escape(account_name)
        ts = self._parse_iso_to_sql_timestamp(assigned_from)
        if not ts:
            return False
        safe_ts = self._sql_escape(ts)
        try:
            with self.get_connection() as conn:
                if conn is None:
                    return False
                cursor = conn.cursor()
                cursor.execute(f"""
                    DELETE FROM {HISTORY_TABLE}
                    WHERE csm_id = '{safe_csm}'
                      AND account_name = '{safe_acc}'
                      AND assigned_from = TIMESTAMP('{safe_ts}')
                """)
                cursor.close()
                return True
        except Exception as e:
            logger.error(f"delete_csm_assignment_history_record: {e}", exc_info=True)
            return False

    def update_csm_assignment_history_record(
        self,
        csm_id: str,
        account_name: str,
        assigned_from_key: str,
        handed_off_from: Optional[str],
        handed_off_from_id: Optional[str],
        assigned_from_new: str,
        assigned_until: Optional[str],
        status: str,
    ) -> bool:
        """Update one assignment history row. assigned_until empty -> NULL (open)."""
        HISTORY_TABLE = "silver.silver_layer.csm_assignment_history"
        safe_csm = self._sql_escape(csm_id)
        safe_acc = self._sql_escape(account_name)
        safe_status = self._sql_escape(status)

        key_ts = self._parse_iso_to_sql_timestamp(assigned_from_key)
        new_from_ts = self._parse_iso_to_sql_timestamp(assigned_from_new)
        if not key_ts or not new_from_ts:
            return False
        safe_key = self._sql_escape(key_ts)
        safe_new_from = self._sql_escape(new_from_ts)

        until_raw = self._parse_iso_to_sql_timestamp(assigned_until) if assigned_until else None
        until_sql = f"TIMESTAMP('{self._sql_escape(until_raw)}')" if until_raw else "NULL"

        ho_sql = f"'{self._sql_escape(handed_off_from)}'" if handed_off_from else "NULL"
        hoid_sql = f"'{self._sql_escape(handed_off_from_id)}'" if handed_off_from_id else "NULL"

        try:
            s_dt = datetime.fromisoformat(str(assigned_from_new).replace("Z", "+00:00"))
            if until_raw:
                e_dt = datetime.fromisoformat(str(assigned_until).replace("Z", "+00:00"))
                days_val = max(0, (e_dt - s_dt).days)
            else:
                days_val = max(0, (date.today() - s_dt.date()).days)
            days_sql = str(days_val)
        except Exception:
            days_sql = "NULL"

        try:
            with self.get_connection() as conn:
                if conn is None:
                    return False
                cursor = conn.cursor()
                cursor.execute(f"""
                    UPDATE {HISTORY_TABLE}
                    SET
                        assigned_from = TIMESTAMP('{safe_new_from}'),
                        assigned_until = {until_sql},
                        days_held = {days_sql},
                        handed_off_from = {ho_sql},
                        handed_off_from_id = {hoid_sql},
                        status = '{safe_status}'
                    WHERE csm_id = '{safe_csm}'
                      AND account_name = '{safe_acc}'
                      AND assigned_from = TIMESTAMP('{safe_key}')
                """)
                cursor.close()
                return True
        except Exception as e:
            logger.error(f"update_csm_assignment_history_record: {e}", exc_info=True)
            return False

    def insert_csm_assignment_history_record(
        self,
        csm_id: str,
        csm_name: str,
        account_name: str,
        assigned_from: str,
        assigned_until: Optional[str],
        handed_off_from: Optional[str],
        handed_off_from_id: Optional[str],
        status: str,
    ) -> bool:
        """Insert one row into csm_assignment_history."""
        HISTORY_TABLE = "silver.silver_layer.csm_assignment_history"
        safe_csm = self._sql_escape(csm_id)
        safe_csm_name = self._sql_escape(csm_name)
        safe_acc = self._sql_escape(account_name)
        safe_status = self._sql_escape(status)

        new_from_ts = self._parse_iso_to_sql_timestamp(assigned_from)
        if not new_from_ts:
            return False
        safe_new_from = self._sql_escape(new_from_ts)

        until_raw = self._parse_iso_to_sql_timestamp(assigned_until) if assigned_until else None
        until_sql = f"TIMESTAMP('{self._sql_escape(until_raw)}')" if until_raw else "NULL"

        ho_sql = f"'{self._sql_escape(handed_off_from)}'" if handed_off_from else "NULL"
        hoid_sql = f"'{self._sql_escape(handed_off_from_id)}'" if handed_off_from_id else "NULL"

        try:
            s_dt = datetime.fromisoformat(str(assigned_from).replace("Z", "+00:00"))
            if until_raw:
                e_dt = datetime.fromisoformat(str(assigned_until).replace("Z", "+00:00"))
                days_val = max(0, (e_dt - s_dt).days)
            else:
                days_val = max(0, (date.today() - s_dt.date()).days)
            days_sql = str(days_val)
        except Exception:
            days_sql = "0"

        try:
            with self.get_connection() as conn:
                if conn is None:
                    return False
                cursor = conn.cursor()
                cursor.execute(f"""
                    INSERT INTO {HISTORY_TABLE} (
                        account_name, csm_name, csm_id, assigned_from, assigned_until,
                        days_held, handed_off_from, handed_off_from_id, changed_by, status
                    ) VALUES (
                        '{safe_acc}',
                        '{safe_csm_name}',
                        '{safe_csm}',
                        TIMESTAMP('{safe_new_from}'),
                        {until_sql},
                        {days_sql},
                        {ho_sql},
                        {hoid_sql},
                        'manual',
                        '{safe_status}'
                    )
                """)
                cursor.close()
                return True
        except Exception as e:
            logger.error(f"insert_csm_assignment_history_record: {e}", exc_info=True)
            return False

    def _drill_feedback_url(
        self, source: Optional[str], ticket_id: Any, record_id: Any
    ) -> Optional[str]:
        """Build Freshdesk ticket or SurveyMonkey response URL when configured."""
        src = (source or "").strip()
        if "Freshdesk" in src and ticket_id is not None:
            base = (self.settings.freshdesk_portal_base or "").strip().rstrip("/")
            if base:
                try:
                    tid = int(ticket_id)
                    return f"{base}/a/tickets/{tid}"
                except (TypeError, ValueError):
                    return None
        if "Survey" in src and record_id is not None:
            tpl = (self.settings.survey_monkey_response_url_template or "").strip()
            if tpl:
                rid = str(record_id).strip()
                return tpl.replace("{response_id}", rid)
        return None

    @staticmethod
    def _flatten_survey_monkey_pivot_row(description: Any, row: tuple) -> List[dict]:
        """Turn pivot row (finance column names) into ordered list of {question, answer} for the UI."""
        meta_display = {
            "response_id": None,
            "survey_type": "Survey type",
            "company": "Company",
            "respondent": "Respondent",
            "email": "Email",
            "response_date": "Response date",
            "ifs_contact": "IFS contact",
        }
        out: List[dict] = []
        cols = description or ()
        for i, col in enumerate(cols):
            raw = col[0] if col else ""
            name = (raw or "").strip()
            lname = name.lower()
            if lname == "response_id":
                continue
            val = row[i] if i < len(row) else None
            if val is None:
                continue
            if hasattr(val, "isoformat"):
                sval = val.isoformat()
            else:
                sval = str(val).strip()
            if not sval:
                continue
            if lname in meta_display:
                disp = meta_display[lname]
                if disp is None:
                    continue
                q_label = disp
            else:
                q_label = name
            out.append({"question": q_label, "answer": sval})
        return out

    def _merge_csm_feedback_details(self, safe_csm_id: str, responses: List[dict]) -> None:
        """Attach survey_questions (SurveyMonkey pivot) and csat_entries (all Freshdesk CSAT lines per ticket)."""
        from .csm_feedback_query import (
            build_freshdesk_csat_for_tickets_sql,
            build_survey_monkey_pivot_for_response_ids_sql,
        )

        sm_ids: List[int] = []
        for item in responses:
            if "Survey" not in str(item.get("source") or ""):
                continue
            rid = item.get("record_id")
            if rid is None:
                continue
            try:
                sm_ids.append(int(rid))
            except (TypeError, ValueError):
                try:
                    sm_ids.append(int(float(rid)))
                except (TypeError, ValueError):
                    continue
        uniq_sm = sorted(set(sm_ids))

        sm_by: dict = {}
        if uniq_sm:
            chunk_size = 100
            try:
                with self.get_connection() as conn:
                    if conn is None:
                        pass
                    else:
                        for i in range(0, len(uniq_sm), chunk_size):
                            chunk = uniq_sm[i : i + chunk_size]
                            ids_in = ",".join(str(x) for x in chunk)
                            sql = build_survey_monkey_pivot_for_response_ids_sql(
                                safe_csm_id, DIM_CUSTOMERS_TABLE, ids_in
                            )
                            cur = conn.cursor()
                            cur.execute(sql)
                            desc = cur.description
                            for row in cur.fetchall():
                                rid = row[0] if row else None
                                if rid is None:
                                    continue
                                rkey = str(int(rid))
                                sm_by[rkey] = self._flatten_survey_monkey_pivot_row(desc, row)
                            cur.close()
            except Exception as e:
                logger.warning(f"SurveyMonkey pivot enrichment skipped: {e}", exc_info=True)

        for item in responses:
            if "Survey" in str(item.get("source") or ""):
                rid = item.get("record_id")
                if rid is not None:
                    try:
                        rkey = str(int(rid))
                    except (TypeError, ValueError):
                        try:
                            rkey = str(int(float(rid)))
                        except (TypeError, ValueError):
                            rkey = str(rid)
                    item["survey_questions"] = sm_by.get(rkey, [])

        ticket_ids: List[int] = []
        for item in responses:
            if "Freshdesk" not in str(item.get("source") or ""):
                continue
            tid = item.get("ticket_id")
            if tid is None:
                continue
            try:
                ticket_ids.append(int(tid))
            except (TypeError, ValueError):
                continue
        uniq = sorted(set(ticket_ids))
        if not uniq:
            return

        fd_by: dict = {}
        chunk_size = 150
        try:
            with self.get_connection() as conn:
                if conn is None:
                    return
                for i in range(0, len(uniq), chunk_size):
                    chunk = uniq[i : i + chunk_size]
                    ids_in = ",".join(str(x) for x in chunk)
                    sql = build_freshdesk_csat_for_tickets_sql(ids_in)
                    cur = conn.cursor()
                    cur.execute(sql)
                    for row in cur.fetchall():
                        tid_raw = row[0]
                        label = row[1]
                        val = row[2]
                        rlab = row[3]
                        npc = row[4]
                        fb = row[5]
                        rdt = row[6]
                        if rdt is not None and hasattr(rdt, "isoformat"):
                            rdt = rdt.isoformat()
                        elif rdt is not None:
                            rdt = str(rdt)
                        entry = {
                            "label": str(label) if label is not None else None,
                            "value": int(val) if val is not None else None,
                            "rating_label": str(rlab) if rlab is not None else None,
                            "nps_category": str(npc) if npc is not None else None,
                            "feedback": str(fb) if fb is not None else None,
                            "response_date": rdt,
                        }
                        tid_key = str(int(tid_raw)) if tid_raw is not None else ""
                        fd_by.setdefault(tid_key, []).append(entry)
                    cur.close()
        except Exception as e:
            logger.warning(f"Freshdesk CSAT enrichment skipped: {e}", exc_info=True)

        for item in responses:
            if "Freshdesk" not in str(item.get("source") or ""):
                continue
            tid = item.get("ticket_id")
            if tid is None:
                continue
            try:
                tk = str(int(tid))
            except (TypeError, ValueError):
                tk = str(tid)
            item["csat_entries"] = fd_by.get(tk, [])

    def get_csm_feedback_satisfaction(self, csm_id: str) -> dict:
        """NPS/CSAT rows for accounts owned by this CSM (SurveyMonkey + Freshdesk)."""
        from .csm_feedback_query import build_csm_feedback_sql

        empty: dict = {
            "summary": {
                "total": 0,
                "survey_monkey_count": 0,
                "freshdesk_count": 0,
                "promoters": 0,
                "passives": 0,
                "detractors": 0,
                "uncategorized": 0,
                "nps": None,
                "unique_customers": 0,
            },
            "by_customer": [],
            "responses": [],
        }
        safe = self._sql_escape(csm_id)
        with self.get_connection() as conn:
            if conn is None:
                return empty
            try:
                sql = build_csm_feedback_sql(safe, DIM_CUSTOMERS_TABLE, DIM_USERS_TABLE)
                cursor = conn.cursor()
                cursor.execute(sql)
                rows = cursor.fetchall()
                cursor.close()
            except Exception as e:
                logger.error(f"get_csm_feedback_satisfaction: {e}", exc_info=True)
                return empty

        cols = [
            "source",
            "record_id",
            "ticket_id",
            "csm",
            "ae",
            "customer_name",
            "region",
            "respondent_email",
            "respondent_name",
            "raw_score",
            "rating_label",
            "nps_category",
            "response_status",
            "feedback",
            "ticket_subject",
            "ticket_status",
            "response_date",
        ]
        responses: List[dict] = []
        str_cols = (
            "feedback", "rating_label", "nps_category", "response_status", "csm", "ae",
            "customer_name", "region", "respondent_email", "respondent_name", "source",
            "ticket_subject", "ticket_status",
        )
        for row in rows:
            item: dict = {}
            for i, col in enumerate(cols):
                v = row[i] if i < len(row) else None
                if v is not None and hasattr(v, "isoformat"):
                    v = v.isoformat()
                elif v is not None and col in str_cols:
                    v = str(v)
                item[col] = v

            src = item.get("source") or ""
            drill = self._drill_feedback_url(src, item.get("ticket_id"), item.get("record_id"))
            item["drill_url"] = drill
            if drill:
                item["drill_label"] = "Open ticket" if "Freshdesk" in str(src) else "View survey"
            else:
                item["drill_label"] = None
            item["survey_questions"] = []
            item["csat_entries"] = []
            responses.append(item)

        self._merge_csm_feedback_details(safe, responses)

        sm = fd = 0
        p = pa = d = u = 0
        by_cust: dict = {}

        for item in responses:
            s = str(item.get("source") or "")
            if "Survey" in s:
                sm += 1
            elif "Freshdesk" in s:
                fd += 1
            cat = item.get("nps_category")
            if cat == "Promoter":
                p += 1
            elif cat == "Passive":
                pa += 1
            elif cat == "Detractor":
                d += 1
            else:
                u += 1

            cn = (str(item.get("customer_name") or "")).strip() or "Unknown"
            reg = item.get("region")
            if cn not in by_cust:
                by_cust[cn] = {
                    "customer_name": cn,
                    "region": reg,
                    "count": 0,
                    "promoters": 0,
                    "passives": 0,
                    "detractors": 0,
                    "last_response_date": None,
                }
            bc = by_cust[cn]
            bc["count"] += 1
            if cat == "Promoter":
                bc["promoters"] += 1
            elif cat == "Passive":
                bc["passives"] += 1
            elif cat == "Detractor":
                bc["detractors"] += 1
            rdt = item.get("response_date")
            if rdt and (bc["last_response_date"] is None or str(rdt) > str(bc["last_response_date"])):
                bc["last_response_date"] = rdt

        total = len(responses)
        nps_cat_total = p + pa + d
        nps_val: Optional[float] = None
        if nps_cat_total > 0:
            nps_val = round((p - d) / nps_cat_total * 100, 1)

        by_customer = sorted(by_cust.values(), key=lambda x: (-x["count"], x["customer_name"]))

        empty["summary"] = {
            "total": total,
            "survey_monkey_count": sm,
            "freshdesk_count": fd,
            "promoters": p,
            "passives": pa,
            "detractors": d,
            "uncategorized": u,
            "nps": nps_val,
            "unique_customers": len(by_customer),
        }
        empty["by_customer"] = by_customer
        empty["responses"] = responses
        return empty

    def get_csm_profile(self, csm_id: str) -> Optional[dict]:
        """Fetch CSM profile from Salesforce user table."""
        SF_USER_TABLE = "workspace.salesforce.user"
        safe_id = self._sql_escape(csm_id)
        with self.get_connection() as conn:
            if conn is None:
                return None
            try:
                cursor = conn.cursor()
                cursor.execute(f"""
                    SELECT
                        u.id,
                        u.name,
                        u.email,
                        sf.phone,
                        sf.mobile_phone,
                        sf.title,
                        sf.department,
                        sf.division,
                        sf.city,
                        sf.state,
                        sf.country,
                        sf.time_zone_sid_key,
                        sf.region_c,
                        sf.is_active,
                        sf.created_date,
                        sf.last_login_date,
                        sf.full_photo_url,
                        sf.medium_photo_url,
                        sf.manager_id,
                        mgr.name AS reports_to
                    FROM {DIM_USERS_TABLE} u
                    LEFT JOIN {SF_USER_TABLE} sf ON sf.id = u.id
                    LEFT JOIN {SF_USER_TABLE} mgr ON sf.manager_id = mgr.id
                    WHERE u.id = '{safe_id}'
                """)
                row = cursor.fetchone()
                cursor.close()
                if not row:
                    return None

                cols = [
                    "id", "name", "email", "phone", "mobile_phone",
                    "title", "department", "division", "city", "state",
                    "country", "timezone", "region", "is_active",
                    "joined_date", "last_login_date", "full_photo_url",
                    "medium_photo_url", "manager_id", "reports_to",
                ]
                profile = {cols[i]: row[i] for i in range(len(cols))}

                location_parts = [p for p in [profile.get("city"), profile.get("state"), profile.get("country")] if p]
                profile["location"] = ", ".join(location_parts) if location_parts else None

                if profile.get("joined_date"):
                    from datetime import datetime, date as date_type
                    try:
                        jd = profile["joined_date"]
                        if isinstance(jd, str):
                            jd = datetime.fromisoformat(jd.replace("Z", "+00:00"))
                        if isinstance(jd, (datetime, date_type)):
                            today = datetime.now()
                            delta = today - (jd if isinstance(jd, datetime) else datetime.combine(jd, datetime.min.time()))
                            profile["tenure_months"] = max(int(delta.days / 30.44), 0)
                        else:
                            profile["tenure_months"] = None
                    except Exception:
                        profile["tenure_months"] = None
                else:
                    profile["tenure_months"] = None

                return profile
            except Exception as e:
                logger.error(f"Error fetching CSM profile: {e}")
                return None

    def get_csms(self, status: Optional[str] = None, account_type: Optional[str] = None) -> CSMListResponse:
        """Get list of all CSMs with their metrics.
        
        Args:
            status: Filter by CSM status (active, inactive, departed). None returns all.
            account_type: Filter accounts by type (Customer, Prospect, etc.). None returns all.
        """
        logger.info(f"get_csms called with status={status}, account_type={account_type}")
        FCT_TABLE = FCT_CONTRACTS_TABLE
        with self.get_connection() as conn:
            if conn is None:
                logger.warning("Connection is None, returning empty CSM list")
                return CSMListResponse(csms=[], total=0)

            try:
                cursor = conn.cursor()
                acct_filter = ""
                if account_type and account_type != "all":
                    safe_type = self._sql_escape(account_type)
                    acct_filter = f" AND c.account_type = '{safe_type}'"
                cursor.execute(f"""
                    SELECT 
                        u.id,
                        u.name,
                        u.email,
                        COUNT(DISTINCT c.account_id) as account_count,
                        COALESCE(SUM(f.ARR_EUR), 0) as total_arr,
                        COUNT(DISTINCT CASE 
                            WHEN f.rev_rec_end_date IS NOT NULL 
                                AND f.rev_rec_end_date <= DATE_ADD(CURRENT_DATE(), 90)
                                AND f.rev_rec_end_date >= CURRENT_DATE()
                            THEN c.account_id 
                        END) as at_risk_count
                    FROM {DIM_USERS_TABLE} u
                    INNER JOIN {DIM_CUSTOMERS_TABLE} c ON c.csm_c = u.id AND c._fivetran_deleted = false{acct_filter}
                    LEFT JOIN {FCT_TABLE} f ON c.account_id = f.account_id 
                        AND f.RENEWAL_NOT_YET_CONTRACTED = 'Y'
                        AND f.revenue_type NOT IN ('Services', 'Perpetual')
                        AND (f.churn_expected_occurred IS NULL OR f.churn_expected_occurred = 'nan')
                        AND f.rev_rec_end_date > CURRENT_DATE()
                    GROUP BY u.id, u.name, u.email
                    ORDER BY account_count DESC
                """)
                rows = cursor.fetchall()
                cursor.close()

                csms = []
                for row in rows:
                    csms.append(CSM(
                        id=row[0] or "",
                        name=row[1] or "Unknown",
                        email=row[2],
                        status="active",
                        account_count=row[3] or 0,
                        total_arr=float(row[4]) if row[4] else 0.0,
                        at_risk_count=row[5] or 0,
                    ))

                # Filter by status if specified
                if status:
                    csms = [c for c in csms if c.status == status]

                return CSMListResponse(csms=csms, total=len(csms))
            except Exception as e:
                logger.error(f"Error fetching CSMs: {e}")
                return CSMListResponse(csms=[], total=0)

    def get_accounts_with_csm(
        self,
        page: int = 1,
        page_size: int = 20,
        csm_id: Optional[str] = None,
        unassigned_only: bool = False,
        search: Optional[str] = None,
        account_type: Optional[str] = None,
    ) -> Tuple[List[AccountWithCSM], int]:
        """Get accounts with their CSM assignment info."""
        logger.info(f"get_accounts_with_csm called: page={page}, csm_id={csm_id}, unassigned_only={unassigned_only}, account_type={account_type}")
        with self.get_connection() as conn:
            if conn is None:
                logger.warning("Connection is None, returning no accounts with CSM")
                return [], 0

            try:
                cursor = conn.cursor()

                # Base query with fct_contracts for ARR and renewal dates
                FCT_TABLE = FCT_CONTRACTS_TABLE
                base_query = f"""
                    SELECT 
                        c.account_id,
                        c.account AS name,
                        c.account_type,
                        c.csm_c,
                        u.name as csm_name,
                        COALESCE(arr_data.total_arr, 0) as arr,
                        r_saas.max_end_date AS saas_renewal_date,
                        DATEDIFF(r_saas.max_end_date, CURRENT_DATE()) AS saas_renewal_days,
                        r_esma.max_end_date AS esma_renewal_date,
                        DATEDIFF(r_esma.max_end_date, CURRENT_DATE()) AS esma_renewal_days
                    FROM {DIM_CUSTOMERS_TABLE} c
                    LEFT JOIN {DIM_USERS_TABLE} u ON c.csm_c = u.id
                    LEFT JOIN (
                        SELECT account_id, SUM(ARR_EUR) as total_arr
                        FROM {FCT_TABLE}
                        WHERE RENEWAL_NOT_YET_CONTRACTED = 'Y'
                          AND revenue_type NOT IN ('Services', 'Perpetual')
                          AND (churn_expected_occurred IS NULL OR churn_expected_occurred = 'nan')
                          AND rev_rec_end_date > CURRENT_DATE()
                        GROUP BY account_id
                    ) arr_data ON c.account_id = arr_data.account_id
                    LEFT JOIN (
                        SELECT ACCOUNT_ID, MAX(REV_REC_END_DATE) AS max_end_date
                        FROM {FCT_TABLE}
                        WHERE RENEWAL_NOT_YET_CONTRACTED = 'Y' AND revenue_type = 'SaaS'
                        GROUP BY ACCOUNT_ID
                    ) r_saas ON c.account_id = r_saas.ACCOUNT_ID
                    LEFT JOIN (
                        SELECT ACCOUNT_ID, MAX(REV_REC_END_DATE) AS max_end_date
                        FROM {FCT_TABLE}
                        WHERE RENEWAL_NOT_YET_CONTRACTED = 'Y' AND revenue_type = 'eSMA'
                        GROUP BY ACCOUNT_ID
                    ) r_esma ON c.account_id = r_esma.ACCOUNT_ID
                    WHERE c._fivetran_deleted = false
                """

                params = []

                # Apply filters
                if account_type and account_type != "all":
                    base_query += " AND c.account_type = ?"
                    params.append(account_type)

                if csm_id:
                    base_query += " AND c.csm_c = ?"
                    params.append(csm_id)

                if unassigned_only:
                    base_query += " AND (c.csm_c IS NULL OR c.csm_c = '')"

                if search:
                    base_query += " AND LOWER(c.account) LIKE LOWER(?)"
                    params.append(f"%{search}%")

                # Get total count
                count_query = f"SELECT COUNT(*) FROM ({base_query}) as filtered"
                cursor.execute(count_query, params)
                total = cursor.fetchone()[0]

                # Add ordering and pagination
                base_query += " ORDER BY c.account ASC"
                offset = (page - 1) * page_size
                base_query += f" LIMIT {page_size} OFFSET {offset}"

                cursor.execute(base_query, params)
                rows = cursor.fetchall()
                cursor.close()

                accounts = []
                for row in rows:
                    # row indices: 0=account_id, 1=name, 2=account_type, 3=csm_c, 4=csm_name
                    # 5=arr, 6=saas_renewal_date, 7=saas_renewal_days, 8=esma_renewal_date, 9=esma_renewal_days
                    saas_renewal_date = row[6]
                    saas_renewal_days = row[7]
                    esma_renewal_date = row[8]
                    esma_renewal_days = row[9]

                    # Parse renewal dates safely
                    def _parse_date_safe(val):
                        if val is None:
                            return None
                        try:
                            s = str(val).strip()
                            if not s or s.lower() in ('nan', 'none', 'null', ''):
                                return None
                            from datetime import date as _date
                            return _date.fromisoformat(s[:10])
                        except Exception:
                            return None

                    # Build renewals list from fct_contracts data
                    renewals_list = []
                    parsed_saas = _parse_date_safe(saas_renewal_date)
                    parsed_esma = _parse_date_safe(esma_renewal_date)
                    if parsed_saas:
                        renewals_list.append(RenewalInfo(
                            revenue_type="SaaS",
                            renewal_date=parsed_saas,
                            renewal_days=int(saas_renewal_days) if saas_renewal_days is not None else None,
                        ))
                    if parsed_esma:
                        renewals_list.append(RenewalInfo(
                            revenue_type="eSMA",
                            renewal_date=parsed_esma,
                            renewal_days=int(esma_renewal_days) if esma_renewal_days is not None else None,
                        ))

                    # Use nearest contract renewal
                    nearest_days = 999
                    nearest_date = None
                    if renewals_list:
                        valid = [r for r in renewals_list if r.renewal_days is not None]
                        if valid:
                            nearest = min(valid, key=lambda r: abs(r.renewal_days))
                            nearest_days = nearest.renewal_days
                            nearest_date = nearest.renewal_date

                    # Derive health score (engagement columns removed from schema)
                    health = self._derive_health_score(nearest_days, None, None, None)

                    accounts.append(AccountWithCSM(
                        id=row[0] or "",
                        name=row[1] or "Unknown",
                        account_type=row[2],
                        csm_id=row[3],
                        csm_name=row[4],
                        arr=float(row[5]) if row[5] else 0.0,
                        health=health.value,
                        renewal_date=nearest_date,
                        renewal_days=nearest_days,
                        renewals=renewals_list,
                    ))

                return accounts, total
            except Exception as e:
                logger.error(f"Error fetching accounts with CSM: {e}")
                return [], 0

    # ==================== ARR Analysis Methods ====================

    def get_arr_analysis(
        self,
        page: int = 1,
        page_size: int = 50,
        revenue_type: Optional[str] = None,
        region: Optional[str] = None,
        search: Optional[str] = None,
        currency: str = "CAD",
        renewal_period: int = 90,
        account_type: Optional[str] = None,
    ) -> dict:
        """Get ARR analysis data from FCT_CONTRACT table joined with dim_customers for industry."""
        logger.info(f"get_arr_analysis called: page={page}, revenue_type={revenue_type}, region={region}, account_type={account_type}")
        
        FCT_CONTRACT_TABLE = FCT_CONTRACTS_TABLE
        DIM_CUSTOMER_TABLE = "silver.silver_layer.dim_customers"
        
        with self.get_connection() as conn:
            if conn is None:
                logger.error("Connection is None - cannot fetch ARR data")
                raise Exception("Database connection failed")

            cursor = conn.cursor()

            # Common WHERE for all top-section queries (Finance-approved logic)
            base_where = f"""
                c.RENEWAL_NOT_YET_CONTRACTED = 'Y'
                AND c.revenue_type NOT IN ('Services', 'Perpetual')
                AND (c.churn_expected_occurred IS NULL OR c.churn_expected_occurred = 'nan')
                AND c.rev_rec_end_date > CURRENT_DATE()
                AND c.rev_rec_end_date <= DATE_ADD(CURRENT_DATE(), {int(renewal_period)})
            """
            
            # Add account_type filter if specified
            if account_type:
                safe_type = account_type.replace("'", "''")
                base_where += f"\n                AND dc.account_type = '{safe_type}'"

            # Portfolio summary (ARR EUR, TCV, customers, contracts)
            logger.info("Fetching ARR portfolio summary...")
            summary_query = f"""
                SELECT
                    COUNT(DISTINCT c.CONTRACT_GROUP) as total_contracts,
                    COUNT(DISTINCT c.account_id) as total_customers,
                    COALESCE(ROUND(SUM(c.ARR_EUR), 0), 0) as total_arr_eur,
                    COALESCE(ROUND(SUM(c.BOOKING_TCV_CAD), 0), 0) as total_tcv_cad,
                    COALESCE(ROUND(SUM(c.ARR_CONTRACT_CURRENCY), 0), 0) as total_arr_native
                FROM {FCT_CONTRACT_TABLE} c
                LEFT JOIN {DIM_CUSTOMER_TABLE} dc ON c.account_id = dc.account_id
                WHERE {base_where}
                  AND dc._fivetran_deleted = false
            """
            cursor.execute(summary_query)
            summary_row = cursor.fetchone()
            logger.info(f"Summary: contracts={summary_row[0]}, customers={summary_row[1]}, arr_eur={summary_row[2]}")

            # Renewals in next 90 days
            logger.info("Fetching renewals...")
            renewals_query = f"""
                SELECT
                    COUNT(DISTINCT c.CONTRACT_GROUP) as renewal_count,
                    COALESCE(ROUND(SUM(c.ARR_EUR), 0), 0) as renewal_arr_eur
                FROM {FCT_CONTRACT_TABLE} c
                LEFT JOIN {DIM_CUSTOMER_TABLE} dc ON c.account_id = dc.account_id
                WHERE {base_where}
                  AND dc._fivetran_deleted = false
                  AND c.rev_rec_end_date <= DATE_ADD(CURRENT_DATE(), 90)
            """
            cursor.execute(renewals_query)
            renewals_row = cursor.fetchone()

            # Breakdown by revenue type
            logger.info("Fetching revenue type breakdown...")
            revenue_type_query = f"""
                SELECT
                    c.revenue_type,
                    COALESCE(ROUND(SUM(c.ARR_EUR), 0), 0) as arr_eur,
                    COALESCE(ROUND(SUM(c.BOOKING_TCV_CAD), 0), 0) as tcv_cad,
                    COUNT(DISTINCT c.CONTRACT_GROUP) as contract_count,
                    COUNT(DISTINCT c.account_id) as customer_count
                FROM {FCT_CONTRACT_TABLE} c
                LEFT JOIN {DIM_CUSTOMER_TABLE} dc ON c.account_id = dc.account_id
                WHERE {base_where}
                  AND dc._fivetran_deleted = false
                  AND c.revenue_type IS NOT NULL AND c.revenue_type != ''
                GROUP BY c.revenue_type
                ORDER BY 2 DESC
            """
            cursor.execute(revenue_type_query)
            revenue_type_rows = cursor.fetchall()

            # Breakdown by region (from dim_customers.region)
            logger.info("Fetching region breakdown...")
            region_query = f"""
                SELECT
                    COALESCE(dc.region, 'Unknown') as region,
                    COALESCE(ROUND(SUM(c.ARR_EUR), 0), 0) as arr_eur,
                    COALESCE(ROUND(SUM(c.BOOKING_TCV_CAD), 0), 0) as tcv_cad,
                    COUNT(DISTINCT c.account_id) as customer_count
                FROM {FCT_CONTRACT_TABLE} c
                LEFT JOIN {DIM_CUSTOMER_TABLE} dc ON c.account_id = dc.account_id
                WHERE {base_where}
                  AND dc._fivetran_deleted = false
                GROUP BY COALESCE(dc.region, 'Unknown')
                ORDER BY 2 DESC
            """
            cursor.execute(region_query)
            region_rows = cursor.fetchall()

            # Breakdown by industry (from dim_customers.industry)
            logger.info("Fetching industry breakdown...")
            industry_query = f"""
                SELECT
                    COALESCE(dc.industry, 'Unknown') as industry,
                    COALESCE(ROUND(SUM(c.ARR_EUR), 0), 0) as arr_eur,
                    COALESCE(ROUND(SUM(c.BOOKING_TCV_CAD), 0), 0) as tcv_cad,
                    COUNT(DISTINCT c.account_id) as customer_count
                FROM {FCT_CONTRACT_TABLE} c
                LEFT JOIN {DIM_CUSTOMER_TABLE} dc ON c.account_id = dc.account_id
                WHERE {base_where}
                  AND dc._fivetran_deleted = false
                GROUP BY COALESCE(dc.industry, 'Unknown')
                ORDER BY 2 DESC
            """
            cursor.execute(industry_query)
            industry_rows = cursor.fetchall()
            
            # Breakdown by account type (from dim_customers.account_type)
            logger.info("Fetching account type breakdown...")
            account_type_query = f"""
                SELECT
                    COALESCE(dc.account_type, 'Unknown') as account_type,
                    COALESCE(ROUND(SUM(c.ARR_EUR), 0), 0) as arr_eur,
                    COUNT(DISTINCT c.account_id) as customer_count
                FROM {FCT_CONTRACT_TABLE} c
                LEFT JOIN {DIM_CUSTOMER_TABLE} dc ON c.account_id = dc.account_id
                WHERE {base_where}
                  AND dc._fivetran_deleted = false
                GROUP BY COALESCE(dc.account_type, 'Unknown')
                ORDER BY 3 DESC
            """
            cursor.execute(account_type_query)
            account_type_rows = cursor.fetchall()
            
            # Build WHERE clause for customer table — same filters as top section
            where_conditions = [
                "f.account IS NOT NULL",
                "f.account != ''",
                "f.RENEWAL_NOT_YET_CONTRACTED = 'Y'",
                "f.revenue_type NOT IN ('Services', 'Perpetual')",
                "(f.churn_expected_occurred IS NULL OR f.churn_expected_occurred = 'nan')",
                "f.rev_rec_end_date > CURRENT_DATE()",
                f"f.rev_rec_end_date <= DATE_ADD(CURRENT_DATE(), {int(renewal_period)})",
            ]
            params = []
            
            if revenue_type:
                where_conditions.append("f.revenue_type = ?")
                params.append(revenue_type)
            if region:
                where_conditions.append("f.region = ?")
                params.append(region)
            if search:
                where_conditions.append("LOWER(f.account) LIKE LOWER(?)")
                params.append(f"%{search}%")
            if account_type:
                safe_type = account_type.replace("'", "''")
                where_conditions.append(f"dc.account_type = '{safe_type}'")
            
            where_clause = " AND ".join(where_conditions)
            
            # Get total customer count first
            logger.info("Counting customers...")
            if account_type:
                count_query = f"""
                    SELECT COUNT(DISTINCT f.account) 
                    FROM {FCT_CONTRACT_TABLE} f
                    LEFT JOIN {DIM_CUSTOMER_TABLE} dc ON f.ACCOUNT_ID = dc.account_id
                    WHERE {where_clause}
                      AND dc._fivetran_deleted = false
                """
            else:
                count_query = f"""
                    SELECT COUNT(DISTINCT f.account) 
                    FROM {FCT_CONTRACT_TABLE} f
                    WHERE {where_clause}
                """
            cursor.execute(count_query, params)
            total_customers = cursor.fetchone()[0]
            logger.info(f"Total customers: {total_customers}")
            
            # Get customer-level data with pagination (join with dim_customers for industry)
            logger.info("Fetching customer data...")
            offset = (page - 1) * page_size
            customer_query = f"""
                SELECT 
                    f.account,
                    MAX(f.ACCOUNT_ID) as account_id,
                    MAX(dc.region) as region,
                    MAX(dc.industry) as industry,
                    COALESCE(ROUND(SUM(f.ARR_EUR), 0), 0) as total_arr_eur,
                    COALESCE(ROUND(SUM(f.BOOKING_TCV_CAD), 0), 0) as total_tcv_cad,
                    COALESCE(ROUND(SUM(f.ARR_CONTRACT_CURRENCY), 0), 0) as total_acv_native,
                    COUNT(DISTINCT f.contract_group) as contract_count,
                    MAX(f.currency) as primary_currency
                FROM {FCT_CONTRACT_TABLE} f
                LEFT JOIN {DIM_CUSTOMER_TABLE} dc ON f.ACCOUNT_ID = dc.account_id
                WHERE {where_clause}
                  AND dc._fivetran_deleted = false
                GROUP BY f.account
                ORDER BY 5 DESC
                LIMIT {page_size} OFFSET {offset}
            """
            
            cursor.execute(customer_query, params)
            customer_rows = cursor.fetchall()
            
            logger.info(f"Fetched {len(customer_rows)} customers")
            
            # Get contract groups for each customer
            account_names = [row[0] for row in customer_rows if row[0]]
            contract_groups_by_account = {}
            
            if account_names:
                # Build placeholders for IN clause
                # Filter by renewal_not_yet_contracted = 'Y'
                placeholders = ", ".join(["?" for _ in account_names])
                contract_groups_query = f"""
                    SELECT 
                        account,
                        contract_group,
                        revenue_type,
                        currency,
                        SUM(COALESCE(arr_contract_currency, 0)) as arr_native,
                        SUM(COALESCE(arr_cad, 0)) as arr_cad,
                        SUM(COALESCE(booking_tcv_allocated_contract_currency, 0)) as tcv_native,
                        SUM(COALESCE(booking_tcv_cad, 0)) as tcv_cad,
                        MIN(`start`) as contract_start,
                        MAX(`end`) as contract_end,
                        COUNT(*) as pob_count
                    FROM {FCT_CONTRACT_TABLE}
                    WHERE account IN ({placeholders})
                    AND renewal_not_yet_contracted = 'Y'
                    AND revenue_type NOT IN ('Services', 'Perpetual')
                    AND (churn_expected_occurred IS NULL OR churn_expected_occurred = 'nan')
                    AND rev_rec_end_date > CURRENT_DATE()
                    AND rev_rec_end_date <= DATE_ADD(CURRENT_DATE(), {int(renewal_period)})
                    GROUP BY account, contract_group, revenue_type, currency
                    ORDER BY account, 6 DESC
                """
                cursor.execute(contract_groups_query, account_names)
                cg_rows = cursor.fetchall()
                
                # Group by account
                for cg_row in cg_rows:
                    acct = cg_row[0]
                    if acct not in contract_groups_by_account:
                        contract_groups_by_account[acct] = []
                    contract_groups_by_account[acct].append(cg_row)
                
                logger.info(f"Fetched contract groups for {len(contract_groups_by_account)} accounts")
            
            cursor.close()
            
            # Build response
            from ..models.schemas import (
                ARRAnalysisResponse, ARRPortfolioSummary, ARRCustomerSummary,
                ARRByRevenueType, ARRByRegion, ARRByIndustry, ARRByAccountType, ARRContractGroup
            )
            
            by_revenue_type = [
                ARRByRevenueType(
                    revenue_type=row[0] or "Unknown",
                    arr_cad=float(row[1]) if row[1] else 0.0,
                    tcv_cad=float(row[2]) if row[2] else 0.0,
                    contract_count=int(row[3]) if row[3] else 0,
                    customer_count=int(row[4]) if row[4] else 0,
                )
                for row in revenue_type_rows
            ]
            
            by_region = [
                ARRByRegion(
                    region=row[0] or "Unknown",
                    arr_cad=float(row[1]) if row[1] else 0.0,
                    tcv_cad=float(row[2]) if row[2] else 0.0,
                    customer_count=int(row[3]) if row[3] else 0,
                )
                for row in region_rows
            ]
            
            by_industry = [
                ARRByIndustry(
                    industry=row[0] or "Unknown",
                    arr_cad=float(row[1]) if row[1] else 0.0,
                    tcv_cad=float(row[2]) if row[2] else 0.0,
                    customer_count=int(row[3]) if row[3] else 0,
                )
                for row in industry_rows
            ]
            
            by_account_type = [
                ARRByAccountType(
                    account_type=row[0] or "Unknown",
                    arr_cad=float(row[1]) if row[1] else 0.0,
                    customer_count=int(row[2]) if row[2] else 0,
                )
                for row in account_type_rows
            ]
            
            summary = ARRPortfolioSummary(
                total_arr_cad=float(summary_row[2]) if summary_row[2] else 0.0,
                total_tcv_cad=float(summary_row[3]) if summary_row[3] else 0.0,
                total_acv_cad=float(summary_row[4]) if summary_row[4] else 0.0,
                total_contracts=int(summary_row[0]) if summary_row[0] else 0,
                total_customers=int(summary_row[1]) if summary_row[1] else 0,
                renewals_next_90_days_arr=float(renewals_row[1]) if renewals_row and renewals_row[1] else 0.0,
                renewals_next_90_days_count=int(renewals_row[0]) if renewals_row and renewals_row[0] else 0,
                by_revenue_type=by_revenue_type,
                by_region=by_region,
                by_industry=by_industry,
                by_account_type=by_account_type,
            )
            
            # Customer rows: account, account_id, region, industry, arr, tcv, acv, contract_count, currency
            customers = []
            for row in customer_rows:
                account_name = row[0] or "Unknown"
                
                # Build contract groups for this customer
                cg_list = []
                if account_name in contract_groups_by_account:
                    for cg_row in contract_groups_by_account[account_name]:
                        # cg_row: account, contract_group, revenue_type, currency, arr_native, arr_cad, tcv_native, tcv_cad, start, end, pob_count
                        cg_list.append(ARRContractGroup(
                            contract_group=cg_row[1] or "Unknown",
                            revenue_type=cg_row[2] or "Unknown",
                            currency=cg_row[3] or "CAD",
                            arr_native=float(cg_row[4]) if cg_row[4] else 0.0,
                            arr_cad=float(cg_row[5]) if cg_row[5] else 0.0,
                            tcv_native=float(cg_row[6]) if cg_row[6] else 0.0,
                            tcv_cad=float(cg_row[7]) if cg_row[7] else 0.0,
                            contract_start=str(cg_row[8]) if cg_row[8] else None,
                            contract_end=str(cg_row[9]) if cg_row[9] else None,
                            performance_obligation_count=int(cg_row[10]) if cg_row[10] else 1,
                        ))
                
                customers.append(ARRCustomerSummary(
                    account=account_name,
                    account_id=row[1],
                    region=row[2] or "Unknown",
                    industry=row[3],
                    total_arr_cad=float(row[4]) if row[4] else 0.0,
                    total_tcv_cad=float(row[5]) if row[5] else 0.0,
                    total_acv_cad=float(row[6]) if row[6] else 0.0,
                    contract_count=int(row[7]) if row[7] else 0,
                    primary_currency=row[8] or "CAD",
                    contract_groups=cg_list,
                ))
            
            return ARRAnalysisResponse(
                summary=summary,
                customers=customers,
                total_customers=int(total_customers) if total_customers else 0,
                page=page,
                page_size=page_size,
            )

    def get_arr_customer_detail(self, account: str) -> dict:
        """Get detailed ARR data for a specific customer."""
        logger.info(f"get_arr_customer_detail called for account: {account}")
        
        FCT_CONTRACT_TABLE = FCT_CONTRACTS_TABLE
        
        with self.get_connection() as conn:
            if conn is None:
                logger.error("Connection is None - cannot fetch customer ARR detail")
                raise Exception("Database connection failed")

            cursor = conn.cursor()
            
            query = f"""
                SELECT 
                    contract_group,
                    revenue_type,
                    currency,
                    SUM(COALESCE(arr_contract_currency, 0)) as arr_native,
                    SUM(COALESCE(arr_cad, 0)) as arr_cad,
                    SUM(COALESCE(booking_tcv_allocated_contract_currency, 0)) as tcv_native,
                    SUM(COALESCE(booking_tcv_cad, 0)) as tcv_cad,
                    SUM(COALESCE(acv_contract_currency, 0)) as acv_native,
                    SUM(COALESCE(acv_cad, 0)) as acv_cad,
                    MIN(`start`) as contract_start,
                    MAX(`end`) as contract_end,
                    AVG(COALESCE(TRY_CAST(of_years_months AS DOUBLE), 0)) / 12 as contract_years,
                    COUNT(*) as pob_count
                FROM {FCT_CONTRACT_TABLE}
                WHERE account = ?
                GROUP BY contract_group, revenue_type, currency
                ORDER BY 5 DESC
            """
            
            cursor.execute(query, [account])
            rows = cursor.fetchall()
            cursor.close()
            
            from ..models.schemas import ARRContractGroup, ARRCustomerSummary
            
            contract_groups = []
            total_arr_cad = 0.0
            total_tcv_cad = 0.0
            total_acv_cad = 0.0
            
            for row in rows:
                cg = ARRContractGroup(
                    contract_group=row[0] or "Unknown",
                    revenue_type=row[1] or "Unknown",
                    currency=row[2] or "CAD",
                    arr_native=float(row[3]) if row[3] else 0.0,
                    arr_cad=float(row[4]) if row[4] else 0.0,
                    tcv_native=float(row[5]) if row[5] else 0.0,
                    tcv_cad=float(row[6]) if row[6] else 0.0,
                    acv_native=float(row[7]) if row[7] else 0.0,
                    acv_cad=float(row[8]) if row[8] else 0.0,
                    contract_start=str(row[9]) if row[9] else None,
                    contract_end=str(row[10]) if row[10] else None,
                    contract_years=float(row[11]) if row[11] else 0.0,
                    performance_obligation_count=int(row[12]) if row[12] else 1,
                )
                contract_groups.append(cg)
                total_arr_cad += cg.arr_cad
                total_tcv_cad += cg.tcv_cad
                total_acv_cad += cg.acv_cad
            
            return ARRCustomerSummary(
                account=account,
                region=contract_groups[0].currency if contract_groups else "Unknown",
                total_arr_cad=total_arr_cad,
                total_tcv_cad=total_tcv_cad,
                total_acv_cad=total_acv_cad,
                contract_count=len(contract_groups),
                contract_groups=contract_groups,
                primary_currency=contract_groups[0].currency if contract_groups else "CAD",
            )

    # ==================== ARR Mock Data Methods ====================

    def _get_mock_arr_analysis(
        self,
        page: int,
        page_size: int,
        revenue_type: Optional[str],
        region: Optional[str],
        search: Optional[str],
    ) -> dict:
        """Return mock ARR analysis data."""
        from ..models.schemas import (
            ARRAnalysisResponse, ARRPortfolioSummary, ARRCustomerSummary,
            ARRByRevenueType, ARRByRegion, ARRContractGroup
        )
        
        by_revenue_type = [
            ARRByRevenueType(revenue_type="SaaS", arr_cad=85200000, tcv_cad=245000000, contract_count=89, customer_count=45),
            ARRByRevenueType(revenue_type="Services", arr_cad=32500000, tcv_cad=98000000, contract_count=56, customer_count=38),
            ARRByRevenueType(revenue_type="eSMA", arr_cad=18300000, tcv_cad=54000000, contract_count=42, customer_count=28),
            ARRByRevenueType(revenue_type="Perpetual", arr_cad=8100000, tcv_cad=8100000, contract_count=15, customer_count=12),
            ARRByRevenueType(revenue_type="Accelerate", arr_cad=2070000, tcv_cad=6200000, contract_count=8, customer_count=6),
        ]
        
        by_region = [
            ARRByRegion(region="AMER", arr_cad=98500000, tcv_cad=295000000, customer_count=52),
            ARRByRegion(region="Europe", arr_cad=38200000, tcv_cad=112000000, customer_count=35),
            ARRByRegion(region="APAC", arr_cad=9500000, tcv_cad=28000000, customer_count=12),
        ]
        
        summary = ARRPortfolioSummary(
            total_arr_cad=146200000,
            total_tcv_cad=435000000,
            total_acv_cad=52800000,
            total_contracts=210,
            total_customers=99,
            renewals_next_90_days_arr=24500000,
            renewals_next_90_days_count=22,
            by_revenue_type=by_revenue_type,
            by_region=by_region,
        )
        
        # Mock customers
        mock_customers = [
            ARRCustomerSummary(
                account="Duke Energy",
                account_id="0016000000NU52GAAT",
                region="AMER",
                total_arr_cad=9120000,
                total_tcv_cad=25800000,
                total_acv_cad=4200000,
                contract_count=4,
                primary_currency="USD",
                contract_groups=[
                    ARRContractGroup(contract_group="DukeDX2", revenue_type="SaaS", currency="USD", arr_native=5740000, arr_cad=7800000, tcv_native=9098892, tcv_cad=12500000, contract_start="2020-12-11", contract_end="2025-12-10", contract_years=5),
                    ARRContractGroup(contract_group="DukeDX2", revenue_type="Services", currency="USD", arr_native=0, arr_cad=0, tcv_native=2804440, tcv_cad=3800000, contract_start="2020-11-01", contract_end="2022-12-09", contract_years=2),
                ]
            ),
            ARRCustomerSummary(
                account="AES",
                account_id="0016000000UoZ5GAAV",
                region="AMER",
                total_arr_cad=8307500,
                total_tcv_cad=18500000,
                total_acv_cad=3800000,
                contract_count=6,
                primary_currency="USD",
            ),
            ARRCustomerSummary(
                account="50Hertz (ELIA)",
                account_id="0016000001BoZbHAAV",
                region="Europe",
                total_arr_cad=4850000,
                total_tcv_cad=8900000,
                total_acv_cad=1800000,
                contract_count=5,
                primary_currency="EUR",
            ),
            ARRCustomerSummary(
                account="American Electric Power - Transmission (AEP)",
                account_id="001f200001dndE8AAI",
                region="AMER",
                total_arr_cad=4100000,
                total_tcv_cad=9200000,
                total_acv_cad=2100000,
                contract_count=3,
                primary_currency="USD",
            ),
            ARRCustomerSummary(
                account="ELIA",
                account_id="00160000017d0x5AAA",
                region="Europe",
                total_arr_cad=3800000,
                total_tcv_cad=7500000,
                total_acv_cad=1500000,
                contract_count=4,
                primary_currency="EUR",
            ),
            ARRCustomerSummary(
                account="Affinity Water",
                account_id="0016000001Kw80gAAB",
                region="Europe",
                total_arr_cad=2950000,
                total_tcv_cad=5200000,
                total_acv_cad=980000,
                contract_count=3,
                primary_currency="GBP",
            ),
            ARRCustomerSummary(
                account="BPA - Generation",
                account_id="0016000000JO214AAD",
                region="AMER",
                total_arr_cad=2800000,
                total_tcv_cad=6800000,
                total_acv_cad=1200000,
                contract_count=2,
                primary_currency="USD",
            ),
            ARRCustomerSummary(
                account="BC Hydro",
                account_id="acc-8",
                region="AMER",
                total_arr_cad=2500000,
                total_tcv_cad=5100000,
                total_acv_cad=890000,
                contract_count=2,
                primary_currency="CAD",
            ),
            ARRCustomerSummary(
                account="Hydro One",
                account_id="acc-9",
                region="AMER",
                total_arr_cad=2200000,
                total_tcv_cad=4800000,
                total_acv_cad=780000,
                contract_count=2,
                primary_currency="CAD",
            ),
            ARRCustomerSummary(
                account="Ontario Power Generation",
                account_id="acc-10",
                region="AMER",
                total_arr_cad=1950000,
                total_tcv_cad=4200000,
                total_acv_cad=720000,
                contract_count=2,
                primary_currency="CAD",
            ),
        ]
        
        # Apply filters
        customers = mock_customers
        if revenue_type:
            # In real data, would filter by revenue type in contract groups
            pass
        if region:
            customers = [c for c in customers if c.region == region]
        if search:
            customers = [c for c in customers if search.lower() in c.account.lower()]
        
        total_customers = len(customers)
        start = (page - 1) * page_size
        end = start + page_size
        
        return ARRAnalysisResponse(
            summary=summary,
            customers=customers[start:end],
            total_customers=total_customers,
            page=page,
            page_size=page_size,
        )

    def _get_mock_arr_customer_detail(self, account: str) -> dict:
        """Return mock ARR detail for a specific customer."""
        from ..models.schemas import ARRCustomerSummary, ARRContractGroup, ARRRevenueMonth
        
        # Generate some mock revenue schedule
        revenue_schedule = []
        for year in range(2024, 2027):
            for month in range(1, 13):
                revenue_schedule.append(ARRRevenueMonth(
                    month=f"{year}-{month:02d}",
                    native_currency=75000 + (month * 1000),
                    cad=102000 + (month * 1350),
                ))
        
        return ARRCustomerSummary(
            account=account,
            account_id="mock-id",
            region="AMER",
            total_arr_cad=5740000,
            total_tcv_cad=12500000,
            total_acv_cad=2100000,
            contract_count=3,
            primary_currency="USD",
            contract_groups=[
                ARRContractGroup(
                    contract_group=f"{account} SaaS",
                    revenue_type="SaaS",
                    currency="USD",
                    arr_native=4200000,
                    arr_cad=5740000,
                    tcv_native=9100000,
                    tcv_cad=12500000,
                    contract_start="2020-12-11",
                    contract_end="2025-12-10",
                    contract_years=5.0,
                    performance_obligation_count=1,
                    revenue_schedule=revenue_schedule[:24],
                ),
                ARRContractGroup(
                    contract_group=f"{account} Services",
                    revenue_type="Services",
                    currency="USD",
                    arr_native=0,
                    arr_cad=0,
                    tcv_native=2800000,
                    tcv_cad=3800000,
                    contract_start="2020-11-01",
                    contract_end="2022-12-09",
                    contract_years=2.1,
                    performance_obligation_count=1,
                ),
            ],
        )


# Dependency injection
def get_databricks_service() -> DatabricksService:
    """Get Databricks service instance."""
    return DatabricksService(get_settings())
