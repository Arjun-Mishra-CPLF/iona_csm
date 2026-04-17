"""Pydantic schemas for API request/response models."""

from datetime import date, datetime
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field


class HealthScore(str, Enum):
    """Account health score levels."""
    CRITICAL = "Critical"
    AT_RISK = "At Risk"
    GOOD = "Good"


class HealthScoreFactor(BaseModel):
    """Individual factor contributing to health score."""
    name: str  # e.g., "Renewal", "Engagement", "Support", "Activity"
    points: int  # Points added to risk score
    max_points: int  # Maximum possible points for this factor
    detail: str  # Human-readable explanation
    icon: str = ""  # Emoji icon for display


class HealthScoreDetail(BaseModel):
    """Detailed health score breakdown with contributing factors."""
    score: int  # 0-100 risk score
    category: HealthScore  # Good, At Risk, Critical
    factors: List[HealthScoreFactor] = []
    has_pendo: bool = False  # Whether Pendo data is available
    has_freshdesk: bool = False  # Whether Freshdesk data is available
    has_gong: bool = False  # Whether Gong call data is available
    scoring_version: str = "rule-based-v1.0"


class HealthScoreHistoryPoint(BaseModel):
    """Single data point in health score history."""
    score_date: date
    health_score: int
    health_category: str


class HealthScoreHistoryResponse(BaseModel):
    """Health score history for trend visualization."""
    account_id: str
    account_name: str
    history: List[HealthScoreHistoryPoint] = []


class RenewalContractLine(BaseModel):
    """One open renewal line from fct_contracts (materiality context)."""
    revenue_type: str
    arr_eur: float = 0.0
    renewal_date: Optional[date] = None
    renewal_days: Optional[int] = None
    contract_group: Optional[str] = None


class RenewalHealthInsightResponse(BaseModel):
    """Renewal-weighted health context + optional LLM narrative for CSMs."""
    account_id: str
    account_name: str
    contracts: List[RenewalContractLine] = []
    nearest_renewal_days: Optional[int] = None
    base_renewal_deduction: int = 0
    adjusted_renewal_deduction: int = 0
    materiality_weight: float = 1.0
    near_term_arr_eur: float = 0.0
    nearest_line_arr_eur: float = 0.0
    share_of_near_term: float = 0.0
    deterministic_explanation: str = ""
    llm_narrative: Optional[str] = None
    scoring_version: str = "renewal-materiality-v1"


class AccountMovement(BaseModel):
    """An account that changed health category between two days."""
    account_id: str
    account_name: str
    prev_score: int
    curr_score: int
    prev_category: str
    curr_category: str
    explanation: str = ""
    recent_scores: List[int] = []


class HealthChangeDay(BaseModel):
    """Health distribution snapshot for one day with movements from prior day."""
    date: date
    prev_date: Optional[date] = None
    good: int = 0
    at_risk: int = 0
    critical: int = 0
    improved: List[AccountMovement] = []
    worsened: List[AccountMovement] = []


class HealthChangesResponse(BaseModel):
    """Health distribution history with daily account movements."""
    days: List[HealthChangeDay] = []
    today_delta: Optional[dict] = None


class WeeklySummaryItem(BaseModel):
    """Single week's activity summary."""
    account_id: str
    account_name: str
    week_start: date
    week_end: date
    narrative: str
    gong_summary: Optional[str] = None
    generated_at: Optional[datetime] = None


class WeeklySummaryResponse(BaseModel):
    """Paginated weekly summaries for an account."""
    account_id: str
    account_name: str
    weeks: List[WeeklySummaryItem] = []
    total_weeks: int = 0


class AccountStatus(str, Enum):
    """Account status values."""
    NEEDS_ATTENTION = "Needs Attention"
    IN_PROGRESS = "In Progress"
    STABLE = "Stable"


class Signal(BaseModel):
    """A signal/alert for an account."""
    id: str
    type: str
    description: str
    severity: str
    detected_at: datetime


class RenewalInfo(BaseModel):
    """Renewal date info for a specific contract/revenue type."""
    revenue_type: str
    renewal_date: Optional[date] = None
    renewal_days: Optional[int] = None
    contract_group: Optional[str] = None
    arr_cad: Optional[float] = None


class Account(BaseModel):
    """Account summary for list view."""
    id: str
    name: str
    health: HealthScore
    health_score_detail: Optional[HealthScoreDetail] = None  # Detailed score breakdown
    primary_signal: Optional[str] = None
    primary_signal_type: Optional[str] = None
    renewal_days: int
    renewal_date: date
    owner_id: str
    owner_name: str
    owner_avatar: Optional[str] = None
    status: AccountStatus
    csm_name: Optional[str] = None
    ae_name: Optional[str] = None
    search_score: Optional[int] = None  # Present when search is applied, higher = better match
    parent_id: Optional[str] = None  # Parent account ID for grouping
    parent_name: Optional[str] = None  # Parent account name for display
    renewals: List[RenewalInfo] = []  # Per-revenue-type renewal dates from fct_contracts


class AccountDetail(Account):
    """Detailed account information."""
    arr: float = Field(description="Annual Recurring Revenue")
    mrr: float = Field(description="Monthly Recurring Revenue")
    contract_start: date
    contract_end: date
    industry: Optional[str] = None
    employees: Optional[int] = None
    signals: List[Signal] = []
    notes: Optional[str] = None


class AccountListResponse(BaseModel):
    """Paginated account list response."""
    accounts: List[Account]
    total: int
    page: int
    page_size: int
    total_pages: int
    at_risk_count: int = 0  # Count of accounts with health score < 70


class HealthDistribution(BaseModel):
    """Health score distribution for portfolio summary."""
    good: int = 0
    at_risk: int = 0
    critical: int = 0


class MetricsSummary(BaseModel):
    """Dashboard KPI metrics."""
    # Portfolio Summary metrics
    total_accounts: int
    total_arr: float = 0.0
    renewals_arr: float = 0.0
    renewals_count: int = 0
    health_distribution: HealthDistribution = HealthDistribution()

    # Action KPIs
    at_risk_count: int
    renewals_90_days: int
    usage_decline_count: int
    expansion_signals: int

    # Day-over-day deltas (vs yesterday)
    at_risk_delta: Optional[int] = None
    usage_decline_delta: Optional[int] = None


class TaskCreate(BaseModel):
    """Request body for creating a task."""
    account_id: str
    title: str
    description: Optional[str] = None
    due_date: Optional[date] = None
    priority: str = "medium"


class Task(BaseModel):
    """Task associated with an account."""
    id: str
    account_id: str
    title: str
    description: Optional[str] = None
    due_date: Optional[date] = None
    priority: str
    status: str
    created_at: datetime
    created_by: str


# ============================================
# Account Detail Page Schemas
# ============================================

class ContributingFactor(BaseModel):
    """Factor contributing to health score."""
    name: str
    impact: str  # "positive", "negative", "neutral"
    description: str


class HealthBreakdown(BaseModel):
    """Detailed health score breakdown."""
    overall_score: int = Field(ge=0, le=100)
    usage_score: int = Field(ge=0, le=100)
    support_score: int = Field(ge=0, le=100)
    engagement_score: int = Field(ge=0, le=100)
    renewal_score: int = Field(ge=0, le=100)
    trend: str  # "improving", "stable", "declining"
    contributing_factors: List[ContributingFactor] = []


class TicketTheme(BaseModel):
    """Theme extracted from support tickets."""
    name: str
    count: int
    severity: str  # "critical", "high", "medium", "low"


class SupportTicket(BaseModel):
    """Individual support ticket with conversation insights."""
    id: str
    title: str
    severity: str  # "critical", "high", "medium", "low"
    status: str  # "open", "in_progress", "resolved"
    created_at: datetime
    updated_at: datetime
    # Conversation summary fields
    summary: Optional[str] = None
    net_sentiment: int = 0  # -ve = negative, 0 = neutral, +ve = positive
    total_messages: int = 0
    customer_messages: int = 0
    support_messages: int = 0
    positive_messages: int = 0
    negative_messages: int = 0
    neutral_messages: int = 0
    last_message_at: Optional[datetime] = None
    ticket_type: Optional[str] = None  # Help Request, Product Defect, etc.
    account_name: Optional[str] = None  # Navigate account (e.g. CSM-wide ticket lists)


class ResolutionBucket(BaseModel):
    """Resolution time distribution bucket."""
    label: str  # "< 1 day", "1-3 days", etc.
    min_days: float
    max_days: float
    count: int
    percentage: float


class ResolutionStats(BaseModel):
    """Resolution time statistics with distribution."""
    mean_days: float = 0.0
    median_days: float = 0.0
    p25_days: float = 0.0  # 25th percentile
    p75_days: float = 0.0  # 75th percentile
    p90_days: float = 0.0  # 90th percentile
    min_days: float = 0.0
    max_days: float = 0.0
    total_resolved: int = 0
    distribution: List[ResolutionBucket] = []


class SupportAnalysis(BaseModel):
    """Support ticket analysis for an account."""
    open_tickets: int
    critical_tickets: int
    high_tickets: int
    avg_resolution_hours: float
    ticket_trend: str  # "increasing", "stable", "decreasing"
    themes: List[TicketTheme] = []
    recent_tickets: List[SupportTicket] = []
    # Aggregate sentiment metrics
    avg_sentiment: float = 0.0  # Average net sentiment across all tickets
    total_tickets: int = 0
    total_customer_messages: int = 0
    total_support_messages: int = 0
    positive_ticket_count: int = 0  # Tickets with positive sentiment
    negative_ticket_count: int = 0  # Tickets with negative sentiment
    neutral_ticket_count: int = 0   # Tickets with neutral sentiment
    # Resolution time distribution
    resolution_stats: Optional[ResolutionStats] = None


class SupportTicketsResponse(BaseModel):
    """Paginated support tickets response."""
    tickets: List[SupportTicket]
    total: int
    page: int
    page_size: int
    total_pages: int


class CSMSupportTicketsResponse(BaseModel):
    """Recent support tickets across all accounts owned by a CSM."""
    tickets: List[SupportTicket]


class UsageTrend(BaseModel):
    """Usage data point."""
    date: date
    value: float
    metric: str


class FeatureAdoption(BaseModel):
    """Feature adoption status."""
    name: str
    adoption_percent: float
    trend: str


class PendoDailyMetric(BaseModel):
    """A single day of Pendo account-level metrics."""
    date_day: str
    active_visitors: int = 0
    sum_minutes: float = 0.0
    sum_events: int = 0
    pages_viewed: int = 0
    features_clicked: int = 0
    page_viewing_visitors: int = 0
    feature_clicking_visitors: int = 0
    avg_minutes_per_user: float = 0.0


class PendoFeatureDaily(BaseModel):
    """Daily metrics for a single Pendo feature."""
    date_day: str
    feature_id: str = ""
    feature_name: str = ""
    count_clicks: int = 0
    sum_minutes: float = 0.0
    unique_visitors: int = 0


class PendoVisitorDaily(BaseModel):
    """Daily metrics for visitors."""
    date_day: str
    unique_visitors: int = 0
    sum_minutes: float = 0.0
    sum_events: int = 0
    returning_visitors: int = 0


class PendoPageDaily(BaseModel):
    """Daily metrics for pages."""
    date_day: str
    page_id: str = ""
    page_name: str = ""
    count_views: int = 0
    sum_minutes: float = 0.0
    unique_visitors: int = 0


class PendoUsageSummary(BaseModel):
    """Pendo usage summary for current vs previous period."""
    current_active_visitors: float = 0
    previous_active_visitors: float = 0
    visitors_change_pct: float = 0
    current_minutes: float = 0
    previous_minutes: float = 0
    minutes_change_pct: float = 0
    current_events: float = 0
    previous_events: float = 0
    events_change_pct: float = 0
    total_data_days: int = 0
    pendo_account_ids: List[str] = []


class PendoTabData(BaseModel):
    """Data for a single Pendo tab (features, visitors, pages)."""
    daily: List[dict] = []
    top_items: List[dict] = []
    total_data_days: int = 0


class UsageAnalysis(BaseModel):
    """Product usage analysis — powered by Pendo daily metrics."""
    current_usage: float = 0
    previous_usage: float = 0
    change_percent: float = 0
    trend: str = "stable"
    usage_history: List[UsageTrend] = []
    features_adopted: List[FeatureAdoption] = []
    pendo_summary: Optional[PendoUsageSummary] = None
    pendo_daily: List[PendoDailyMetric] = []
    pendo_features: Optional[PendoTabData] = None
    pendo_visitors: Optional[PendoTabData] = None
    pendo_pages: Optional[PendoTabData] = None
    has_pendo_data: bool = False


class ProductWhitespace(BaseModel):
    """License utilization for a product."""
    name: str
    licensed: int
    used: int
    utilization_percent: float


class ExpansionOpportunity(BaseModel):
    """Expansion/upsell opportunity."""
    product: str
    potential_value: float
    reason: str
    confidence: str  # "high", "medium", "low"


class WhitespaceAnalysis(BaseModel):
    """License vs usage (whitespace) analysis."""
    total_licenses: int
    used_licenses: int
    utilization_percent: float
    products: List[ProductWhitespace] = []
    expansion_opportunities: List[ExpansionOpportunity] = []


class ContractEvent(BaseModel):
    """Historical contract event."""
    date: date
    type: str  # "new", "renewal", "expansion", "contraction", "amendment"
    description: str
    value_change: float


class ContractGroup(BaseModel):
    """Individual contract group within an account."""
    contract_group: str
    revenue_type: str
    currency: str = "CAD"
    arr: float = 0.0
    arr_cad: float = 0.0
    tcv: float = 0.0
    tcv_cad: float = 0.0
    contract_start: Optional[str] = None
    contract_end: Optional[str] = None
    days_until_end: Optional[int] = None
    renewal_not_yet_contracted: bool = False


class LuminanceDocument(BaseModel):
    """A contract document stored in Luminance."""
    document_id: str
    title: str
    url: str
    state: str = "import_complete"
    document_type: Optional[str] = None


class ContractContext(BaseModel):
    """Contract and renewal context — supports multiple contract groups."""
    # Summary across all contracts
    total_arr_cad: float = 0.0
    total_tcv_cad: float = 0.0
    nearest_renewal_date: Optional[date] = None
    days_until_renewal: int = 0
    contract_count: int = 0
    revenue_types: List[str] = []
    # Individual contract groups
    contracts: List[ContractGroup] = []
    # Luminance contract documents
    luminance_documents: List[LuminanceDocument] = []
    # Legacy fields for backward compat
    contract_type: str = "N/A"
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    renewal_date: Optional[date] = None
    contract_value: float = 0.0
    arr: float = 0.0
    mrr: float = 0.0
    payment_terms: str = "N/A"
    auto_renewal: bool = False
    contract_history: List[ContractEvent] = []


class ChangeEvent(BaseModel):
    """Change since last touch."""
    id: str
    date: datetime
    type: str  # "meeting", "email", "call", "support", "usage", "contract", "stakeholder"
    source: str
    title: str
    description: str
    importance: str  # "high", "medium", "low"


class RiskFactor(BaseModel):
    """Individual risk factor."""
    name: str
    impact: int = Field(ge=0, le=100)
    description: str
    trend: str  # "worsening", "stable", "improving"


class RecommendedAction(BaseModel):
    """Recommended action to address risk."""
    title: str
    description: str
    priority: str  # "urgent", "high", "medium", "low"
    action_type: str


class RiskAssessment(BaseModel):
    """Churn and renewal risk assessment."""
    churn_risk_score: int = Field(ge=0, le=100)
    renewal_risk_score: int = Field(ge=0, le=100)
    risk_level: str  # "critical", "high", "medium", "low"
    risk_factors: List[RiskFactor] = []
    recommended_actions: List[RecommendedAction] = []


class SentimentSource(BaseModel):
    """Sentiment from a specific source."""
    type: str  # "email", "meeting", "support", "survey"
    sentiment: int  # -100 to 100
    count: int


class SentimentInteraction(BaseModel):
    """Individual sentiment interaction."""
    date: datetime
    type: str
    sentiment: int
    summary: str


class SentimentAnalysis(BaseModel):
    """Customer sentiment analysis."""
    overall_sentiment: int = Field(ge=-100, le=100)
    sentiment_label: str  # "positive", "neutral", "negative"
    trend: str  # "improving", "stable", "declining"
    sources: List[SentimentSource] = []
    recent_interactions: List[SentimentInteraction] = []


class BenchmarkMetric(BaseModel):
    """Single benchmark metric comparison."""
    name: str
    account_value: float
    peer_average: float
    peer_median: float
    percentile: int


class BenchmarkData(BaseModel):
    """Cross-customer benchmarking data."""
    peer_group: str
    metrics: List[BenchmarkMetric] = []


class ActionAlert(BaseModel):
    """Action-taking alert."""
    id: str
    type: str  # "renewal_risk", "churn_risk", "upsell", "support_escalation", etc.
    title: str
    description: str
    severity: str  # "critical", "high", "medium", "low"
    timestamp: datetime
    suggested_action: str
    action_url: Optional[str] = None
    is_read: bool = False


class HumanNote(BaseModel):
    """CSM-added note."""
    id: str
    author: str
    author_email: str
    content: str
    created_at: datetime
    updated_at: datetime
    tags: List[str] = []


class MeetingBrief(BaseModel):
    """AI-generated meeting brief."""
    generated_at: datetime
    snapshot_id: str
    summary: str
    key_points: List[str] = []
    talking_points: List[str] = []
    risks_to_address: List[str] = []
    opportunities: List[str] = []
    recent_activity_summary: str
    recommended_topics: List[str] = []


class ValueGoal(BaseModel):
    """Value realization goal."""
    id: str
    name: str
    target: float
    current: float
    unit: str
    status: str  # "on_track", "at_risk", "behind"
    due_date: date


class ValueRealization(BaseModel):
    """Value realization tracking."""
    goals: List[ValueGoal] = []
    overall_realization_percent: float
    time_to_value_days: int
    adoption_score: int


class SignalDetail(BaseModel):
    """Extended signal for detail page."""
    id: str
    type: str
    title: str
    description: str
    severity: str
    timestamp: datetime
    source: str
    is_read: bool = False


class ConfluenceImplementationResponse(BaseModel):
    """Confluence KB client implementation summary (kb_confluence_customer_context) for an account."""

    has_content: bool = False
    page_title: Optional[str] = None
    page_text: Optional[str] = None
    page_id: Optional[str] = None
    space_id: Optional[str] = None
    root_page_name: Optional[str] = None


# ============================================
# Gong Activity Schemas
# ============================================

class GongTrackerSignal(BaseModel):
    """A single Gong topic tracker with hit count across recent calls."""
    tracker_name: str
    call_count: int
    mention_count: int
    category: str  # "risk" | "engagement" | "general"


class GongCallSummary(BaseModel):
    """Summary of a single Gong call for display in the widget."""
    call_id: str
    title: str
    started_at: datetime
    duration_minutes: int
    brief_excerpt: Optional[str] = None  # first 200 chars of call_brief
    customer_attendees: List[str] = []
    csm_attendees: List[str] = []


class GongActivityAnalysis(BaseModel):
    """Gong engagement analysis for an account — cadence, topics, derived indicator."""
    # Cadence
    meetings_30d: int
    meetings_90d: int
    last_meeting_date: Optional[datetime] = None
    days_since_last_meeting: Optional[int] = None

    # Topic signals from tracker hits (last 90d)
    tracker_signals: List[GongTrackerSignal] = []
    risk_signal_calls: int = 0
    engagement_signal_calls: int = 0

    # Derived engagement indicator (no LLM)
    engagement_label: str  # "Risk signals present" | "Healthy engagement" | "Neutral"
    engagement_trend: str  # "improving" | "stable" | "declining"

    # 0–100 score for health score wiring
    engagement_score: int

    # Recent call list + key points from the most recent call
    recent_calls: List[GongCallSummary] = []
    latest_key_points: List[str] = []


class AccountFullDetail(BaseModel):
    """Complete account detail with all use case data."""
    # Basic account info
    account: AccountDetail
    
    # Use case data
    health_breakdown: HealthBreakdown
    support_analysis: SupportAnalysis
    usage_analysis: UsageAnalysis
    whitespace: WhitespaceAnalysis
    contract: ContractContext
    changes_since_last_touch: List[ChangeEvent] = []
    risk_assessment: RiskAssessment
    sentiment: SentimentAnalysis
    benchmark: BenchmarkData
    alerts: List[ActionAlert] = []
    signals: List[SignalDetail] = []
    notes: List[HumanNote] = []
    meeting_brief: MeetingBrief
    value_realization: ValueRealization
    gong_activity: Optional[GongActivityAnalysis] = None

    # Metadata
    last_updated: datetime
    last_touch_date: datetime


# ============================================
# CSM Management Schemas
# ============================================

class CSM(BaseModel):
    """Customer Success Manager."""
    id: str
    name: str
    email: Optional[str] = None
    status: str = "active"  # active, inactive, departed
    account_count: int = 0
    total_arr: float = 0
    at_risk_count: int = 0


class CSMListResponse(BaseModel):
    """List of CSMs response."""
    csms: List[CSM]
    total: int


class CSMStats(BaseModel):
    """CSM management dashboard stats."""
    active_csms: int
    avg_accounts_per_csm: float
    unassigned_accounts: int
    total_arr_managed: float


class AccountWithCSM(BaseModel):
    """Account with CSM assignment info."""
    id: str
    name: str
    account_type: Optional[str] = None
    csm_id: Optional[str] = None
    csm_name: Optional[str] = None
    arr: float = 0
    health: str = "Unknown"
    renewal_date: Optional[date] = None
    renewal_days: Optional[int] = None
    renewals: List[RenewalInfo] = []


class AccountWithCSMListResponse(BaseModel):
    """Paginated list of accounts with CSM info."""
    accounts: List[AccountWithCSM]
    total: int
    page: int
    page_size: int
    total_pages: int


# ============================================
# ARR Analysis Schemas (FCT_CONTRACT)
# ============================================

class ARRRevenueMonth(BaseModel):
    """Monthly revenue data point."""
    month: str  # Format: "2025-01"
    native_currency: float = 0.0
    cad: float = 0.0


class ARRContractGroup(BaseModel):
    """Contract group level summary."""
    contract_group: str
    revenue_type: str
    currency: str
    arr_native: float = 0.0
    arr_cad: float = 0.0
    tcv_native: float = 0.0
    tcv_cad: float = 0.0
    acv_native: float = 0.0
    acv_cad: float = 0.0
    contract_start: Optional[str] = None
    contract_end: Optional[str] = None
    contract_years: Optional[float] = None
    performance_obligation_count: int = 1
    revenue_schedule: List[ARRRevenueMonth] = []


class ARRCustomerSummary(BaseModel):
    """Customer-level ARR summary."""
    account: str
    account_id: Optional[str] = None
    region: str
    industry: Optional[str] = None
    total_arr_cad: float = 0.0
    total_arr_native: float = 0.0  # Sum when same currency, else CAD
    total_tcv_cad: float = 0.0
    total_acv_cad: float = 0.0
    contract_count: int = 0
    contract_groups: List[ARRContractGroup] = []
    primary_currency: str = "USD"
    renewal_next_90_days: float = 0.0


class ARRByRevenueType(BaseModel):
    """ARR breakdown by revenue type."""
    revenue_type: str
    arr_cad: float = 0.0
    tcv_cad: float = 0.0
    contract_count: int = 0
    customer_count: int = 0


class ARRByRegion(BaseModel):
    """ARR breakdown by region."""
    region: str
    arr_cad: float = 0.0
    tcv_cad: float = 0.0
    customer_count: int = 0


class ARRByIndustry(BaseModel):
    """ARR breakdown by industry."""
    industry: str
    arr_cad: float = 0.0
    tcv_cad: float = 0.0
    customer_count: int = 0


class ARRByAccountType(BaseModel):
    """ARR breakdown by account type."""
    account_type: str
    arr_cad: float = 0.0
    customer_count: int = 0


class ARRPortfolioSummary(BaseModel):
    """Overall ARR portfolio summary."""
    total_arr_cad: float = 0.0
    total_tcv_cad: float = 0.0
    total_acv_cad: float = 0.0
    total_contracts: int = 0
    total_customers: int = 0
    renewals_next_90_days_arr: float = 0.0
    renewals_next_90_days_count: int = 0
    by_revenue_type: List[ARRByRevenueType] = []
    by_region: List[ARRByRegion] = []
    by_industry: List[ARRByIndustry] = []
    by_account_type: List[ARRByAccountType] = []


class ARRAnalysisResponse(BaseModel):
    """Full ARR analysis page response."""
    summary: ARRPortfolioSummary
    customers: List[ARRCustomerSummary]
    total_customers: int
    page: int = 1
    page_size: int = 50


class ARRContractDetailResponse(BaseModel):
    """Detailed contract group response with full revenue schedule."""
    contract_group: str
    account: str
    revenue_type: str
    currency: str
    arr_native: float = 0.0
    arr_cad: float = 0.0
    tcv_native: float = 0.0
    tcv_cad: float = 0.0
    contract_start: Optional[str] = None
    contract_end: Optional[str] = None
    revenue_schedule: List[ARRRevenueMonth] = []


# ============================================
# Customer Growth Schemas
# ============================================

class MonthlyGrowthPoint(BaseModel):
    """A single month's customer growth data."""
    year: int
    month: int
    label: str  # "Jan 2025"
    new_count: int = 0
    churn_count: int = 0
    net_change: int = 0
    cumulative_total: int = 0


class CustomerEvent(BaseModel):
    """An individual customer acquisition or churn event."""
    account_id: str
    account_name: str
    date: str  # ISO date string
    event_type: str  # "new" or "churned"
    industry: Optional[str] = None
    region: Optional[str] = None


class CustomerGrowthSummary(BaseModel):
    """Summary stats for customer growth."""
    new_last_12m: int = 0
    new_prior_12m: int = 0
    yoy_growth_pct: float = 0.0
    total_customers_now: int = 0
    net_change_12m: int = 0
    churn_last_12m: int = 0
    avg_per_month: float = 0.0


class CustomerGrowthResponse(BaseModel):
    """Full customer growth page response."""
    summary: CustomerGrowthSummary
    monthly_series: List[MonthlyGrowthPoint] = []
    events: List[CustomerEvent] = []


class GroupMonthlyPoint(BaseModel):
    """A single month's data for one group (region/industry)."""
    year: int
    month: int
    label: str
    cumulative_total: int = 0


class GroupSeries(BaseModel):
    """Monthly series for a single group value."""
    group_name: str
    series: List[GroupMonthlyPoint] = []


class CustomerGrowthBreakdownResponse(BaseModel):
    """Breakdown of customer growth by a dimension (region/industry)."""
    dimension: str  # "industry" or "region"
    groups: List[GroupSeries] = []


# ============================================
# Notification / Email Schemas
# ============================================

class NotificationRecipient(BaseModel):
    """A user enrolled to receive bulk notification emails."""
    user_email: str
    user_name: str
    role: str  # 'csm_lead', 'manager', 'admin', 'executive'
    receive_all_weekly: bool = False
    receive_all_daily: bool = False
    active: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class NotificationRecipientCreate(BaseModel):
    user_email: str
    user_name: str
    role: str
    receive_all_weekly: bool = False
    receive_all_daily: bool = False


class NotificationRecipientUpdate(BaseModel):
    user_name: Optional[str] = None
    role: Optional[str] = None
    receive_all_weekly: Optional[bool] = None
    receive_all_daily: Optional[bool] = None
    active: Optional[bool] = None


class TestEmailRequest(BaseModel):
    to_email: str
    to_name: Optional[str] = None


class TriggerEmailResponse(BaseModel):
    success: bool
    emails_sent: int = 0
    emails_failed: int = 0
    skipped: int = 0
    errors: List[str] = []
    detail: str = ""
    test_mode: bool = False
    test_recipient: Optional[str] = Field(
        default=None,
        description="Normalized test addresses echoed back; comma-separated when multiple were used.",
    )


class AccountHealthChange(BaseModel):
    """A detected health score change for one account."""
    account_id: str
    account_name: str
    previous_score: int
    current_score: int
    previous_category: str
    current_category: str
    score_delta: int
    category_changed: bool


class AccountSupportChange(BaseModel):
    """Detected support ticket changes for one account."""
    account_id: str
    account_name: str
    new_critical: int = 0
    new_high: int = 0
    total_open: int = 0
    sentiment_change: Optional[str] = None


class AccountUsageChange(BaseModel):
    """Detected Pendo usage anomalies for one account."""
    account_id: str
    account_name: str
    current_visitors: int = 0
    avg_visitors_7d: float = 0.0
    drop_pct: float = 0.0
    trend: str = "stable"  # 'declining', 'stable', 'improving'


class AccountGongChange(BaseModel):
    """Detected Gong activity for one account."""
    account_id: str
    account_name: str
    new_calls_today: int = 0
    risk_tracker_hits: int = 0
    days_since_last_meeting: Optional[int] = None
    alert_type: str = ""  # 'risk_signal', 'no_meeting', 'new_call'


class AccountRenewalAlert(BaseModel):
    """Renewal proximity alert for one account."""
    account_id: str
    account_name: str
    renewal_days: int
    arr: float = 0.0
    window: str  # '30d', '60d', '90d'


class DailyChangeSummary(BaseModel):
    """Aggregated daily changes across all categories for one account."""
    account_id: str
    account_name: str
    csm_email: Optional[str] = None
    health_changes: List[AccountHealthChange] = []
    support_changes: List[AccountSupportChange] = []
    usage_changes: List[AccountUsageChange] = []
    gong_changes: List[AccountGongChange] = []
    renewal_alerts: List[AccountRenewalAlert] = []

    @property
    def has_changes(self) -> bool:
        return any([
            self.health_changes,
            self.support_changes,
            self.usage_changes,
            self.gong_changes,
            self.renewal_alerts,
        ])
