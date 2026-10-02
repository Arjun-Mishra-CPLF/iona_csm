# Databricks notebook source
# Databricks notebook source
# Notebook: compute_health_scores (Daily Snapshot)
# Computes health scores using the SAME logic as the web app

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, DoubleType
from datetime import date

# ═══════════════════════════════════════════════════════════════
# STEP 1: Get all accounts with renewal days
# FIXED: Removed strict filters to match web app logic
# ═══════════════════════════════════════════════════════════════

accounts_df = spark.sql("""
    SELECT
        c.account_id,
        c.account AS account_name,
        MIN(
            CASE
                WHEN fct.RENEWAL_NOT_YET_CONTRACTED = 'Y'
                    AND fct.rev_rec_end_date IS NOT NULL
                THEN DATEDIFF(TRY_CAST(fct.REV_REC_END_DATE AS DATE), CURRENT_DATE())
                ELSE 9999
            END
        ) as renewal_days
    FROM silver.silver_layer.dim_customers c
    LEFT JOIN silver.silver_layer.fct_contracts fct ON c.account_id = fct.account_id
    WHERE c._fivetran_deleted = false
      AND COALESCE(c.account_type, '') != 'Churn'
      AND c.account_id NOT IN (
          SELECT fc.account_id
          FROM silver.silver_layer.fct_contracts fc
          WHERE fc.account_id IS NOT NULL
          GROUP BY fc.account_id
          HAVING COUNT(*) = SUM(
              CASE WHEN fc.renewal_not_yet_contracted = 'Y'
                    AND fc.churn_expected_occurred = 'Y'
              THEN 1 ELSE 0 END
          )
      )
    GROUP BY c.account_id, c.account
""")

print(f"Accounts loaded: {accounts_df.count()}")

# Debug: Check Anglian Water renewal days
print("Debug - Anglian Water renewal:")
accounts_df.filter(F.col("account_name").like("%Anglian%")).show(truncate=False)

# ═══════════════════════════════════════════════════════════════
# STEP 1b: ARR materiality data (nearest renewal ARR vs total)
# Uses arr_cumulative_eur to scale renewal deductions by contract
# materiality — prevents small contracts from driving large deductions
# ═══════════════════════════════════════════════════════════════

arr_df = spark.sql("""
    WITH contract_lines AS (
        SELECT
            account_id,
            DATEDIFF(TRY_CAST(REV_REC_END_DATE AS DATE), CURRENT_DATE()) AS days_to_renewal,
            COALESCE(TRY_CAST(arr_cumulative_eur AS DOUBLE), 0) AS line_arr
        FROM silver.silver_layer.fct_contracts
        WHERE RENEWAL_NOT_YET_CONTRACTED = 'Y'
          AND rev_rec_end_date IS NOT NULL
    ),
    account_totals AS (
        SELECT
            account_id,
            MIN(days_to_renewal) AS min_days,
            SUM(line_arr) AS total_account_arr
        FROM contract_lines
        GROUP BY account_id
    ),
    nearest AS (
        SELECT
            cl.account_id,
            SUM(cl.line_arr) AS nearest_renewal_arr
        FROM contract_lines cl
        JOIN account_totals at ON cl.account_id = at.account_id
            AND cl.days_to_renewal = at.min_days
        GROUP BY cl.account_id
    )
    SELECT
        at.account_id,
        COALESCE(n.nearest_renewal_arr, 0) AS nearest_renewal_arr,
        at.total_account_arr
    FROM account_totals at
    LEFT JOIN nearest n ON at.account_id = n.account_id
""")

print(f"ARR materiality data loaded: {arr_df.count()}")

# ═══════════════════════════════════════════════════════════════
# STEP 2: Get Pendo metrics (30-day current vs 30-60 day previous)
# ═══════════════════════════════════════════════════════════════

pendo_df = spark.sql("""
    SELECT
        pc.name AS account_name,
        SUM(CASE WHEN p.date_day >= DATE_SUB(CURRENT_DATE(), 30)
            THEN COALESCE(p.count_active_visitors, 0) ELSE 0 END) as current_visitors,
        SUM(CASE WHEN p.date_day >= DATE_SUB(CURRENT_DATE(), 60)
                  AND p.date_day < DATE_SUB(CURRENT_DATE(), 30)
            THEN COALESCE(p.count_active_visitors, 0) ELSE 0 END) as previous_visitors
    FROM silver.silver_layer.dim_pendo_account_customers pc
    LEFT JOIN silver.silver_layer.fct_pendo_account_daily_metrics p
        ON p.account_id = pc.id AND p.date_day >= DATE_SUB(CURRENT_DATE(), 60)
    GROUP BY pc.name
""")

print(f"Pendo metrics loaded: {pendo_df.count()}")

# ═══════════════════════════════════════════════════════════════
# STEP 3: Get Freshdesk ticket counts
# ═══════════════════════════════════════════════════════════════

freshdesk_df = spark.sql("""
    SELECT
        fc.name AS account_name,
        SUM(CASE WHEN t.label_for_customer NOT IN ('Closed', 'Resolved')
                  AND t.priority = 'Urgent' THEN 1 ELSE 0 END) as open_critical,
        SUM(CASE WHEN t.label_for_customer NOT IN ('Closed', 'Resolved')
                  AND t.priority = 'High' THEN 1 ELSE 0 END) as open_high,
        COUNT(CASE WHEN t.label_for_customer NOT IN ('Closed', 'Resolved')
              THEN 1 END) as open_total
    FROM silver.silver_layer.dim_freshdesk_account_customers fc
    LEFT JOIN silver.silver_layer.fct_freshdesk_ticket_history t
        ON t.company_id = fc.id
    WHERE fc._fivetran_deleted = false
    GROUP BY fc.name
""")

print(f"Freshdesk metrics loaded: {freshdesk_df.count()}")

# ═══════════════════════════════════════════════════════════════
# STEP 3.5: Gong sentiment signals (90-day window)
# Uses Gong's auto-detected tracker hits to measure risk vs engagement.
# Only accounts that appear in the bridge join get a Gong signal;
# all others receive -1 (sentinel = no Gong data → no deduction).
#
# Risk trackers:       competitive, objection, pricing concerns
# Engagement trackers: champion, value, business goals, decision process
# ═══════════════════════════════════════════════════════════════

GONG_CALLS    = "silver.silver_layer.fct_gong_call"
GONG_BRIDGE   = "silver.silver_layer.bridge_gong_call_to_sf_object"
GONG_TRACKERS = "silver.silver_layer.fct_gong_call_tracker_hit"

RISK_TRACKERS = "('Competitors','Competition','Objections','Customer objections','Customer concerns','Discount','Reactions to pricing')"
ENG_TRACKERS  = "('Value (tracker)','Champion','Business goals (tracker)','Strategic business goals','Decision process (tracker)','Decision process (beta)','Economic buyer')"

gong_df = spark.sql(f"""
    SELECT
        b.object_id AS account_id,
        COUNT(DISTINCT CASE
            WHEN t.tracker_name IN {RISK_TRACKERS}
            THEN g.call_id END)                        AS risk_signal_calls,
        COUNT(DISTINCT CASE
            WHEN t.tracker_name IN {ENG_TRACKERS}
            THEN g.call_id END)                        AS engagement_signal_calls
    FROM {GONG_CALLS} g
    JOIN {GONG_BRIDGE} b  ON g.call_id = b.call_id
    JOIN {GONG_TRACKERS} t ON CAST(t.call_id AS STRING) = g.call_id
    WHERE b.object_type = 'account'
      AND g.call_started_ts >= DATE_SUB(CURRENT_DATE(), 90)
    GROUP BY b.object_id
""")

print(f"Gong sentiment data loaded: {gong_df.count()} accounts with tracker signals")

# ═══════════════════════════════════════════════════════════════
# STEP 4: Join all data (including ARR materiality and Gong)
# ═══════════════════════════════════════════════════════════════

joined_df = accounts_df \
    .join(arr_df, "account_id", "left") \
    .join(pendo_df,
          F.lower(F.trim(accounts_df.account_name)) == F.lower(F.trim(pendo_df.account_name)),
          "left") \
    .drop(pendo_df.account_name) \
    .join(freshdesk_df,
          F.lower(F.trim(accounts_df.account_name)) == F.lower(F.trim(freshdesk_df.account_name)),
          "left") \
    .drop(freshdesk_df.account_name) \
    .join(gong_df, "account_id", "left")

# Fill nulls — use -1 for Gong signal columns to distinguish "no Gong data"
# from "has calls but 0 tracker hits" (which would be 0)
joined_df = joined_df.fillna({
    "current_visitors": 0,
    "previous_visitors": 0,
    "open_critical": 0,
    "open_high": 0,
    "open_total": 0,
    "renewal_days": 999,
    "nearest_renewal_arr": 0.0,
    "total_account_arr": 0.0,
    "risk_signal_calls": -1,
    "engagement_signal_calls": -1,
})

print(f"Joined data: {joined_df.count()}")

# ═══════════════════════════════════════════════════════════════
# STEP 5: Calculate health scores (MATCHES WEB APP LOGIC)
# Rebalanced weights — total max deductions = 100:
#   Renewal    max 20 (was 25)
#   Pendo      max 35 (was 40)
#   Freshdesk  max 30 (was 35)
#   Gong       max 15 (new)
# ═══════════════════════════════════════════════════════════════

@F.udf(IntegerType())
def calculate_health_score(renewal_days, current_visitors, previous_visitors,
                           open_critical, open_high, has_pendo, has_freshdesk,
                           nearest_renewal_arr, total_account_arr,
                           risk_signal_calls, engagement_signal_calls):
    """
    Calculate health score with materiality-aware renewal deduction and Gong sentiment.
    Score = 100 - (renewal + pendo + freshdesk + gong deductions)

    Materiality ratio = nearest_renewal_arr / total_account_arr
    If no ARR data (both 0), full deduction applies (conservative default).

    Gong: risk_signal_calls == -1 means no Gong data → 0 deduction (don't penalise).
    """
    # 1. RENEWAL DEDUCTION (max 20) — scaled by materiality
    if renewal_days is None:
        renewal_days = 999

    if renewal_days <= 30:
        renewal_deduction = 20
    elif renewal_days <= 60:
        renewal_deduction = 15
    elif renewal_days <= 90:
        renewal_deduction = 10
    elif renewal_days <= 180:
        renewal_deduction = 4
    else:
        renewal_deduction = 0

    # Apply materiality scaling: scale deduction by proportion of ARR at risk
    if renewal_deduction > 0 and total_account_arr is not None and total_account_arr > 0:
        nearest_arr = nearest_renewal_arr if nearest_renewal_arr is not None else 0.0
        materiality_ratio = nearest_arr / total_account_arr
        renewal_deduction = int(round(renewal_deduction * materiality_ratio))

    # 2. PENDO DEDUCTION (max 35)
    pendo_deduction = 0
    if has_pendo:
        if previous_visitors and previous_visitors > 0:
            change_pct = ((current_visitors - previous_visitors) / previous_visitors) * 100

            if current_visitors == 0:
                pendo_deduction = 35
            elif change_pct <= -50:
                pendo_deduction = 30
            elif change_pct <= -30:
                pendo_deduction = 22
            elif change_pct <= -10:
                pendo_deduction = 10
            else:
                pendo_deduction = 0  # Stable or growing
        elif current_visitors and current_visitors > 0:
            pendo_deduction = 0  # Has activity, no baseline
        else:
            pendo_deduction = 18  # No activity recorded

    # 3. FRESHDESK DEDUCTION (max 30)
    freshdesk_deduction = 0
    if has_freshdesk:
        if open_critical and open_critical > 0:
            freshdesk_deduction += 18
        if open_high and open_high > 0:
            freshdesk_deduction += min(8, open_high * 4)
        freshdesk_deduction = min(30, freshdesk_deduction)

    # 4. GONG SENTIMENT DEDUCTION (max 15)
    # risk_signal_calls == -1 means this account has no Gong data at all → skip
    gong_deduction = 0
    has_gong = risk_signal_calls is not None and risk_signal_calls >= 0
    if has_gong:
        r = risk_signal_calls if risk_signal_calls is not None else 0
        e = engagement_signal_calls if engagement_signal_calls is not None else 0
        if r == 0 and e == 0:
            gong_deduction = 3   # Has calls (bridge join hit), but no notable tracker hits
        elif r == 0:
            gong_deduction = 0   # Only positive signals detected
        elif e == 0:
            gong_deduction = 15  # Pure risk signal, nothing positive
        elif r > e:
            gong_deduction = 10  # Risk-dominant
        else:
            gong_deduction = 5   # Mixed — slight concern

    # Calculate final score
    total_deduction = renewal_deduction + pendo_deduction + freshdesk_deduction + gong_deduction
    score = 100 - total_deduction
    return max(0, min(100, score))


@F.udf(StringType())
def get_health_category(score):
    """Map score to category."""
    if score is None:
        return "Good"
    if score >= 70:
        return "Good"
    elif score >= 40:
        return "At Risk"
    else:
        return "Critical"


# Add has_pendo, has_freshdesk, has_gong flags
health_df = joined_df \
    .withColumn("has_pendo",
                (F.col("current_visitors") > 0) | (F.col("previous_visitors") > 0)) \
    .withColumn("has_freshdesk",
                F.col("open_total") > 0) \
    .withColumn("has_gong",
                F.col("risk_signal_calls") >= 0)

# Calculate health score
health_df = health_df.withColumn(
    "health_score",
    calculate_health_score(
        F.col("renewal_days"),
        F.col("current_visitors"),
        F.col("previous_visitors"),
        F.col("open_critical"),
        F.col("open_high"),
        F.col("has_pendo"),
        F.col("has_freshdesk"),
        F.col("nearest_renewal_arr"),
        F.col("total_account_arr"),
        F.col("risk_signal_calls"),
        F.col("engagement_signal_calls"),
    )
)

# Add category
health_df = health_df.withColumn(
    "health_category",
    get_health_category(F.col("health_score"))
)

# Debug: Show Anglian Water to verify materiality effect
print("\n🔍 Debug - Anglian Water health score (with materiality):")
health_df.filter(F.col("account_name").like("%Anglian%")).show(truncate=False)

# Show some at-risk accounts
print("\n⚠️ At Risk Accounts (score < 70):")
health_df.filter(F.col("health_score") < 70).orderBy("health_score").show(20, truncate=False)

# ═══════════════════════════════════════════════════════════════
# STEP 6: Write to table (daily snapshot)
# Added risk_signal_calls, engagement_signal_calls, has_gong for Gong factor
# ═══════════════════════════════════════════════════════════════

final_df = health_df.select(
    "account_id",
    "account_name",
    "health_score",
    "health_category",
    "renewal_days",
    "current_visitors",
    "previous_visitors",
    "open_critical",
    "open_high",
    "open_total",
    "has_pendo",
    "has_freshdesk",
    F.round(F.col("nearest_renewal_arr"), 2).alias("nearest_renewal_arr"),
    F.round(F.col("total_account_arr"), 2).alias("total_account_arr"),
    "risk_signal_calls",
    "engagement_signal_calls",
    "has_gong",
).withColumn("score_date", F.current_date()) \
 .withColumn("computed_at", F.current_timestamp())

print(f"\nRecords to insert: {final_df.count()}")

TABLE_NAME = "silver.silver_layer.account_health_scores_history"

# Check if table exists
table_exists = spark.catalog.tableExists(TABLE_NAME)

if not table_exists:
    # First run - create table
    print(f"Creating new table: {TABLE_NAME}")
    final_df.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(TABLE_NAME)
    print(f"✓ Created table with {final_df.count()} records")
else:
    # Delete today's data if exists, then insert fresh
    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE score_date = CURRENT_DATE()")

    final_df.write \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(TABLE_NAME)
    print(f"✓ Inserted {final_df.count()} records for {date.today()}")

# ═══════════════════════════════════════════════════════════════
# Summary
# ═══════════════════════════════════════════════════════════════

print("\n📊 Table Summary:")
spark.sql(f"""
    SELECT
        score_date,
        COUNT(*) as accounts,
        ROUND(AVG(health_score), 1) as avg_score,
        SUM(CASE WHEN health_category = 'Good' THEN 1 ELSE 0 END) as good,
        SUM(CASE WHEN health_category = 'At Risk' THEN 1 ELSE 0 END) as at_risk,
        SUM(CASE WHEN health_category = 'Critical' THEN 1 ELSE 0 END) as critical,
        SUM(CASE WHEN has_gong = true THEN 1 ELSE 0 END) as with_gong_data
    FROM {TABLE_NAME}
    GROUP BY score_date
    ORDER BY score_date DESC
    LIMIT 10
""").show()

# Verify Anglian Water in final table
print("\n✅ Final check - Anglian Water in table:")
spark.sql(f"""
    SELECT account_name, health_score, health_category, renewal_days,
           open_high, open_critical, nearest_renewal_arr, total_account_arr,
           risk_signal_calls, engagement_signal_calls, has_gong
    FROM {TABLE_NAME}
    WHERE score_date = CURRENT_DATE()
      AND account_name LIKE '%Anglian%'
""").show(truncate=False)

# Show accounts with notable Gong risk signals
print("\n📞 Accounts with Gong risk signals (top 10):")
spark.sql(f"""
    SELECT account_name, health_score, health_category,
           risk_signal_calls, engagement_signal_calls
    FROM {TABLE_NAME}
    WHERE score_date = CURRENT_DATE()
      AND has_gong = true
      AND risk_signal_calls > 0
    ORDER BY risk_signal_calls DESC
    LIMIT 10
""").show(truncate=False)
