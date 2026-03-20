"""
raw_monthly_duration_kpis.py
"""

import pyarrow as pa
from database import get_connection


def _dur_to_hours(col: str) -> str:
    """Convert hh:mm:ss text column to hours as FLOAT."""
    return f"""(
        split_part(CAST({col} AS VARCHAR), ':', 1)::FLOAT +
        split_part(CAST({col} AS VARCHAR), ':', 2)::FLOAT / 60 +
        split_part(CAST({col} AS VARCHAR), ':', 3)::FLOAT / 3600
    )"""


# ─────────────────────────────────────────────────────────────────────────────
# KPI 05: Total Content Hours Processed
# ─────────────────────────────────────────────────────────────────────────────

def get_kpi05_total_content_hours_processed() -> list:
    con = get_connection()
    table = con.execute(f"""
        SELECT
            ROUND(SUM({_dur_to_hours('"Total Created Duration"')}), 2)
                AS "total_content_hours"
        FROM raw_monthly_duration
    """).arrow()
    con.close()
    return [
        [table, "kpi_card"],
        [table, "bar"],
    ]


# ─────────────────────────────────────────────────────────────────────────────
# KPI 05 Trend: Monthly Created Duration
# ─────────────────────────────────────────────────────────────────────────────

def get_kpi05_monthly_created_duration_trend() -> list:
    con = get_connection()
    table = con.execute(f"""
        SELECT
            "Month",
            ROUND({_dur_to_hours('"Total Created Duration"')}, 2)
                AS "total_created_hours"
        FROM raw_monthly_duration
        ORDER BY strptime("Month", '%b, %Y')
    """).arrow()
    con.close()
    return [
        [table, "line"],
        [table, "bar"],
    ]


# ─────────────────────────────────────────────────────────────────────────────
# KPI 03: MoM Duration Growth (%)
# ─────────────────────────────────────────────────────────────────────────────

def get_kpi03_mom_duration_growth() -> list:
    con = get_connection()
    table = con.execute(f"""
        WITH monthly AS (
            SELECT
                "Month",
                ROUND({_dur_to_hours('"Total Created Duration"')}, 2)
                    AS "current_month_hours"
            FROM raw_monthly_duration
        ),
        lagged AS (
            SELECT
                "Month",
                "current_month_hours",
                LAG("current_month_hours") OVER (
                    ORDER BY strptime("Month", '%b, %Y')
                ) AS "last_month_hours"
            FROM monthly
        )
        SELECT
            "Month",
            "current_month_hours",
            "last_month_hours",
            ROUND(
                ("current_month_hours" - "last_month_hours") * 100.0
                / NULLIF("last_month_hours", 0),
                2
            ) AS "mom_growth_pct"
        FROM lagged
        ORDER BY strptime("Month", '%b, %Y')
    """).arrow()
    con.close()
    return [
        [table, "line"],
        [table, "bar"],
    ]