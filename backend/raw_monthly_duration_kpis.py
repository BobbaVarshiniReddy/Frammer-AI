"""
kpis.py
"""

import pyarrow as pa
from database import get_connection


def get_kpi05_total_content_hours_processed() -> tuple[pa.Table, str]:
    """
    KPI 05: Total Content Hours Processed.
    Formula: SUM(parse_hrs("Total Created Duration"))

    Returns
    -------
    table       : columns ["total_content_hours"]
    description : str
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            SUM(parse_hrs("Total Created Duration")) AS "total_content_hours"
        FROM month_wise_duration
    """).arrow()
    con.close()
    return table, "KPI 05 – Total Content Hours Processed"


def get_kpi05_monthly_created_duration_trend() -> tuple[pa.Table, str]:
    """
    KPI 05 Trend: Monthly Created Duration Trend.
    Formula: SUM(parse_hrs("Total Created Duration")) per month

    Returns
    -------
    table       : columns ["Month", "total_created_hours"]
    description : str
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "Month"                                                   AS "Month",
            SUM(parse_hrs("Total Created Duration"))                  AS "total_created_hours"
        FROM month_wise_duration
        GROUP BY "Month"
        ORDER BY strptime("Month", '%b, %Y')
    """).arrow()
    con.close()
    return table, "KPI 05 – Monthly Created Duration Trend"


def get_kpi03_mom_duration_growth() -> tuple[pa.Table, str]:
    """
    KPI 03: MoM Duration Growth (%)

    Returns
    -------
    table       : columns ["Month", "current_month_hours", "last_month_hours", "mom_growth_pct"]
    description : str
    """
    con = get_connection()
    table = con.execute("""
        WITH monthly_data AS (
            SELECT
                "Month",
                SUM(parse_hrs("Total Created Duration")) AS "current_month_hours"
            FROM month_wise_duration
            GROUP BY "Month"
        ),
        lagged_data AS (
            SELECT
                "Month",
                "current_month_hours",
                LAG("current_month_hours") OVER (
                    ORDER BY strptime("Month", '%b, %Y')
                ) AS "last_month_hours"
            FROM monthly_data
        )
        SELECT
            "Month",
            "current_month_hours",
            "last_month_hours",
            ROUND(
                ("current_month_hours" - "last_month_hours") * 100.0
                / "last_month_hours",
                2
            ) AS "mom_growth_pct"
        FROM lagged_data
        ORDER BY strptime("Month", '%b, %Y')
    """).arrow()
    con.close()
    return table, "KPI 03 – MoM Duration Growth (%)"