"""
kpis.py
"""

import pyarrow as pa
from database import get_connection


#-------------------------------------------------------------------------
# KPI 05: Total Content Hours Processed
#-------------------------------------------------------------------------
def get_kpi05_total_content_hours_processed() -> list:
    """
    KPI 05: Total Content Hours Processed.
    Formula: SUM(parse_hrs("Total Created Duration"))

    Returns multiple chart options in list format:
    [
        [table, "chart_type"],
        ...
    ]
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            SUM(parse_hrs("Total Created Duration")) AS "total_content_hours"
        FROM month_wise_duration
    """).arrow()
    con.close()

    return [
        [table, "kpi_card"],  # simple KPI display
        [table, "bar"]        # optional bar for visual emphasis
    ]


#-------------------------------------------------------------------------
# KPI 05 Trend: Monthly Created Duration
#-------------------------------------------------------------------------
def get_kpi05_monthly_created_duration_trend() -> list:
    """
    KPI 05 Trend: Monthly Created Duration Trend.
    Formula: SUM(parse_hrs("Total Created Duration")) per month

    Returns multiple chart options in list format
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "Month" AS "Month",
            SUM(parse_hrs("Total Created Duration")) AS "total_created_hours"
        FROM month_wise_duration
        GROUP BY "Month"
        ORDER BY strptime("Month", '%b, %Y')
    """).arrow()
    con.close()

    return [
        [table, "line"],       # trend over months
        [table, "bar"],        # monthly comparison
        [table, "area"]        # area chart for cumulative feeling
    ]


#-------------------------------------------------------------------------
# KPI 03: MoM Duration Growth (%)
#-------------------------------------------------------------------------
def get_kpi03_mom_duration_growth() -> list:
    """
    KPI 03: MoM Duration Growth (%)
    Returns multiple chart options in list format
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

    return [
        [table, "line"],    # trend line of MoM %
        [table, "bar"],     # bar for visual comparison
        [table, "area"]     # area chart for growth visualization
    ]
