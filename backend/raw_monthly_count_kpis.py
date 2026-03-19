"""
kpis.py
"""

import pyarrow as pa
from database import get_connection


#-------------------------------------------------------------------------
# KPI 03: MoM Upload Growth Rate (%)
#-------------------------------------------------------------------------
def get_kpi03_mom_upload_growth() -> list:
    """
    MoM Upload Growth Rate:
    (This Month Uploaded − Last Month Uploaded) ÷ Last Month × 100
    Returns multiple chart options
    """
    con = get_connection()
    table = con.execute("""
        WITH lagged_data AS (
            SELECT
                "Month",
                "Total Uploaded",
                LAG("Total Uploaded") OVER (
                    ORDER BY strptime("Month", '%b, %y')
                ) AS "Last Month Uploaded"
            FROM raw_monthly_count
        )
        SELECT
            "Month",
            "Total Uploaded",
            "Last Month Uploaded",
            ROUND(
                100.0 * (
                    "Total Uploaded" - "Last Month Uploaded"
                ) / NULLIF("Last Month Uploaded", 0),
                2
            ) AS "MoM_Upload_Growth_%"
        FROM lagged_data
        ORDER BY strptime("Month", '%b, %y')
    """).arrow()
    con.close()

    return [
        [table, "line"],  # trend line
        [table, "bar"],   # monthly comparison
        [table, "area"]   # growth visualization
    ]


#-------------------------------------------------------------------------
# KPI 07: Monthly Publish Rate (%)
#-------------------------------------------------------------------------
def get_kpi07_monthly_publish_rate() -> list:
    """
    Monthly Publish Rate:
    Total Published ÷ Total Created × 100
    Returns multiple chart options
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "Month",
            "Total Created",
            "Total Published",
            ROUND(
                100.0 * "Total Published" / NULLIF("Total Created", 0),
                2
            ) AS "Monthly_Publish_Rate_%"
        FROM raw_monthly_count
        ORDER BY strptime("Month", '%b, %y')
    """).arrow()
    con.close()

    return [
        [table, "line"],  # trend
        [table, "bar"],   # comparison
        [table, "area"]   # area view
    ]


#-------------------------------------------------------------------------
# KPI 01: Overall Publish Rate (%)
#-------------------------------------------------------------------------
def get_kpi01_overall_publish_rate() -> list:
    """
    Overall Publish Rate:
    Running Published ÷ Running Created × 100
    Returns multiple chart options
    """
    con = get_connection()
    table = con.execute("""
        WITH ordered_data AS (
            SELECT
                "Month",
                "Total Created",
                "Total Published",
                strptime("Month", '%b, %y') AS month_date
            FROM raw_monthly_count
        )
        SELECT
            "Month",
            SUM("Total Created") OVER (ORDER BY month_date) AS "Cumulative_Created",
            SUM("Total Published") OVER (ORDER BY month_date) AS "Cumulative_Published",
            ROUND(
                100.0 *
                SUM("Total Published") OVER (ORDER BY month_date)
                / NULLIF(SUM("Total Created") OVER (ORDER BY month_date), 0),
                2
            ) AS "Overall_Publish_Rate_%"
        FROM ordered_data
        ORDER BY month_date
    """).arrow()
    con.close()

    return [
        [table, "line"],   # cumulative trend
        [table, "area"],   # area view
        [table, "bar"]     # monthly bar comparison
    ]


#-------------------------------------------------------------------------
# KPI 02: Monthly Amplification Ratio
#-------------------------------------------------------------------------
def get_kpi02_monthly_amplification_ratio() -> list:
    """
    Amplification Ratio:
    Total Created ÷ Total Uploaded
    Returns multiple chart options
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "Month",
            "Total Created",
            "Total Uploaded",
            ROUND(
                1.0 * "Total Created" / NULLIF("Total Uploaded", 0),
                2
            ) AS "Amplification_Ratio"
        FROM raw_monthly_count
        ORDER BY strptime("Month", '%b, %y')
    """).arrow()
    con.close()

    return [
        [table, "line"],   # trend
        [table, "bar"],    # comparison
        [table, "area"]    # area visualization
    ]
