"""
kpis.py
"""

import pyarrow as pa
from database import get_connection

def get_kpi03_mom_upload_growth() -> tuple[pa.Table, str]:
    """
    MoM Upload Growth Rate:
    (This Month Uploaded − Last Month Uploaded) ÷ Last Month × 100
    """
    con = get_connection()
    table = con.execute("""
        WITH lagged_data AS (
            SELECT
                "Month",
                "Total Uploaded",
                LAG("Total Uploaded") OVER (
                    ORDER BY strptime("Month", '%b %y')
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
        ORDER BY strptime("Month", '%b %y')
    """).arrow()
    con.close()
    return table, "KPI 03 - MoM Upload Growth Rate (%)"


def get_kpi07_monthly_publish_rate() -> tuple[pa.Table, str]:
    """
    Monthly Publish Rate:
    Total Published ÷ Total Created × 100
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
        ORDER BY strptime("Month", '%b %y')
    """).arrow()
    con.close()
    return table, "KPI 07 - Monthly Publish Rate Trend (%)"


def get_kpi01_overall_publish_rate() -> tuple[pa.Table, str]:
    """
    Overall Publish Rate:
    Running Published ÷ Running Created × 100
    """
    con = get_connection()
    table = con.execute("""
        WITH ordered_data AS (
            SELECT
                "Month",
                "Total Created",
                "Total Published",
                strptime("Month", '%b %y') AS month_date
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
    return table, "KPI 01 - Overall Publish Rate (%)"


def get_kpi02_monthly_amplification_ratio() -> tuple[pa.Table, str]:
    """
    Amplification Ratio:
    Total Created ÷ Total Uploaded
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
        ORDER BY strptime("Month", '%b %y')
    """).arrow()
    con.close()
    return table, "KPI 02 - Monthly Amplification Ratio"
