import pyarrow as pa
from database import get_connection


def get_kpi01_overall_publish_rate() -> tuple[pa.Table, str]:
    """
    KPI-01: Overall Publish Rate
    SUM(Published Count) ÷ SUM(Created Count) × 100
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            ROUND(
                100.0 * SUM("Published Count") / NULLIF(SUM("Created Count"), 0),
                2
            ) AS "Overall_Publish_Rate_%"
        FROM client_1_combined_data
    """).arrow()
    con.close()
    return table, "KPI 01 - Overall Publish Rate (%)"


def get_kpi02_amplification_ratio() -> tuple[pa.Table, str]:
    """
    KPI-02: Amplification Ratio
    SUM(Created Count) ÷ SUM(Uploaded Count)
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            ROUND(
                1.0 * SUM("Created Count") / NULLIF(SUM("Uploaded Count"), 0),
                2
            ) AS "Amplification_Ratio"
        FROM client_1_combined_data
    """).arrow()
    con.close()
    return table, "KPI 02 - Amplification Ratio"


def get_kpi04_dropoff_volume() -> tuple[pa.Table, str]:
    """
    KPI-04: Drop-off Volume
    SUM(Created Count) − SUM(Published Count)
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            SUM("Created Count") - SUM("Published Count") AS "Dropoff_Volume"
        FROM client_1_combined_data
    """).arrow()
    con.close()
    return table, "KPI 04 - Drop-off Volume"


def get_kpi05_total_content_hours() -> tuple[pa.Table, str]:
    """
    KPI-05: Total Content Hours
    SUM(Created Duration)
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            ROUND(
                SUM(
                    EXTRACT(EPOCH FROM CAST("Created Duration (hh:mm:ss)" AS TIME))
                ) / 3600,
                2
            ) AS "Total_Content_Hours"
        FROM client_1_combined_data
    """).arrow()
    con.close()
    return table, "KPI 05 - Total Content Hours"


def get_kpi06_publish_rate_by_channel() -> tuple[pa.Table, str]:
    """
    KPI-06: Publish Rate by Channel
    Channel Published Count ÷ Channel Created Count × 100
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "Channel",
            "Published Count",
            "Created Count",
            ROUND(
                100.0 * "Published Count" / NULLIF("Created Count", 0),
                2
            ) AS "Publish_Rate_%"
        FROM client_1_combined_data
        ORDER BY "Publish_Rate_%" DESC NULLS LAST
    """).arrow()
    con.close()
    return table, "KPI 06 - Publish Rate by Channel (%)"


def get_kpi08_zero_publish_channels() -> tuple[pa.Table, str]:
    """
    KPI-08: Zero Publish Channel Count
    COUNT(rows WHERE Published Count = 0)
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            COUNT(*) AS "Zero_Publish_Channel_Count"
        FROM client_1_combined_data
        WHERE "Published Count" = 0
    """).arrow()
    con.close()
    return table, "KPI 08 - Zero Publish Channel Count"
