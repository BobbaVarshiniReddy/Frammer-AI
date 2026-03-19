"""
by_channel_and_user_kpis.py
KPI definitions for combined_data by_channel_and_user
"""
import pyarrow as pa
from database import get_connection


def get_kpi06_publish_rate_by_channel() -> tuple[pa.Table, str]:
    """
    KPI-06 : Publish Rate by Channel
    SUM(Published) ÷ SUM(Created) × 100
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            TRIM("Channel")                                          AS "Channel",
            SUM("Published Count")                                   AS "Total_Published",
            SUM("Created Count")                                     AS "Total_Created",
            ROUND(
                100.0 * SUM("Published Count")
                / NULLIF(SUM("Created Count"), 0),
                2
            )                                                        AS "Publish_Rate_%"
        FROM raw_channel_user
        GROUP BY TRIM("Channel")
        ORDER BY "Publish_Rate_%" DESC
    """).arrow()
    con.close()
    return table, "KPI-06 - Publish Rate by Channel (%)"


def get_kpi16_user_publish_rate_within_channel() -> tuple[pa.Table, str]:
    """
    KPI-16 : User Publish Rate within Channel
    User Published ÷ User Created × 100
    Filtered to specific channel — returns all channel-user pairs
    ranked by publish rate within each channel
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            TRIM("Channel")                                          AS "Channel",
            "User",
            SUM("Published Count")                                   AS "Total_Published",
            SUM("Created Count")                                     AS "Total_Created",
            ROUND(
                100.0 * SUM("Published Count")
                / NULLIF(SUM("Created Count"), 0),
                2
            )                                                        AS "User_Publish_Rate_%",
            RANK() OVER (
                PARTITION BY TRIM("Channel")
                ORDER BY
                    ROUND(
                        100.0 * SUM("Published Count")
                        / NULLIF(SUM("Created Count"), 0),
                        2
                    ) DESC
            )                                                        AS "Rank_Within_Channel"
        FROM raw_channel_user
        GROUP BY TRIM("Channel"), "User"
        ORDER BY TRIM("Channel"), "Rank_Within_Channel"
    """).arrow()
    con.close()
    return table, "KPI-16 - User Publish Rate within Channel (%)"


def get_kpi17_user_upload_volume_by_channel() -> tuple[pa.Table, str]:
    """
    KPI-17 : User Upload Volume by Channel
    ORDER BY Uploaded Count DESC within Channel
    Shows top uploaders per channel
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            TRIM("Channel")                                          AS "Channel",
            "User",
            SUM("Uploaded Count")                                    AS "Total_Uploads",
            RANK() OVER (
                PARTITION BY TRIM("Channel")
                ORDER BY SUM("Uploaded Count") DESC
            )                                                        AS "Upload_Rank"
        FROM raw_channel_user
        GROUP BY TRIM("Channel"), "User"
        ORDER BY TRIM("Channel"), "Upload_Rank"
    """).arrow()
    con.close()
    return table, "KPI-17 - User Upload Volume by Channel"


def get_kpi18_channel_user_cross_dimension() -> tuple[pa.Table, str]:
    """
    KPI-18 : Channel × User Cross-Dimension Metric
    Published Count ÷ Created Count × 100
    Each row = one Channel-User pair publish rate
    Reveals same user behaving differently across channels
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            TRIM("Channel")                                          AS "Channel",
            "User",
            SUM("Uploaded Count")                                    AS "Total_Uploads",
            SUM("Created Count")                                     AS "Total_Created",
            SUM("Published Count")                                   AS "Total_Published",
            ROUND(
                100.0 * SUM("Published Count")
                / NULLIF(SUM("Created Count"), 0),
                2
            )                                                        AS "Publish_Rate_%",
            ROUND(
                1.0 * SUM("Created Count")
                / NULLIF(SUM("Uploaded Count"), 0),
                2
            )                                                        AS "Amplification_Ratio"
        FROM raw_channel_user
        GROUP BY TRIM("Channel"), "User"
        ORDER BY "Publish_Rate_%" DESC
    """).arrow()
    con.close()
    return table, "KPI-18 - Channel x User Cross-Dimension Metric"