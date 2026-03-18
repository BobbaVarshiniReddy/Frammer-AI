"""
by_channel_and_user_plots.py
Plot function definitions for combined_data by_channel_and_user
"""
import pyarrow as pa
from database import get_connection


# ─────────────────────────────────────────────────────────────────────────────
# Plot 1 — Publish Rate % per Channel
# ─────────────────────────────────────────────────────────────────────────────
def get_publish_rate_by_channel() -> tuple[pa.Table, str]:
    """
    Plot 1 : Publish Rate % per Channel (bar)
    Returns
    -------
    table      : columns ["Channel", "Publish_Rate_%"]
    chart_type : "bar"
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            TRIM("Channel")                                          AS "Channel",
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
    return table, "bar"


# ─────────────────────────────────────────────────────────────────────────────
# Plot 2 — Created vs Published per Channel
# ─────────────────────────────────────────────────────────────────────────────
def get_created_vs_published_by_channel() -> tuple[pa.Table, str]:
    """
    Plot 2 : Total Created vs Total Published per Channel (grouped_bar)
    Returns
    -------
    table      : columns ["Channel", "Total_Created", "Total_Published"]
    chart_type : "grouped_bar"
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            TRIM("Channel")                AS "Channel",
            SUM("Created Count")           AS "Total_Created",
            SUM("Published Count")         AS "Total_Published"
        FROM raw_channel_user
        GROUP BY TRIM("Channel")
        ORDER BY "Total_Created" DESC
    """).arrow()
    con.close()
    return table, "grouped_bar"


# ─────────────────────────────────────────────────────────────────────────────
# Plot 3 — Upload Volume per Channel
# ─────────────────────────────────────────────────────────────────────────────
def get_upload_volume_by_channel() -> tuple[pa.Table, str]:
    """
    Plot 3 : Total Upload Volume per Channel (bar)
    Returns
    -------
    table      : columns ["Channel", "Total_Uploads"]
    chart_type : "bar"
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            TRIM("Channel")                AS "Channel",
            SUM("Uploaded Count")          AS "Total_Uploads"
        FROM raw_channel_user
        GROUP BY TRIM("Channel")
        ORDER BY "Total_Uploads" DESC
    """).arrow()
    con.close()
    return table, "bar"


# ─────────────────────────────────────────────────────────────────────────────
# Plot 4 — Top 3 Uploaders per Channel
# ─────────────────────────────────────────────────────────────────────────────
def get_top3_uploaders_per_channel() -> tuple[pa.Table, str]:
    """
    Plot 4 : Top 3 Uploaders per Channel (bar)
    Returns
    -------
    table      : columns ["Channel", "User", "Total_Uploads", "Upload_Rank"]
    chart_type : "bar"
    """
    con = get_connection()
    table = con.execute("""
        WITH ranked AS (
            SELECT
                TRIM("Channel")                                      AS "Channel",
                "User",
                SUM("Uploaded Count")                                AS "Total_Uploads",
                RANK() OVER (
                    PARTITION BY TRIM("Channel")
                    ORDER BY SUM("Uploaded Count") DESC
                )                                                    AS "Upload_Rank"
            FROM raw_channel_user
            GROUP BY TRIM("Channel"), "User"
        )
        SELECT *
        FROM ranked
        WHERE "Upload_Rank" <= 3
        ORDER BY "Channel", "Upload_Rank"
    """).arrow()
    con.close()
    return table, "bar"


# ─────────────────────────────────────────────────────────────────────────────
# Plot 5 — Top 3 User Publish Rate per Channel
# ─────────────────────────────────────────────────────────────────────────────
def get_top3_user_publish_rate_per_channel() -> tuple[pa.Table, str]:
    """
    Plot 5 : Top 3 Users by Publish Rate within each Channel (bar)
    Returns
    -------
    table      : columns ["Channel", "User", "User_Publish_Rate_%", "Rank"]
    chart_type : "bar"
    """
    con = get_connection()
    table = con.execute("""
        WITH ranked AS (
            SELECT
                TRIM("Channel")                                      AS "Channel",
                "User",
                ROUND(
                    100.0 * SUM("Published Count")
                    / NULLIF(SUM("Created Count"), 0),
                    2
                )                                                    AS "User_Publish_Rate_%",
                RANK() OVER (
                    PARTITION BY TRIM("Channel")
                    ORDER BY
                        ROUND(
                            100.0 * SUM("Published Count")
                            / NULLIF(SUM("Created Count"), 0),
                            2
                        ) DESC
                )                                                    AS "Rank"
            FROM raw_channel_user
            WHERE "Created Count" >= 5
            GROUP BY TRIM("Channel"), "User"
        )
        SELECT *
        FROM ranked
        WHERE "Rank" <= 3
        ORDER BY "Channel", "Rank"
    """).arrow()
    con.close()
    return table, "bar"


# ─────────────────────────────────────────────────────────────────────────────
# Plot 6 — User × Channel Publish Rate Heatmap
# ─────────────────────────────────────────────────────────────────────────────
def get_user_channel_publish_rate_heatmap() -> tuple[pa.Table, str]:
    """
    Plot 6 : User × Channel Publish Rate % Heatmap
    Returns
    -------
    table      : columns ["Channel", "User", "User_Publish_Rate_%"]
    chart_type : "heatmap"
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            TRIM("Channel")                                          AS "Channel",
            "User",
            ROUND(
                100.0 * SUM("Published Count")
                / NULLIF(SUM("Created Count"), 0),
                2
            )                                                        AS "User_Publish_Rate_%"
        FROM raw_channel_user
        WHERE "Created Count" >= 5
        GROUP BY TRIM("Channel"), "User"
        ORDER BY TRIM("Channel"), "User"
    """).arrow()
    con.close()
    return table, "heatmap"


# ─────────────────────────────────────────────────────────────────────────────
# Plot 7 — Top Users Publish Rate across Channels (KPI-18)
# ─────────────────────────────────────────────────────────────────────────────
def get_top_users_publish_rate_across_channels() -> tuple[pa.Table, str]:
    """
    Plot 7 : Top 8 Users — Publish Rate % across all Channels (small_multiples)
    Reveals same user behaving differently across channels
    Returns
    -------
    table      : columns ["Channel", "User", "User_Publish_Rate_%"]
    chart_type : "small_multiples"
    """
    con = get_connection()
    table = con.execute("""
        WITH top_users AS (
            SELECT "User"
            FROM raw_channel_user
            GROUP BY "User"
            ORDER BY SUM("Published Count") DESC
            LIMIT 8
        )
        SELECT
            TRIM("Channel")                                          AS "Channel",
            b."User",
            ROUND(
                100.0 * SUM("Published Count")
                / NULLIF(SUM("Created Count"), 0),
                2
            )                                                        AS "User_Publish_Rate_%"
        FROM raw_channel_user b
        INNER JOIN top_users t ON b."User" = t."User"
        WHERE "Created Count" >= 5
        GROUP BY TRIM("Channel"), b."User"
        ORDER BY b."User", TRIM("Channel")
    """).arrow()
    con.close()
    return table, "small_multiples"

# ─────────────────────────────────────────────────────────────────────────────
# Plot 8 — User Contribution % per Channel 
# ─────────────────────────────────────────────────────────────────────────────
def get_user_contribution_per_channel() -> tuple[pa.Table, str]:
    """
    Plot : Top 3 User Contribution % per Channel (small_multiples)
    Each user's share of channel's total published count
    Returns
    -------
    table      : columns ["Channel", "User", "Published_Count", "Contribution_%"]
    chart_type : "small_multiples"
    """
    con = get_connection()
    table = con.execute("""
        WITH channel_totals AS (
            SELECT
                TRIM("Channel")                AS "Channel",
                SUM("Published Count")         AS "Channel_Total_Published"
            FROM raw_channel_user
            GROUP BY TRIM("Channel")
        ),
        user_published AS (
            SELECT
                TRIM("Channel")                AS "Channel",
                "User",
                SUM("Published Count")         AS "Published_Count"
            FROM raw_channel_user
            GROUP BY TRIM("Channel"), "User"
            HAVING SUM("Published Count") > 0
        ),
        ranked AS (
            SELECT
                u."Channel",
                u."User",
                u."Published_Count",
                ROUND(
                    100.0 * u."Published_Count"
                    / NULLIF(c."Channel_Total_Published", 0),
                    2
                )                              AS "Contribution_%",
                RANK() OVER (
                    PARTITION BY u."Channel"
                    ORDER BY u."Published_Count" DESC
                )                              AS "Rank"
            FROM user_published u
            JOIN channel_totals c ON u."Channel" = c."Channel"
        )
        SELECT "Channel", "User", "Published_Count", "Contribution_%"
        FROM ranked
        WHERE "Rank" <= 3
        ORDER BY "Channel", "Contribution_%" DESC
    """).arrow()
    con.close()
    return table, "small_multiples"


# ─────────────────────────────────────────────────────────────────────────────
# Plot- 9 — Top 10 Users by Overall Publish Rate % 
# ─────────────────────────────────────────────────────────────────────────────
def get_top10_users_by_publish_rate() -> tuple[pa.Table, str]:
    """
    Plot : Top 10 Users by Overall Publish Rate % (bar)
    Minimum 10 creations for statistical validity
    Returns
    -------
    table      : columns ["User", "Total_Published", "Total_Created", "User_Publish_Rate_%"]
    chart_type : "bar"
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "User",
            SUM("Published Count")                                   AS "Total_Published",
            SUM("Created Count")                                     AS "Total_Created",
            ROUND(
                100.0 * SUM("Published Count")
                / NULLIF(SUM("Created Count"), 0),
                2
            )                                                        AS "User_Publish_Rate_%"
        FROM raw_channel_user
        GROUP BY "User"
        HAVING SUM("Created Count") >= 10
        ORDER BY "User_Publish_Rate_%" DESC
        LIMIT 10
    """).arrow()
    con.close()
    return table, "bar"