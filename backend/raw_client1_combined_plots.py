"""
Plots for raw_client1_combined_data
"""

import pyarrow as pa
from database import get_connection

# ⚠️ Check your actual table name at http://127.0.0.1:8000/debug/tables
# and replace "raw_client1_combined" below if different
TABLE = "raw_channel_summary"


# ---------------------------------------------------------------------------
# Plot 1 — Publish Rate by Channel
# ---------------------------------------------------------------------------
def get_publish_rate_by_channel() -> tuple[pa.Table, str]:
    """
    Returns
    -------
    table      : ["Channel", "publish_rate"]
    chart_type : "bar"
    """
    con = get_connection()
    table = con.execute(f"""
        SELECT
            "Channel",
            ROUND(
                100.0 * "Published Count" / NULLIF("Created Count", 0),
                2
            ) AS "publish_rate"
        FROM {TABLE}
        ORDER BY "publish_rate" DESC NULLS LAST
    """).arrow()
    con.close()
    return table, "bar"


# ---------------------------------------------------------------------------
# Plot 2 — Uploaded vs Created vs Published Counts
# ---------------------------------------------------------------------------
def get_content_flow_counts() -> tuple[pa.Table, str]:
    """
    Returns
    -------
    table      : ["Channel", "Uploaded", "Created", "Published"]
    chart_type : "bar"
    """
    con = get_connection()
    table = con.execute(f"""
        SELECT
            "Channel",
            "Uploaded Count"  AS "Uploaded",
            "Created Count"   AS "Created",
            "Published Count" AS "Published"
        FROM {TABLE}
    """).arrow()
    con.close()
    return table, "bar"


 
# ---------------------------------------------------------------------------
# Plot 3 — Total Duration by Channel
# ---------------------------------------------------------------------------
def get_total_duration_by_channel() -> tuple[pa.Table, str]:
    """
    Returns
    -------
    table      : ["Channel", "total_duration_sec"]
    chart_type : "bar"
    """
    con = get_connection()
    table = con.execute("""
    SELECT
        "Channel",
        ROUND(
            (
                (
                    CAST(SPLIT_PART(CAST("Uploaded Duration (hh:mm:ss)"  AS VARCHAR), ':', 1) AS INTEGER) * 3600 +
                    CAST(SPLIT_PART(CAST("Uploaded Duration (hh:mm:ss)"  AS VARCHAR), ':', 2) AS INTEGER) * 60   +
                    CAST(SPLIT_PART(CAST("Uploaded Duration (hh:mm:ss)"  AS VARCHAR), ':', 3) AS INTEGER)
                ) +
                (
                    CAST(SPLIT_PART(CAST("Created Duration (hh:mm:ss)"   AS VARCHAR), ':', 1) AS INTEGER) * 3600 +
                    CAST(SPLIT_PART(CAST("Created Duration (hh:mm:ss)"   AS VARCHAR), ':', 2) AS INTEGER) * 60   +
                    CAST(SPLIT_PART(CAST("Created Duration (hh:mm:ss)"   AS VARCHAR), ':', 3) AS INTEGER)
                ) +
                (
                    CAST(SPLIT_PART(CAST("Published Duration (hh:mm:ss)" AS VARCHAR), ':', 1) AS INTEGER) * 3600 +
                    CAST(SPLIT_PART(CAST("Published Duration (hh:mm:ss)" AS VARCHAR), ':', 2) AS INTEGER) * 60   +
                    CAST(SPLIT_PART(CAST("Published Duration (hh:mm:ss)" AS VARCHAR), ':', 3) AS INTEGER)
                )
            ) / 3600.0,   -- ✅ Convert seconds → hours
        2) AS "total_duration_hours"
    FROM raw_channel_summary
    ORDER BY "total_duration_hours" DESC
""").arrow()
    con.close()
    return table, "bar"


# ---------------------------------------------------------------------------
# Plot 4 — Created vs Published Efficiency
# ---------------------------------------------------------------------------
def get_created_vs_published_efficiency() -> tuple[pa.Table, str]:
    """
    Returns
    -------
    table      : ["Channel", "efficiency_pct"]
    chart_type : "line"
    """
    con = get_connection()
    table = con.execute(f"""
        SELECT
            "Channel",
            ROUND(
                100.0 * "Published Count" / NULLIF("Created Count", 0),
                2
            ) AS "efficiency_pct"
        FROM {TABLE}
        ORDER BY "efficiency_pct" DESC NULLS LAST
    """).arrow()
    con.close()
    return table, "line"


# ---------------------------------------------------------------------------
# Plot 5 — Zero Publish Channels
# ---------------------------------------------------------------------------
def get_zero_publish_channels_detail() -> tuple[pa.Table, str]:
    """
    Returns
    -------
    table      : ["Channel", "Created Count"]
    chart_type : "bar"
    """
    con = get_connection()
    table = con.execute(f"""
        SELECT
            "Channel",
            "Created Count"
        FROM {TABLE}
        WHERE "Published Count" = 0
        ORDER BY "Created Count" DESC
    """).arrow()
    con.close()
    return table, "bar"