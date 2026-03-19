"""
Plots for CLIENT_1_combined_data.csv
"""

import pyarrow as pa
from database import get_connection


# ---------------------------------------------------------------------------
# Plot 1 — Publish Rate by Channel
# ---------------------------------------------------------------------------

def get_publish_rate_by_channel() -> tuple[pa.Table, str]:
    """
    Plot: Publish Rate (%) per channel

    Returns
    -------
    table      : ["Channel", "publish_rate"]
    chart_type : "bar"
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "Channel",
            ROUND(
                100.0 * "Published Count" / NULLIF("Created Count", 0),
                2
            ) AS "publish_rate"
        FROM client_1_combined_data
        ORDER BY "publish_rate" DESC NULLS LAST
    """).arrow()
    con.close()
    return table, "bar"


# ---------------------------------------------------------------------------
# Plot 2 — Uploaded vs Created vs Published Counts
# ---------------------------------------------------------------------------

def get_content_flow_counts() -> tuple[pa.Table, str]:
    """
    Plot: Content pipeline comparison

    Returns
    -------
    table      : ["Channel", "Uploaded", "Created", "Published"]
    chart_type : "bar"
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "Channel",
            "Uploaded Count"  AS "Uploaded",
            "Created Count"   AS "Created",
            "Published Count" AS "Published"
        FROM client_1_combined_data
    """).arrow()
    con.close()
    return table, "bar"


# ---------------------------------------------------------------------------
# Plot 3 — Total Duration by Channel
# ---------------------------------------------------------------------------

def get_total_duration_by_channel() -> tuple[pa.Table, str]:
    """
    Plot: Total duration (Uploaded + Created + Published)

    Returns
    -------
    table      : ["Channel", "total_duration_sec"]
    chart_type : "bar"
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "Channel",
            (
                EXTRACT(EPOCH FROM CAST("Uploaded Duration (hh:mm:ss)" AS TIME)) +
                EXTRACT(EPOCH FROM CAST("Created Duration (hh:mm:ss)" AS TIME)) +
                EXTRACT(EPOCH FROM CAST("Published Duration (hh:mm:ss)" AS TIME))
            ) AS "total_duration_sec"
        FROM client_1_combined_data
        ORDER BY "total_duration_sec" DESC
    """).arrow()
    con.close()
    return table, "bar"


# ---------------------------------------------------------------------------
# Plot 4 — Created vs Published Efficiency
# ---------------------------------------------------------------------------

def get_created_vs_published_efficiency() -> tuple[pa.Table, str]:
    """
    Plot: Created vs Published efficiency per channel

    Returns
    -------
    table      : ["Channel", "efficiency_pct"]
    chart_type : "line"
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "Channel",
            ROUND(
                100.0 * "Published Count" / NULLIF("Created Count", 0),
                2
            ) AS "efficiency_pct"
        FROM client_1_combined_data
        ORDER BY "efficiency_pct" DESC NULLS LAST
    """).arrow()
    con.close()
    return table, "line"


# ---------------------------------------------------------------------------
# Plot 5 — Zero Publish Channels (Highlight)
# ---------------------------------------------------------------------------

def get_zero_publish_channels_detail() -> tuple[pa.Table, str]:
    """
    Plot: Channels with zero published content

    Returns
    -------
    table      : ["Channel", "Created Count"]
    chart_type : "bar"
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "Channel",
            "Created Count"
        FROM client_1_combined_data
        WHERE "Published Count" = 0
        ORDER BY "Created Count" DESC
    """).arrow()
    con.close()
    return table, "bar"
