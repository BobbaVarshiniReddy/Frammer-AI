"""
plots_channel_user.py
"""

import pyarrow as pa
from database import get_connection


# ---------------------------------------------------------------------------
# Plot 1 — Publish Rate per Channel (Bar)  [KPI-06]
# ---------------------------------------------------------------------------

def get_publish_rate_per_channel() -> tuple[pa.Table, str]:
    """
    Plot 1 (KPI-06): Bar – overall publish rate (%) for each channel,
    sorted descending.

    Returns
    -------
    table      : columns ["Channel", "publish_rate_pct"]
    chart_type : "bar"
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            TRIM("Channel")                                                 AS "Channel",
            ROUND(
                100.0 * SUM("Published Count") / NULLIF(SUM("Created Count"), 0),
                2
            )                                                               AS "publish_rate_pct"
        FROM raw_channel_user
        GROUP BY TRIM("Channel")
        ORDER BY "publish_rate_pct" DESC
    """).arrow()
    con.close()
    return table, "bar"


# ---------------------------------------------------------------------------
# Plot 2 — Upload / Created / Published Volume per Channel (Bar)  [KPI-17]
# ---------------------------------------------------------------------------

def get_volume_per_channel() -> tuple[pa.Table, str]:
    """
    Plot 2 (KPI-17): Grouped bar – total Uploaded, Created, and Published
    counts per channel, so the pipeline funnel is visible side-by-side.

    Returns
    -------
    table      : columns ["Channel", "metric", "count"]
                 Long format (3 rows per channel).
    chart_type : "bar"
    """
    con = get_connection()
    wide = con.execute("""
        SELECT
            TRIM("Channel")             AS "Channel",
            SUM("Uploaded Count")       AS "total_uploaded",
            SUM("Created Count")        AS "total_created",
            SUM("Published Count")      AS "total_published"
        FROM raw_channel_user
        GROUP BY TRIM("Channel")
        ORDER BY "total_uploaded" DESC
    """).fetchall()
    con.close()

    channels, metrics, counts = [], [], []
    for row in wide:
        channel, uploaded, created, published = row
        for metric, val in [
            ("Uploaded", uploaded),
            ("Created",  created),
            ("Published", published),
        ]:
            channels.append(channel)
            metrics.append(metric)
            counts.append(val)

    table = pa.table({
        "Channel": pa.array(channels, type=pa.string()),
        "metric":  pa.array(metrics,  type=pa.string()),
        "count":   pa.array(counts,   type=pa.int64()),
    })
    return table, "bar"


# ---------------------------------------------------------------------------
# Plot 3 — Top N Uploaders Across All Channels (Bar)  [KPI-17]
# ---------------------------------------------------------------------------

def get_top_uploaders_all_channels(top_n: int = 10) -> tuple[pa.Table, str]:
    """
    Plot 3 (KPI-17): Bar – top N users by total uploaded count across
    all channels, excluding zero-upload users.

    Parameters
    ----------
    top_n : int, default 10

    Returns
    -------
    table      : columns ["User", "total_uploaded"]
    chart_type : "bar"
    """
    con = get_connection()
    table = con.execute(f"""
        SELECT
            "User"                      AS "User",
            SUM("Uploaded Count")       AS "total_uploaded"
        FROM raw_channel_user
        GROUP BY "User"
        HAVING SUM("Uploaded Count") > 0
        ORDER BY "total_uploaded" DESC
        LIMIT {top_n}
    """).arrow()
    con.close()
    return table, "bar"


# ---------------------------------------------------------------------------
# Plot 4 — User Publish Rate Across All Channels (Line)  [KPI-16]
# ---------------------------------------------------------------------------

def get_user_publish_rate_all_channels(top_n: int = 15) -> tuple[pa.Table, str]:
    """
    Plot 4 (KPI-16): Line – publish rate (%) per user aggregated across
    all channels, top N sorted descending.
    Excludes users with zero created count.

    Parameters
    ----------
    top_n : int, default 15

    Returns
    -------
    table      : columns ["User", "publish_rate_pct"]
    chart_type : "line"
    """
    con = get_connection()
    table = con.execute(f"""
        SELECT
            "User"                                                          AS "User",
            ROUND(
                100.0 * SUM("Published Count") / NULLIF(SUM("Created Count"), 0),
                2
            )                                                               AS "publish_rate_pct"
        FROM raw_channel_user
        GROUP BY "User"
        HAVING SUM("Created Count") > 0
        ORDER BY "publish_rate_pct" DESC
        LIMIT {top_n}
    """).arrow()
    con.close()
    return table, "line"


# ---------------------------------------------------------------------------
# Plot 5 — Channel × User Publish Rate Heatmap  [KPI-18]
# ---------------------------------------------------------------------------

def get_channel_user_publish_rate_heatmap() -> tuple[pa.Table, str]:
    """
    Plot 5 (KPI-18): Heatmap – publish rate (%) for every
    (Channel, User) combination in long format.
    Cells with no created videos are excluded.

    Returns
    -------
    table      : columns ["Channel", "User", "publish_rate_pct"]
    chart_type : "heatmap"
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            TRIM("Channel")                                                 AS "Channel",
            "User"                                                          AS "User",
            ROUND(
                100.0 * "Published Count" / NULLIF("Created Count", 0),
                2
            )                                                               AS "publish_rate_pct"
        FROM raw_channel_user
        WHERE "Created Count" > 0
        ORDER BY "Channel", "User"
    """).arrow()
    con.close()
    return table, "heatmap"


# ---------------------------------------------------------------------------
# Plot 6 — Upload Volume per User within a Channel (Bar)  [KPI-17]
# ---------------------------------------------------------------------------

def get_upload_volume_per_user_in_channel(channel: str) -> tuple[pa.Table, str]:
    """
    Plot 6 (KPI-17): Bar – uploaded count per user within a specific
    channel, sorted descending.

    Parameters
    ----------
    channel : str
        Channel name to filter on (e.g. "A", "B", ...).

    Returns
    -------
    table      : columns ["User", "uploaded_count"]
    chart_type : "bar"
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "User"              AS "User",
            "Uploaded Count"    AS "uploaded_count"
        FROM raw_channel_user
        WHERE TRIM("Channel") = ?
        ORDER BY "uploaded_count" DESC
    """, [channel]).arrow()
    con.close()
    return table, "bar"


# ---------------------------------------------------------------------------
# Plot 7 — User Publish Rate within a Channel (Line)  [KPI-16]
# ---------------------------------------------------------------------------

def get_user_publish_rate_in_channel(channel: str) -> tuple[pa.Table, str]:
    """
    Plot 7 (KPI-16): Line – publish rate (%) per user within a specific
    channel, sorted descending.
    Excludes users with zero created count in that channel.

    Parameters
    ----------
    channel : str
        Channel name to filter on (e.g. "A", "B", ...).

    Returns
    -------
    table      : columns ["User", "publish_rate_pct"]
    chart_type : "line"
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "User"                                                          AS "User",
            ROUND(
                100.0 * SUM("Published Count") / NULLIF(SUM("Created Count"), 0),
                2
            )                                                               AS "publish_rate_pct"
        FROM raw_channel_user
        WHERE TRIM("Channel") = ?
          AND "Created Count" > 0
        GROUP BY "User"
        ORDER BY "publish_rate_pct" DESC
    """, [channel]).arrow()
    con.close()
    return table, "line"


# ---------------------------------------------------------------------------
# Plot 8 — Pipeline Funnel per User: Uploaded → Created → Published (Line)
# ---------------------------------------------------------------------------

def get_pipeline_funnel_per_user(top_n: int = 10) -> tuple[pa.Table, str]:
    """
    Plot 8: Line – for the top N uploaders, shows all three pipeline
    stages (Uploaded, Created, Published) as separate series.
    Useful for spotting where drop-off happens per user.

    Parameters
    ----------
    top_n : int, default 10

    Returns
    -------
    table      : columns ["User", "metric", "count"]
                 Long format (3 rows per user).
    chart_type : "line"
    """
    con = get_connection()
    wide = con.execute(f"""
        SELECT
            "User"                      AS "User",
            SUM("Uploaded Count")       AS "total_uploaded",
            SUM("Created Count")        AS "total_created",
            SUM("Published Count")      AS "total_published"
        FROM raw_channel_user
        GROUP BY "User"
        ORDER BY "total_uploaded" DESC
        LIMIT {top_n}
    """).fetchall()
    con.close()

    users, metrics, counts = [], [], []
    for row in wide:
        user, uploaded, created, published = row
        for metric, val in [
            ("Uploaded",  uploaded),
            ("Created",   created),
            ("Published", published),
        ]:
            users.append(user)
            metrics.append(metric)
            counts.append(val)

    table = pa.table({
        "User":   pa.array(users,   type=pa.string()),
        "metric": pa.array(metrics, type=pa.string()),
        "count":  pa.array(counts,  type=pa.int64()),
    })
    return table, "line"