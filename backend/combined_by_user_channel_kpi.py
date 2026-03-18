import pyarrow as pa
from database import get_connection


def get_kpi18_channel_user_cross_dimension() -> tuple[pa.Table, str]:
    """
    KPI-18: For every (Channel, User) row, the publish rate —
    what percentage of that user's created videos in that channel
    were eventually published.
    Formula: (Row Published Count) ÷ (Row Created Count) × 100

    Returns
    -------
    table       : columns ["Channel", "User", "published_count",
                            "created_count", "publish_rate_pct"]
                  Rows sorted by publish_rate_pct DESC.
    description : str
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "Channel"                                                           AS "Channel",
            "User"                                                              AS "User",
            "Published Count"                                                   AS "published_count",
            "Created Count"                                                     AS "created_count",
            ROUND(
                100.0 * "Published Count" / NULLIF("Created Count", 0),
                2
            )                                                                   AS "publish_rate_pct"
        FROM raw_channel_user
        ORDER BY "publish_rate_pct" DESC
    """).arrow()
    con.close()
    return table, "KPI-18 – Channel × User Cross-Dimension Metric (%)"


def get_kpi16_user_publish_rate_within_channel(channel: str) -> tuple[pa.Table, str]:
    """
    KPI-16 (channel-level): For a specific channel, the publish rate
    per user — how many of each user's created videos were published.
    Formula: User Published ÷ User Created × 100
             (filtered to the given channel)

    Parameters
    ----------
    channel : str
        Channel name to filter on (e.g. "A", "B", ...).

    Returns
    -------
    table       : columns ["User", "published_count", "created_count",
                            "publish_rate_pct"]
                  Rows sorted by publish_rate_pct DESC.
    description : str
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "User"                                                              AS "User",
            SUM("Published Count")                                              AS "published_count",
            SUM("Created Count")                                                AS "created_count",
            ROUND(
                100.0 * SUM("Published Count") / NULLIF(SUM("Created Count"), 0),
                2
            )                                                                   AS "publish_rate_pct"
        FROM raw_channel_user
        WHERE TRIM("Channel") = ?
        GROUP BY "User"
        ORDER BY "publish_rate_pct" DESC
    """, [channel]).arrow()
    con.close()
    return table, f"KPI-16 – User Publish Rate within Channel '{channel}' (%)"


def get_kpi06_publish_rate_by_channel() -> tuple[pa.Table, str]:
    """
    KPI-06 (user drill-down): For every channel, the overall publish
    rate — summing all users' published and created counts.
    Formula: SUM(Published per channel) ÷ SUM(Created per channel) × 100

    Returns
    -------
    table       : columns ["Channel", "total_published", "total_created",
                            "publish_rate_pct"]
                  Rows sorted by publish_rate_pct DESC.
    description : str
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            TRIM("Channel")                                                     AS "Channel",
            SUM("Published Count")                                              AS "total_published",
            SUM("Created Count")                                                AS "total_created",
            ROUND(
                100.0 * SUM("Published Count") / NULLIF(SUM("Created Count"), 0),
                2
            )                                                                   AS "publish_rate_pct"
        FROM raw_channel_user
        GROUP BY TRIM("Channel")
        ORDER BY "publish_rate_pct" DESC
    """).arrow()
    con.close()
    return table, "KPI-06 – Publish Rate by Channel (user detail) (%)"


def get_kpi17_user_upload_volume_by_channel(channel: str) -> tuple[pa.Table, str]:
    """
    KPI-17 (channel-scoped): Within a specific channel, rank users by
    their uploaded video count — highest uploaders first.
    Formula: ORDER BY Uploaded Count DESC within Channel

    Parameters
    ----------
    channel : str
        Channel name to filter on (e.g. "A", "B", ...).

    Returns
    -------
    table       : columns ["User", "uploaded_count", "created_count",
                            "published_count"]
                  Rows sorted by uploaded_count DESC.
    description : str
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "User"                  AS "User",
            "Uploaded Count"        AS "uploaded_count",
            "Created Count"         AS "created_count",
            "Published Count"       AS "published_count"
        FROM raw_channel_user
        WHERE TRIM("Channel") = ?
        ORDER BY "uploaded_count" DESC
    """, [channel]).arrow()
    con.close()
    return table, f"KPI-17 – User Upload Volume by Channel '{channel}'"


def get_kpi17_user_upload_volume_all_channels() -> tuple[pa.Table, str]:
    """
    KPI-17 (all channels): Aggregate uploaded count per user across all
    channels, ranked descending — a global uploader leaderboard.
    Formula: SUM(Uploaded Count) per User, ORDER BY DESC

    Returns
    -------
    table       : columns ["User", "total_uploaded", "total_created",
                            "total_published"]
                  Rows sorted by total_uploaded DESC.
    description : str
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "User"                          AS "User",
            SUM("Uploaded Count")           AS "total_uploaded",
            SUM("Created Count")            AS "total_created",
            SUM("Published Count")          AS "total_published"
        FROM raw_channel_user
        GROUP BY "User"
        ORDER BY "total_uploaded" DESC
    """).arrow()
    con.close()
    return table, "KPI-17 – User Upload Volume (all channels)"
