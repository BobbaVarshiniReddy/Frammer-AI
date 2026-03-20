"""
plots_platform.py
"""

import pyarrow as pa
from database import get_connection


PLATFORMS = [
    "Facebook", "Instagram", "Linkedin",
    "Reels", "Shorts", "X", "Youtube", "Threads",
]


# ---------------------------------------------------------------------------
# Plot 1 — Published Count per Platform (Bar)
# ---------------------------------------------------------------------------

def get_published_count_per_platform() -> tuple[pa.Table, str]:
    """
    Plot 1 (KPI-14): Bar – total published video count for each platform,
    sorted descending.

    Returns
    -------
    table      : columns ["platform", "total_published"]
    chart_type : "bar"
    """
    con = get_connection()
    union_sql = "\n            UNION ALL ".join(
        f"SELECT '{p}' AS platform, SUM(\"{p}\") AS total_published FROM raw_channels"
        for p in PLATFORMS
    )
    table = con.execute(f"""
        SELECT platform, total_published
        FROM (
            {union_sql}
        )
        ORDER BY total_published DESC
    """).arrow()
    con.close()
    return [[table, "bar"]]


# ---------------------------------------------------------------------------
# Plot 2 — Platform Publish Distribution % (Bar)
# ---------------------------------------------------------------------------

def get_platform_distribution_pct() -> tuple[pa.Table, str]:
    """
    Plot 2 (KPI-14): Bar – each platform's share of total published
    videos across all platforms, sorted descending.

    Returns
    -------
    table      : columns ["platform", "distribution_pct"]
    chart_type : "bar"
    """
    con = get_connection()
    union_sql = "\n            UNION ALL ".join(
        f"SELECT '{p}' AS platform, SUM(\"{p}\") AS total FROM raw_channel_platform"
        for p in PLATFORMS
    )
    table = con.execute(f"""
        WITH raw_channel_platform AS (
            {union_sql}
        ),
        grand_total AS (
            SELECT SUM(total) AS grand FROM raw_channel_platform
        )
        SELECT
            pt.platform,
            ROUND(100.0 * pt.total / NULLIF(gt.grand, 0), 2) AS "distribution_pct"
        FROM raw_channel_platform
        CROSS JOIN grand_total gt
        ORDER BY "distribution_pct" DESC
    """).arrow()
    con.close()
    return [[table, "bar"]]


# ---------------------------------------------------------------------------
# Plot 3 — Zero-Publish vs Active Channels (Bar)
# ---------------------------------------------------------------------------

def get_zero_vs_active_channels() -> tuple[pa.Table, str]:
    """
    Plot 3 (KPI-08): Bar – count of channels split into
    'zero_publish' (all platforms = 0) vs 'active'.

    Returns
    -------
    table      : columns ["category", "channel_count"]
    chart_type : "bar"
    """
    sum_expr = " + ".join(f'COALESCE("{p}", 0)' for p in PLATFORMS)
    con = get_connection()
    table = con.execute(f"""
        WITH classified AS (
            SELECT
                CASE WHEN ({sum_expr}) = 0
                    THEN 'zero_publish'
                    ELSE 'active'
                END AS category
            FROM raw_channel_platform
        )
        SELECT
            category,
            COUNT(*) AS "channel_count"
        FROM raw_channel_platform
        GROUP BY category
        ORDER BY category = 'zero_publish' DESC
    """).arrow()
    con.close()
    return [[table, "bar"]]


# ---------------------------------------------------------------------------
# Plot 4 — Per-Channel Total Publishes, Top N Channels (Bar)
# ---------------------------------------------------------------------------

def get_top_channels_by_publishes(top_n: int = 10) -> tuple[pa.Table, str]:
    """
    Plot 4: Bar – top N channels ranked by total publishes across all
    platforms, excluding zero-publish channels.

    Parameters
    ----------
    top_n : int, default 10

    Returns
    -------
    table      : columns ["Channels", "total_published"]
    chart_type : "bar"
    """
    sum_expr = " + ".join(f'COALESCE("{p}", 0)' for p in PLATFORMS)
    con = get_connection()
    table = con.execute(f"""
        SELECT
            "Channels"                      AS "Channels",
            ({sum_expr})                    AS "total_published"
        FROM raw_channel_platform
        WHERE ({sum_expr}) > 0
        ORDER BY "total_published" DESC
        LIMIT {top_n}
    """).arrow()
    con.close()
    return [[table, "bar"]]


# ---------------------------------------------------------------------------
# Plot 5 — Platform Activity Heatmap (per Channel × Platform)
# ---------------------------------------------------------------------------

def get_channel_platform_heatmap() -> tuple[pa.Table, str]:
    """
    Plot 5: Heatmap – published count for every (Channel, Platform)
    combination in long format.

    Returns
    -------
    table      : columns ["Channels", "platform", "published_count"]
    chart_type : "heatmap"
    """
    union_sql = "\n        UNION ALL ".join(
        f"SELECT \"Channels\", '{p}' AS platform, COALESCE(\"{p}\", 0) AS published_count FROM raw_channels"
        for p in PLATFORMS
    )
    con = get_connection()
    table = con.execute(f"""
        SELECT "Channels", platform, published_count
        FROM (
            {union_sql}
        )
        ORDER BY "Channels", platform
    """).arrow()
    con.close()
    return [[table, "heatmap"]]


# ---------------------------------------------------------------------------
# Plot 6 — Platform Diversity per Channel (Line)
# ---------------------------------------------------------------------------

def get_platform_diversity_per_channel() -> tuple[pa.Table, str]:
    """
    Plot 6: Line – number of distinct platforms (with at least 1 publish)
    used by each channel, sorted descending.
    Highlights channels that spread content across many platforms vs
    those that concentrate on one.

    Returns
    -------
    table      : columns ["Channels", "platforms_used"]
    chart_type : "line"
    """
    indicator_exprs = " + ".join(
        f'CASE WHEN COALESCE("{p}", 0) > 0 THEN 1 ELSE 0 END'
        for p in PLATFORMS
    )
    con = get_connection()
    table = con.execute(f"""
        SELECT
            "Channels"                          AS "Channels",
            ({indicator_exprs})                 AS "platforms_used"
        FROM raw_channel_platform
        ORDER BY "platforms_used" DESC, "Channels"
    """).arrow()
    con.close()
    return table, "line"


# ---------------------------------------------------------------------------
# Plot 7 — Channel Publish Share per Platform (Line)
# ---------------------------------------------------------------------------

def get_channel_publish_share_per_platform(platform: str = "Youtube") -> tuple[pa.Table, str]:
    """
    Plot 7: Line – each channel's percentage share of a single
    platform's total publishes, sorted descending.
    Useful for spotting dominant channels on a given platform.

    Parameters
    ----------
    platform : str, default "Youtube"
        One of: Facebook, Instagram, Linkedin, Reels, Shorts, X, Youtube, Threads

    Returns
    -------
    table      : columns ["Channels", "share_pct"]
    chart_type : "line"
    """
    con = get_connection()
    table = con.execute(f"""
        WITH total AS (
            SELECT SUM("{platform}") AS grand FROM raw_channels
        )
        SELECT
            "Channels",
            ROUND(100.0 * COALESCE("{platform}", 0) / NULLIF(t.grand, 0), 2) AS "share_pct"
        FROM raw_channel_platform
        CROSS JOIN total t
        WHERE COALESCE("{platform}", 0) > 0
        ORDER BY "share_pct" DESC
    """).arrow()
    con.close()
    return table, "line"

