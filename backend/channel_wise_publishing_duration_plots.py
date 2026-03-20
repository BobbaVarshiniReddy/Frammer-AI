"""
plots_duration.py
"""

import pyarrow as pa
from database import get_connection


PLATFORM_COLS = [
    "Facebook Duration",
    "Instagram Duration",
    "Linkedin Duration",
    "Reels Duration",
    "Shorts Duration",
    "X Duration",
    "Youtube Duration",
    "Threads Duration",
]


def parse_duration_to_seconds(col: str) -> str:
    """
    DuckDB-native HH:MM:SS → total seconds conversion.
    """
    return f"epoch(CAST({col} AS INTERVAL))"


# ---------------------------------------------------------------------------
# Plot 1 — Platform Duration Distribution (KPI-14)
# Bar: each platform's % share of total duration
# ---------------------------------------------------------------------------

def get_platform_duration_distribution() -> tuple[pa.Table, str]:
    """
    Plot 1 (KPI-14): Bar – percentage share of total published duration
    per platform across all channels.

    Returns
    -------
    table      : columns ["Platform", "total_seconds", "share_pct"]
    chart_type : "bar"
    """
    con = get_connection()

    union_parts = []
    for col in PLATFORM_COLS:
        platform_name = col.replace(" Duration", "")
        sec_expr = parse_duration_to_seconds(f'"{col}"')
        union_parts.append(f"""
            SELECT
                '{platform_name}'   AS "Platform",
                {sec_expr}          AS "seconds"
            FROM raw_channel_duration
        """)

    union_sql = "\nUNION ALL\n".join(union_parts)

    table = con.execute(f"""
        WITH all_platforms AS (
            {union_sql}
        )
        SELECT
            "Platform",
            ROUND(SUM("seconds"), 2)                                    AS "total_seconds",
            ROUND(
                100.0 * SUM("seconds") / SUM(SUM("seconds")) OVER (),
                2
            )                                                           AS "share_pct"
        FROM all_platforms
        GROUP BY "Platform"
        ORDER BY "share_pct" DESC
    """).arrow()

    con.close()
    return [[table, "bar"]]


# ---------------------------------------------------------------------------
# Plot 2 — Absolute Duration by Platform (KPI-05)
# Bar: total seconds published per platform
# ---------------------------------------------------------------------------

def get_absolute_duration_by_platform() -> tuple[pa.Table, str]:
    """
    Plot 2 (KPI-05): Bar – total published duration in seconds per platform,
    sorted descending.

    Returns
    -------
    table      : columns ["Platform", "total_seconds"]
    chart_type : "bar"
    """
    con = get_connection()

    union_parts = []
    for col in PLATFORM_COLS:
        platform_name = col.replace(" Duration", "")
        sec_expr = parse_duration_to_seconds(f'"{col}"')
        union_parts.append(f"""
            SELECT
                '{platform_name}'   AS "Platform",
                {sec_expr}          AS "seconds"
            FROM raw_channel_duration
        """)

    union_sql = "\nUNION ALL\n".join(union_parts)

    table = con.execute(f"""
        WITH all_platforms AS (
            {union_sql}
        )
        SELECT
            "Platform",
            ROUND(SUM("seconds"), 2) AS "total_seconds"
        FROM all_platforms
        GROUP BY "Platform"
        ORDER BY "total_seconds" DESC
    """).arrow()

    con.close()
    return [[table, "bar"]]


# ---------------------------------------------------------------------------
# Plot 3 — Channel Total Duration
# Bar: grand total seconds per channel across all platforms
# ---------------------------------------------------------------------------

def get_channel_total_duration() -> tuple[pa.Table, str]:
    """
    Plot 3: Bar – total published duration (seconds) per channel,
    summed across all platforms. Sorted descending.
    Excludes channels with zero total duration.

    Returns
    -------
    table      : columns ["Channels", "total_seconds"]
    chart_type : "bar"
    """
    con = get_connection()

    total_expr = " +\n            ".join(
        parse_duration_to_seconds(f'"{col}"') for col in PLATFORM_COLS
    )

    table = con.execute(f"""
        SELECT
            "Channels",
            ROUND(
                (
                    {total_expr}
                ),
                2
            ) AS "total_seconds"
        FROM raw_channel_duration
        WHERE (
            {total_expr}
        ) > 0
        ORDER BY "total_seconds" DESC
    """).arrow()

    con.close()
    return [[table, "bar"]]


# ---------------------------------------------------------------------------
# Plot 4 — Channel × Platform Duration Heatmap
# Heatmap: seconds per channel per platform
# ---------------------------------------------------------------------------

def get_channel_platform_duration_heatmap() -> tuple[pa.Table, str]:
    """
    Plot 4: Heatmap – duration in seconds for each (Channel, Platform)
    combination, in long format suitable for a heatmap.

    Returns
    -------
    table      : columns ["Channels", "Platform", "seconds"]
    chart_type : "heatmap"
    """
    con = get_connection()

    union_parts = []
    for col in PLATFORM_COLS:
        platform_name = col.replace(" Duration", "")
        sec_expr = parse_duration_to_seconds(f'"{col}"')
        union_parts.append(f"""
            SELECT
                "Channels"          AS "Channels",
                '{platform_name}'   AS "Platform",
                ROUND({sec_expr}, 2) AS "seconds"
            FROM raw_channel_duration
        """)

    union_sql = "\nUNION ALL\n".join(union_parts)

    table = con.execute(f"""
        {union_sql}
        ORDER BY "Channels", "Platform"
    """).arrow()

    con.close()
    return [[table, "heatmap"]]


# ---------------------------------------------------------------------------
# Plot 5 — Top N Active Channels by Duration
# Line: top N channels with highest total duration
# ---------------------------------------------------------------------------

def get_top_channels_by_duration(top_n: int = 7) -> tuple[pa.Table, str]:
    """
    Plot 5: Line – top N channels ranked by total published duration
    across all platforms. Excludes zero-duration channels.

    Parameters
    ----------
    top_n : int, default 7

    Returns
    -------
    table      : columns ["Channels", "total_seconds"]
    chart_type : "line"
    """
    con = get_connection()

    total_expr = " +\n            ".join(
        parse_duration_to_seconds(f'"{col}"') for col in PLATFORM_COLS
    )

    table = con.execute(f"""
        SELECT
            "Channels",
            ROUND(
                (
                    {total_expr}
                ),
                2
            ) AS "total_seconds"
        FROM raw_channel_duration
        WHERE (
            {total_expr}
        ) > 0
        ORDER BY "total_seconds" DESC
        LIMIT {top_n}
    """).arrow()

    con.close()
    return [[table, "line"]]


# ---------------------------------------------------------------------------
# Plot 6 — Platform Duration Share per Channel (% breakdown)
# Line: for each active channel, % of their duration on each platform
# ---------------------------------------------------------------------------

def get_platform_share_per_channel() -> tuple[pa.Table, str]:
    """
    Plot 6: Line – for each active channel, the percentage of their total
    duration that was published on each platform.

    Returns
    -------
    table      : columns ["Channels", "Platform", "share_pct"]
    chart_type : "line"
    """
    con = get_connection()

    union_parts = []
    for col in PLATFORM_COLS:
        platform_name = col.replace(" Duration", "")
        sec_expr = parse_duration_to_seconds(f'"{col}"')
        union_parts.append(f"""
            SELECT
                "Channels"          AS "Channels",
                '{platform_name}'   AS "Platform",
                {sec_expr}          AS "seconds"
            FROM raw_channel_duration
        """)

    union_sql = "\nUNION ALL\n".join(union_parts)

    table = con.execute(f"""
        WITH long AS (
            {union_sql}
        ),
        channel_totals AS (
            SELECT
                "Channels",
                SUM("seconds") AS "channel_total"
            FROM long
            GROUP BY "Channels"
        )
        SELECT
            l."Channels",
            l."Platform",
            ROUND(
                100.0 * SUM(l."seconds") / NULLIF(ct."channel_total", 0),
                2
            ) AS "share_pct"
        FROM long l
        JOIN channel_totals ct ON l."Channels" = ct."Channels"
        WHERE ct."channel_total" > 0
        GROUP BY l."Channels", l."Platform", ct."channel_total"
        ORDER BY l."Channels", "share_pct" DESC
    """).arrow()

    con.close()
    return [[table, "line"]]


# ---------------------------------------------------------------------------
# Plot 7 — Data Completeness Heatmap (zero-duration check)
# Heatmap: % of channels with non-zero duration per platform
# ---------------------------------------------------------------------------

def get_platform_coverage_heatmap() -> tuple[pa.Table, str]:
    """
    Plot 7: Heatmap – for each platform, the percentage of channels
    that have a non-zero published duration (i.e. actually used it).

    Returns
    -------
    table      : columns ["Platform", "active_channel_pct"]
    chart_type : "heatmap"
    """
    con = get_connection()

    select_exprs = []
    for col in PLATFORM_COLS:
        platform_name = col.replace(" Duration", "")
        sec_expr = parse_duration_to_seconds(f'"{col}"')
        select_exprs.append(
            f"ROUND(100.0 * SUM(CASE WHEN {sec_expr} > 0 THEN 1 ELSE 0 END) / COUNT(*), 2) AS \"{platform_name}\""
        )

    con = get_connection()
    wide = con.execute(f"""
        SELECT {', '.join(select_exprs)}
        FROM raw_channel_duration
    """).fetchone()
    con.close()

    platform_names = [col.replace(" Duration", "") for col in PLATFORM_COLS]
    table = pa.table({
        "Platform":           pa.array(platform_names, type=pa.string()),
        "active_channel_pct": pa.array(list(wide),    type=pa.float64()),
    })
    return [[table, "heatmap"]]