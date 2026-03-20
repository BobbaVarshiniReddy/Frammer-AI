"""
kpis_duration.py
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
    Uses epoch() on an interval cast.
    """
    return f"epoch(CAST({col} AS INTERVAL))"


# ---------------------------------------------------------------------------
# KPI-14 (duration): Platform Publish Duration Distribution
# Formula: SUM(parse_hrs(Platform Duration)) ÷ SUM(All Platform Durations) × 100
# ---------------------------------------------------------------------------

def get_kpi14_platform_duration_distribution() -> tuple[pa.Table, str]:
    """
    KPI 14 (duration): For every platform, the percentage share of total
    published duration attributed to that platform.

    Returns
    -------
    table       : columns ["Platform", "total_seconds", "share_pct"]
                  Rows sorted by share_pct DESC.
    chart_type  : "bar"
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
            SUM("seconds")                                              AS "total_seconds",
            ROUND(
                100.0 * SUM("seconds") / SUM(SUM("seconds")) OVER (),
                2
            )                                                           AS "share_pct"
        FROM raw_channel_duration
        GROUP BY "Platform"
        ORDER BY "share_pct" DESC
    """).arrow()

    con.close()
    return [[table, "bar"]]


# ---------------------------------------------------------------------------
# KPI-05 (partial): Published Duration by Platform
# Formula: SUM(parse_hrs(Platform Duration Column))
# ---------------------------------------------------------------------------

def get_kpi05_published_duration_by_platform() -> tuple[pa.Table, str]:
    """
    KPI 05 (partial): Total published duration per platform in seconds.

    Returns
    -------
    table       : columns ["Platform", "total_seconds"]
                  Rows sorted by total_seconds DESC.
    chart_type  : "bar"
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
            SUM("seconds") AS "total_seconds"
        FROM raw_channel_duration
        GROUP BY "Platform"
        ORDER BY "total_seconds" DESC
    """).arrow()

    con.close()
    return [[table, "bar"]]


# ---------------------------------------------------------------------------
# BONUS: Channel-wise total duration across ALL platforms
# ---------------------------------------------------------------------------

def get_channel_total_duration() -> tuple[pa.Table, str]:
    """
    For each channel, the total published duration (in seconds) summed
    across all platforms. Sorted by total_seconds DESC.

    Returns
    -------
    table       : columns ["Channels", "total_seconds"]
    chart_type  : "bar"
    """
    con = get_connection()

    total_expr = " +\n            ".join(
        parse_duration_to_seconds(f'"{col}"') for col in PLATFORM_COLS
    )

    table = con.execute(f"""
        SELECT
            "Channels",
            (
                {total_expr}
            ) AS "total_seconds"
        FROM raw_channel_duration
        ORDER BY "total_seconds" DESC
    """).arrow()

    con.close()
    return [[table, "bar"]]


# ---------------------------------------------------------------------------
# BONUS: Channel × Platform duration matrix (seconds)
# ---------------------------------------------------------------------------

def get_channel_platform_duration_matrix() -> tuple[pa.Table, str]:
    """
    Returns a wide table with one row per channel and one column per platform
    (values in seconds), plus a grand-total column.

    Returns
    -------
    table       : columns ["Channels", <platform>..., "total_seconds"]
    chart_type  : "heatmap"
    """
    con = get_connection()

    col_exprs = []
    for col in PLATFORM_COLS:
        platform_name = col.replace(" Duration", "")
        sec_expr = parse_duration_to_seconds(f'"{col}"')
        col_exprs.append(f'{sec_expr} AS "{platform_name}"')

    total_expr = " + ".join(
        parse_duration_to_seconds(f'"{col}"') for col in PLATFORM_COLS
    )

    table = con.execute(f"""
        SELECT
            "Channels",
            {',\n            '.join(col_exprs)},
            ({total_expr}) AS "total_seconds"
        FROM raw_channel_duration
        ORDER BY "total_seconds" DESC
    """).arrow()

    con.close()
    return [[table, "heatmap"]]