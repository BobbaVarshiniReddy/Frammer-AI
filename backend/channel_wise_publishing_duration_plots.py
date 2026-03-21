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
    ✅ Fix: VARCHAR SPLIT_PART approach — handles >24h values
    DuckDB cannot cast TIME > 23:59:59 or use INTERVAL on duration columns
    """
    return f"""(
        CAST(SPLIT_PART(CAST({col} AS VARCHAR), ':', 1) AS INTEGER) * 3600 +
        CAST(SPLIT_PART(CAST({col} AS VARCHAR), ':', 2) AS INTEGER) * 60   +
        CAST(SPLIT_PART(CAST({col} AS VARCHAR), ':', 3) AS INTEGER)
    )"""


# ---------------------------------------------------------------------------
# Plot 1 — Platform Duration Distribution
# ---------------------------------------------------------------------------
def get_platform_duration_distribution() -> list:
    """
    Returns
    -------
    table      : columns ["Platform", "total_seconds", "share_pct"]
    chart_type : "bar"
    """
    con = get_connection()

    union_parts = []
    for col in PLATFORM_COLS:
        platform_name = col.replace(" Duration", "")
        sec_expr      = parse_duration_to_seconds(f'"{col}"')
        union_parts.append(f"""
            SELECT
                '{platform_name}' AS "Platform",
                {sec_expr}        AS "seconds"
            FROM raw_channel_duration
        """)

    union_sql = "\nUNION ALL\n".join(union_parts)

    table = con.execute(f"""
        WITH all_platforms AS (
            {union_sql}
        )
        SELECT
            "Platform",
            ROUND(SUM("seconds"), 2)                               AS "total_seconds",
            ROUND(
                100.0 * SUM("seconds") / SUM(SUM("seconds")) OVER (),
                2
            )                                                      AS "share_pct"
        FROM all_platforms
        GROUP BY "Platform"
        ORDER BY "share_pct" DESC
    """).arrow()

    con.close()
    return [[table, "bar"]]


# ---------------------------------------------------------------------------
# Plot 2 — Absolute Duration by Platform
# ---------------------------------------------------------------------------
def get_absolute_duration_by_platform() -> list:
    """
    Returns
    -------
    table      : columns ["Platform", "total_seconds"]
    chart_type : "bar"
    """
    con = get_connection()

    union_parts = []
    for col in PLATFORM_COLS:
        platform_name = col.replace(" Duration", "")
        sec_expr      = parse_duration_to_seconds(f'"{col}"')
        union_parts.append(f"""
            SELECT
                '{platform_name}' AS "Platform",
                {sec_expr}        AS "seconds"
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
# ---------------------------------------------------------------------------
def get_channel_total_duration() -> list:
    """
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
            ROUND(({total_expr}), 2) AS "total_seconds"
        FROM raw_channel_duration
        WHERE ({total_expr}) > 0
        ORDER BY "total_seconds" DESC
    """).arrow()

    con.close()
    return [[table, "bar"]]


# ---------------------------------------------------------------------------
# Plot 4 — Channel × Platform Duration Heatmap
# ✅ Fix: "Channels" first, "Platform" second for correct heatmap rendering
# ---------------------------------------------------------------------------
def get_channel_platform_duration_heatmap() -> list:
    """
    Returns
    -------
    table      : columns ["Channels", "Platform", "seconds"]
    chart_type : "heatmap"
    """
    con = get_connection()

    union_parts = []
    for col in PLATFORM_COLS:
        platform_name = col.replace(" Duration", "")
        sec_expr      = parse_duration_to_seconds(f'"{col}"')
        union_parts.append(f"""
            SELECT
                "Channels"           AS "Channels",
                '{platform_name}'    AS "Platform",
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
# Plot 5 — Top N Channels by Duration
# ---------------------------------------------------------------------------
def get_top_channels_by_duration(top_n: int = 7) -> list:
    """
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
            ROUND(({total_expr}), 2) AS "total_seconds"
        FROM raw_channel_duration
        WHERE ({total_expr}) > 0
        ORDER BY "total_seconds" DESC
        LIMIT {top_n}
    """).arrow()

    con.close()
    return [[table, "bar"]]


# ---------------------------------------------------------------------------
# Plot 6 — Platform Share per Channel
# ✅ Fix: "Channels" first, "Platform" second for small_multiples
# ---------------------------------------------------------------------------
def get_platform_share_per_channel() -> list:
    """
    Returns
    -------
    table      : columns ["Channels", "Platform", "share_pct"]
    chart_type : "small_multiples"
    """
    con = get_connection()

    union_parts = []
    for col in PLATFORM_COLS:
        platform_name = col.replace(" Duration", "")
        sec_expr      = parse_duration_to_seconds(f'"{col}"')
        union_parts.append(f"""
            SELECT
                "Channels"        AS "Channels",
                '{platform_name}' AS "Platform",
                {sec_expr}        AS "seconds"
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
    return [[table, "small_multiples"]]


def get_platform_coverage_heatmap() -> list:
    """
    Returns
    -------
    table      : columns ["Platform", "active_channel_pct"]
    chart_type : "bar"
    """
    select_exprs = []
    for col in PLATFORM_COLS:
        platform_name = col.replace(" Duration", "")
        sec_expr      = parse_duration_to_seconds(f'"{col}"')
        select_exprs.append(
            f'ROUND(100.0 * SUM(CASE WHEN {sec_expr} > 0 THEN 1 ELSE 0 END) / COUNT(*), 2) AS "{platform_name}"'
        )

    con = get_connection()
    wide = con.execute(f"""
        SELECT {', '.join(select_exprs)}
        FROM raw_channel_duration
    """).fetchone()
    con.close()

    platform_names = [col.replace(" Duration", "") for col in PLATFORM_COLS]

    table = pa.table({
        "Platform"          : pa.array(platform_names, type=pa.string()),
        "active_channel_pct": pa.array(list(wide),     type=pa.float64()),
    })
    return [[table, "bar"]]   # ✅ Changed from "heatmap" to "bar"