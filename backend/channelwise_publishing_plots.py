"""
plots_platform.py
"""

import pyarrow as pa
from database import get_connection

# ⚠️ Check actual table name at http://127.0.0.1:8000/debug/tables
TABLE = "raw_channel_platform"

PLATFORMS = [
    "Facebook", "Instagram", "Linkedin",
    "Reels", "Shorts", "X", "Youtube", "Threads",
]


# ---------------------------------------------------------------------------
# Plot 1 — Published Count per Platform
# ---------------------------------------------------------------------------
def get_published_count_per_platform() -> list:
    con = get_connection()
    union_sql = "\nUNION ALL ".join(
        f"SELECT '{p}' AS \"platform\", SUM(\"{p}\") AS \"total_published\" FROM {TABLE}"
        for p in PLATFORMS
    )
    table = con.execute(f"""
        SELECT "platform", "total_published"
        FROM ({union_sql})
        ORDER BY "total_published" DESC
    """).arrow()
    con.close()
    return [[table, "bar"]]


# ---------------------------------------------------------------------------
# Plot 2 — Platform Publish Distribution %
# ---------------------------------------------------------------------------
def get_platform_distribution_pct() -> list:
    con = get_connection()
    union_sql = "\nUNION ALL ".join(
        f"SELECT '{p}' AS \"platform\", SUM(\"{p}\") AS \"total\" FROM {TABLE}"
        for p in PLATFORMS
    )
    table = con.execute(f"""
        WITH platform_totals AS (
            {union_sql}
        ),
        grand_total AS (
            SELECT SUM("total") AS grand FROM platform_totals
        )
        SELECT
            pt."platform",
            ROUND(100.0 * pt."total" / NULLIF(gt.grand, 0), 2) AS "distribution_pct"
        FROM platform_totals pt
        CROSS JOIN grand_total gt
        ORDER BY "distribution_pct" DESC
    """).arrow()
    con.close()
    return [[table, "bar"]]


# ---------------------------------------------------------------------------
# Plot 3 — Zero-Publish vs Active Channels
# ✅ Fix: was querying raw_channel_platform after defining CTE — now queries CTE
# ---------------------------------------------------------------------------
def get_zero_vs_active_channels() -> list:
    sum_expr = " + ".join(f'COALESCE("{p}", 0)' for p in PLATFORMS)
    con = get_connection()
    table = con.execute(f"""
        WITH classified AS (
            SELECT
                CASE WHEN ({sum_expr}) = 0
                    THEN 'Zero Publish'
                    ELSE 'Active'
                END AS "category"
            FROM {TABLE}
        )
        SELECT
            "category",
            COUNT(*) AS "channel_count"
        FROM classified                  -- ✅ Query the CTE not original table
        GROUP BY "category"
        ORDER BY "category" = 'Zero Publish' DESC
    """).arrow()
    con.close()
    return [[table, "bar"]]


# ---------------------------------------------------------------------------
# Plot 4 — Top N Channels by Publishes
# ---------------------------------------------------------------------------
def get_top_channels_by_publishes(top_n: int = 10) -> list:
    sum_expr = " + ".join(f'COALESCE("{p}", 0)' for p in PLATFORMS)
    con = get_connection()
    table = con.execute(f"""
        SELECT
            "Channels"      AS "Channels",
            ({sum_expr})    AS "total_published"
        FROM {TABLE}
        WHERE ({sum_expr}) > 0
        ORDER BY "total_published" DESC
        LIMIT {top_n}
    """).arrow()
    con.close()
    return [[table, "bar"]]


# ---------------------------------------------------------------------------
# Plot 5 — Channel × Platform Heatmap
# ✅ "Channels" first (Y-axis), "platform" last (X-axis) for CustomPlot
# ---------------------------------------------------------------------------
def get_channel_platform_heatmap() -> list:
    union_sql = "\nUNION ALL ".join(
        f"SELECT \"Channels\", '{p}' AS \"platform\", COALESCE(\"{p}\", 0) AS \"published_count\" FROM {TABLE}"
        for p in PLATFORMS
    )
    con = get_connection()
    table = con.execute(f"""
        SELECT "Channels", "platform", "published_count"
        FROM ({union_sql})
        ORDER BY "Channels", "platform"
    """).arrow()
    con.close()
    return [[table, "heatmap"]]


# ---------------------------------------------------------------------------
# Plot 6 — Platform Diversity per Channel
# ✅ Fix: return list not tuple, "bar" not "line"
# ---------------------------------------------------------------------------
def get_platform_diversity_per_channel() -> list:
    indicator_exprs = " + ".join(
        f'CASE WHEN COALESCE("{p}", 0) > 0 THEN 1 ELSE 0 END'
        for p in PLATFORMS
    )
    con = get_connection()
    table = con.execute(f"""
        SELECT
            "Channels"          AS "Channels",
            ({indicator_exprs}) AS "platforms_used"
        FROM {TABLE}
        WHERE ({indicator_exprs}) > 0
        ORDER BY "platforms_used" DESC, "Channels"
    """).arrow()
    con.close()
    return [[table, "bar"]]   # ✅ bar not line


# ---------------------------------------------------------------------------
# Plot 7 — Channel Publish Share per Platform
# ✅ Fix: return list, show ALL platforms not just Youtube
#         by aggregating share across all platforms per channel
# ---------------------------------------------------------------------------
def get_channel_publish_share_per_platform() -> list:
    """
    Shows each channel's % share across ALL platforms combined
    """
    sum_expr = " + ".join(f'COALESCE("{p}", 0)' for p in PLATFORMS)
    con = get_connection()
    table = con.execute(f"""
        WITH grand AS (
            SELECT SUM({sum_expr}) AS total FROM {TABLE}
        )
        SELECT
            "Channels",
            ROUND(
                100.0 * ({sum_expr}) / NULLIF(g.total, 0),
                2
            ) AS "share_pct"
        FROM {TABLE}
        CROSS JOIN grand g
        WHERE ({sum_expr}) > 0
        ORDER BY "share_pct" DESC
        LIMIT 15
    """).arrow()
    con.close()
    return [[table, "bar"]]   # ✅ bar not line