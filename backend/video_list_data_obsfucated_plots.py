"""
plots.py
"""

import pyarrow as pa
from database import get_connection


# ---------------------------------------------------------------------------
# Plot 1 — Published vs Content Type
# ---------------------------------------------------------------------------

def get_published_vs_content_type() -> tuple[pa.Table, str]:
    """
    Plot 1 (KPIs 14, 16, 17, 19, 20, 21): Grouped bar – count of videos
    for each (Type, Published) combination.

    Returns
    -------
    table      : columns ["Type", "Published", "count"]
    chart_type : "bar"
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "Type"          AS "Type",
            "Published"     AS "Published",
            COUNT(*)        AS "count"
        FROM raw_video_list
        GROUP BY "Type", "Published"
        ORDER BY "Type", "Published"
    """).arrow()
    con.close()
    return table, "bar"


# ---------------------------------------------------------------------------
# Plot 2 — Published vs Team
# ---------------------------------------------------------------------------

def get_published_vs_team() -> tuple[pa.Table, str]:
    """
    Plot 2: Grouped bar – count of videos for each
    (Team Name, Published) combination.

    Returns
    -------
    table      : columns ["Team Name", "Published", "count"]
    chart_type : "bar"
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "Team Name"     AS "Team Name",
            "Published"     AS "Published",
            COUNT(*)        AS "count"
        FROM raw_video_list
        GROUP BY "Team Name", "Published"
        ORDER BY "Team Name", "Published"
    """).arrow()
    con.close()
    return table, "bar"


# ---------------------------------------------------------------------------
# Plot 3 — Data Completeness Heatmap
# ---------------------------------------------------------------------------

def get_data_completeness_heatmap() -> tuple[pa.Table, str]:
    """
    Plot 3: Heatmap – per-column completeness percentage
    (rows where value != 'unknown').

    Columns checked: Headline, Source, Published, Team Name, Type,
                     Uploaded By, Published Platform, Published URL

    Returns
    -------
    table      : columns ["Column Name", "completeness_pct"]
                 One row per checked column, long format.
    chart_type : "heatmap"
    """
    checked_cols = [
        "Headline",
        "Source",
        "Published",
        "Team Name",
        "Type",
        "Uploaded By",
        "Published Platform",
        "Published URL",
    ]

    select_exprs = ",\n            ".join(
        f"ROUND(100.0 * SUM(CASE WHEN LOWER(\"{c}\") != 'unknown' THEN 1 ELSE 0 END) / COUNT(*), 2) AS \"{c}\""
        for c in checked_cols
    )

    con = get_connection()
    wide = con.execute(f"""
        SELECT {select_exprs}
        FROM raw_video_list
    """).fetchone()
    con.close()

    # Pivot to long format: one row per column
    col_names = pa.array(checked_cols, type=pa.string())
    pct_values = pa.array(list(wide), type=pa.float64())
    table = pa.table({"Column Name": col_names, "completeness_pct": pct_values})
    return table, "heatmap"


# ---------------------------------------------------------------------------
# Plot 4 — Top 7 Uploaders by Video Count
# ---------------------------------------------------------------------------

def get_top_uploaders(top_n: int = 7) -> tuple[pa.Table, str]:
    """
    Plot 4 (KPI 17): Line – top N uploaders ranked by video count,
    excluding 'unknown' uploaders.

    Parameters
    ----------
    top_n : int, default 7

    Returns
    -------
    table      : columns ["Uploaded By", "video_count"]
    chart_type : "line"
    """
    con = get_connection()
    table = con.execute(f"""
        SELECT
            "Uploaded By"       AS "Uploaded By",
            COUNT("Video ID")   AS "video_count"
        FROM raw_video_list
        WHERE LOWER("Uploaded By") != 'unknown'
        GROUP BY "Uploaded By"
        ORDER BY "video_count" DESC
        LIMIT {top_n}
    """).arrow()
    con.close()
    return table, "line"


# ---------------------------------------------------------------------------
# Plot 5 — Published % by Content Type
# ---------------------------------------------------------------------------

def get_published_pct_by_type() -> tuple[pa.Table, str]:
    """
    Plot 5: Line – percentage of videos with Published='Yes' per content
    type, sorted descending.

    Returns
    -------
    table      : columns ["Type", "published_pct"]
    chart_type : "line"
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "Type"                                                          AS "Type",
            ROUND(
                100.0
                * SUM(CASE WHEN "Published" = 'Yes' THEN 1 ELSE 0 END)
                / COUNT(*),
                2
            )                                                               AS "published_pct"
        FROM raw_video_list
        GROUP BY "Type"
        ORDER BY "published_pct" DESC
    """).arrow()
    con.close()
    return table, "line"


# ---------------------------------------------------------------------------
# Plot 6 — Published % by User
# ---------------------------------------------------------------------------

def get_published_pct_by_uploader(top_n: int = 10) -> tuple[pa.Table, str]:
    """
    Plot 6 (KPI 16): Line – percentage published per uploader, top N
    sorted descending.

    Parameters
    ----------
    top_n : int, default 10

    Returns
    -------
    table      : columns ["Uploaded By", "published_pct"]
    chart_type : "line"
    """
    con = get_connection()
    table = con.execute(f"""
        SELECT
            "Uploaded By"                                                   AS "Uploaded By",
            ROUND(
                100.0
                * SUM(CASE WHEN "Published" = 'Yes' THEN 1 ELSE 0 END)
                / COUNT(*),
                2
            )                                                               AS "published_pct"
        FROM raw_video_list
        GROUP BY "Uploaded By"
        ORDER BY "published_pct" DESC
        LIMIT {top_n}
    """).arrow()
    con.close()
    return table, "line"