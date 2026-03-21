"""
raw_video_list_plots.py
"""

import pyarrow as pa
from database import get_connection


def get_published_vs_content_type() -> tuple[pa.Table, str]:
    con = get_connection()
    table = con.execute("""
        SELECT
            "Type"                                      AS "Type",
            CASE WHEN "Published" = true THEN 'Yes'
                 ELSE 'No' END                          AS "Published",
            COUNT(*)                                    AS "count"
        FROM raw_video_list
        GROUP BY "Type", "Published"
        ORDER BY "Type", "Published"
    """).arrow()
    con.close()
    return table, "bar"


def get_published_vs_team() -> tuple[pa.Table, str]:
    con = get_connection()
    table = con.execute("""
        SELECT
            "Team Name"                                 AS "Team Name",
            CASE WHEN "Published" = true THEN 'Yes'
                 ELSE 'No' END                          AS "Published",
            COUNT(*)                                    AS "count"
        FROM raw_video_list
        GROUP BY "Team Name", "Published"
        ORDER BY "Team Name", "Published"
    """).arrow()
    con.close()
    return table, "bar"


def get_data_completeness_heatmap() -> tuple[pa.Table, str]:
    checked_cols = [
        "Headline", "Source", "Team Name", "Type",
        "Uploaded By", "Published Platform", "Published URL",
    ]
    select_exprs = ",\n            ".join(
        f"ROUND(100.0 * SUM(CASE WHEN LOWER(CAST(\"{c}\" AS VARCHAR)) != 'unknown' THEN 1 ELSE 0 END) / COUNT(*), 2) AS \"{c}\""
        for c in checked_cols
    )
    con = get_connection()
    wide = con.execute(f"""
        SELECT {select_exprs}
        FROM raw_video_list
    """).fetchone()
    con.close()
    col_names  = pa.array(checked_cols, type=pa.string())
    pct_values = pa.array(list(wide),   type=pa.float64())
    table = pa.table({"Column Name": col_names, "completeness_pct": pct_values})
    return table, "heatmap"


def get_top_uploaders(top_n: int = 7) -> tuple[pa.Table, str]:
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


def get_published_pct_by_type() -> tuple[pa.Table, str]:
    con = get_connection()
    table = con.execute("""
        SELECT
            "Type"                                                          AS "Type",
            ROUND(
                100.0
                * SUM(CASE WHEN "Published" = true THEN 1 ELSE 0 END)
                / COUNT(*),
                2
            )                                                               AS "published_pct"
        FROM raw_video_list
        GROUP BY "Type"
        ORDER BY "published_pct" DESC
    """).arrow()
    con.close()
    return table, "line"


def get_published_pct_by_uploader(top_n: int = 10) -> tuple[pa.Table, str]:
    con = get_connection()
    table = con.execute(f"""
        SELECT
            "Uploaded By"                                                   AS "Uploaded By",
            ROUND(
                100.0
                * SUM(CASE WHEN "Published" = true THEN 1 ELSE 0 END)
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