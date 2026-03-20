"""
raw_video_list_kpis.py
"""

import pyarrow as pa
from database import get_connection


def get_kpi14_published_platform_distribution() -> tuple[pa.Table, str]:
    """
    KPI 14: For every Published Platform, the percentage share of
    published videos on that platform.
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "Published Platform"                                        AS "Published Platform",
            ROUND(
                100.0 * COUNT(*) / SUM(COUNT(*)) OVER (),
                2
            )                                                           AS "share_pct"
        FROM raw_video_list
        WHERE "Published" = true
        GROUP BY "Published Platform"
        ORDER BY "share_pct" DESC
    """).arrow()
    con.close()
    return table, "bar"


def get_kpi16_published_rate_per_uploader() -> tuple[pa.Table, str]:
    """
    KPI 16: For each uploader, the percentage of their uploaded videos
    that were published.
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "Uploaded By"                                               AS "Uploaded By",
            COUNT(*)                                                    AS "uploaded",
            SUM(CASE WHEN "Published" = true THEN 1 ELSE 0 END)       AS "published",
            ROUND(
                100.0
                * SUM(CASE WHEN "Published" = true THEN 1 ELSE 0 END)
                / COUNT(*),
                2
            )                                                           AS "Published_Rate_%"
        FROM raw_video_list
        GROUP BY "Uploaded By"
        ORDER BY "Published_Rate_%" DESC
    """).arrow()
    con.close()
    return table, "bar"


def get_kpi17_top_uploaders(top_n: int = 5) -> tuple[pa.Table, str]:
    """
    KPI 17: The top N uploaders ranked by total number of uploaded videos.
    """
    con = get_connection()
    table = con.execute(f"""
        SELECT
            "Uploaded By"       AS "Uploaded By",
            COUNT("Video ID")   AS "video_count"
        FROM raw_video_list
        GROUP BY "Uploaded By"
        ORDER BY "video_count" DESC
        LIMIT {top_n}
    """).arrow()
    con.close()
    return table, "line"


def get_kpi19_unknown_team_name_rate() -> tuple[pa.Table, str]:
    """
    KPI 19: Percentage of rows where Team Name is 'Unknown'.
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            COUNT(*)                                                        AS "total_rows",
            SUM(CASE WHEN LOWER("Team Name") = 'unknown' THEN 1 ELSE 0 END) AS "unknown_rows",
            ROUND(
                100.0
                * SUM(CASE WHEN LOWER("Team Name") = 'unknown' THEN 1 ELSE 0 END)
                / COUNT(*),
                2
            )                                                               AS "unknown_team_rate_pct"
        FROM raw_video_list
    """).arrow()
    con.close()
    return table, None


def get_kpi20_published_url_completeness() -> tuple[pa.Table, str]:
    """
    KPI 20: Among published rows, percentage with a non-Unknown Published URL.
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            COUNT(*)                                                                AS "published_rows",
            SUM(CASE WHEN LOWER("Published URL") != 'unknown' THEN 1 ELSE 0 END)   AS "url_present_rows",
            ROUND(
                100.0
                * SUM(CASE WHEN LOWER("Published URL") != 'unknown' THEN 1 ELSE 0 END)
                / COUNT(*),
                2
            )                                                                       AS "url_completeness_pct"
        FROM raw_video_list
        WHERE "Published" = true
    """).arrow()
    con.close()
    return table, None


def get_kpi21_overall_published_rate() -> tuple[pa.Table, str]:
    """
    KPI 21: Percentage of all videos that have been published.
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            COUNT(*)                                                        AS "total_rows",
            SUM(CASE WHEN "Published" = true THEN 1 ELSE 0 END)           AS "published_rows",
            ROUND(
                100.0
                * SUM(CASE WHEN "Published" = true THEN 1 ELSE 0 END)
                / COUNT(*),
                2
            )                                                               AS "published_rate_pct"
        FROM raw_video_list
    """).arrow()
    con.close()
    return table, None


def get_kpi22_duplicate_video_id_count() -> tuple[pa.Table, str]:
    """
    KPI 22: Number of Video IDs that appear more than once.
    """
    con = get_connection()
    table = con.execute("""
        SELECT COUNT(*) AS "duplicate_video_id_count"
        FROM (
            SELECT "Video ID"
            FROM raw_video_list
            GROUP BY "Video ID"
            HAVING COUNT(*) > 1
        )
    """).arrow()
    con.close()
    return table, None