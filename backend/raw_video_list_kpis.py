"""
kpis.py
"""

import pyarrow as pa
from database import get_connection


def get_kpi14_published_platform_distribution() -> tuple[pa.Table, str]:
    """
    KPI 14: For every Published Platform, the percentage share of
    published videos on that platform.

    Returns
    -------
    table       : columns ["Published Platform", "share_pct"]
                  Rows sorted by share_pct DESC.
    description : str
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
        WHERE "Published" = 'Yes'
        GROUP BY "Published Platform"
        ORDER BY "share_pct" DESC
    """).arrow()
    con.close()
    return table, "bar"


def get_kpi16_published_rate_per_uploader() -> tuple[pa.Table, str]:
    """
    KPI 16: For each uploader, the percentage of their uploaded videos
    that were published.  Sorted by Published_Rate_% DESC.

    Returns
    -------
    table       : columns ["Uploaded By", "uploaded", "published", "Published_Rate_%"]
    description : str
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "Uploaded By"                                               AS "Uploaded By",
            COUNT(*)                                                    AS "uploaded",
            SUM(CASE WHEN "Published" = 'Yes' THEN 1 ELSE 0 END)       AS "published",
            ROUND(
                100.0
                * SUM(CASE WHEN "Published" = 'Yes' THEN 1 ELSE 0 END)
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

    Parameters
    ----------
    top_n : int, default 5

    Returns
    -------
    table       : columns ["Uploaded By", "video_count"]
    description : str
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
    Formula: Unknown rows ÷ Total rows × 100

    Returns
    -------
    table       : columns ["total_rows", "unknown_rows", "unknown_team_rate_pct"]
    description : str
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
    KPI 20: Among rows where Published = 'Yes', the percentage that have
    a non-Unknown Published URL.
    Formula: Non-null URLs ÷ Published rows × 100

    Returns
    -------
    table       : columns ["published_rows", "url_present_rows", "url_completeness_pct"]
    description : str
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
        WHERE "Published" = 'Yes'
    """).arrow()
    con.close()
    return table, None


def get_kpi21_overall_published_rate() -> tuple[pa.Table, str]:
    """
    KPI 21: Percentage of all videos that have been published.
    Formula: Published rows ÷ Total rows × 100

    Returns
    -------
    table       : columns ["total_rows", "published_rows", "published_rate_pct"]
    description : str
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            COUNT(*)                                                        AS "total_rows",
            SUM(CASE WHEN "Published" = 'Yes' THEN 1 ELSE 0 END)           AS "published_rows",
            ROUND(
                100.0
                * SUM(CASE WHEN "Published" = 'Yes' THEN 1 ELSE 0 END)
                / COUNT(*),
                2
            )                                                               AS "published_rate_pct"
        FROM raw_video_list
    """).arrow()
    con.close()
    return table, None


def get_kpi22_duplicate_video_id_count() -> tuple[pa.Table, str]:
    """
    KPI 22: Number of Video IDs that appear more than once in the dataset.
    Returns
    -------
    table       : columns ["duplicate_video_id_count"]
    description : str
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