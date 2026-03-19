import pyarrow as pa
from database import get_connection


def get_kpi09_input_type_publish_rate():
    """
    KPI-09: For every Input Type, the percentage of created videos
    that were eventually published.
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            input_type,
            ROUND(
                100.0 * SUM(published_count) / NULLIF(SUM(created_count), 0),
                2
            ) AS publish_rate_pct
        FROM (
            SELECT
                "Input Type"      AS input_type,
                "Created Count"   AS created_count,
                "Published Count" AS published_count
            FROM raw_input_type
        )
        GROUP BY input_type
        ORDER BY publish_rate_pct DESC
    """).arrow()
    con.close()
    return [[table, "none"]]


def get_kpi10_input_type_amplification_ratio():
    """
    KPI-10: Created Count / Uploaded Count
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            input_type,
            ROUND(
                1.0 * SUM(created_count) / NULLIF(SUM(uploaded_count), 0),
                2
            ) AS amplification_ratio
        FROM (
            SELECT
                "Input Type"      AS input_type,
                "Created Count"   AS created_count,
                "Uploaded Count"  AS uploaded_count
            FROM raw_input_type
        )
        GROUP BY input_type
        ORDER BY amplification_ratio DESC
    """).arrow()
    con.close()
    return [[table, "none"]]


def get_kpi19_unknown_input_type_rate():
    """
    KPI-19: % of uploaded videos with unknown input type
    """
    con = get_connection()
    table = con.execute("""
        WITH totals AS (
            SELECT SUM("Uploaded Count") AS total_uploaded
            FROM raw_input_type
        ),
        grouped AS (
            SELECT
                CASE
                    WHEN LOWER(TRIM("Input Type")) = 'unknown' THEN 'unknown'
                    ELSE 'known'
                END AS category,
                SUM("Uploaded Count") AS uploaded_count
            FROM raw_input_type
            GROUP BY 1
        )
        SELECT
            g.category,
            g.uploaded_count,
            ROUND(
                100.0 * g.uploaded_count / NULLIF(t.total_uploaded, 0),
                2
            ) AS share_pct
        FROM grouped g
        CROSS JOIN totals t
        ORDER BY
            CASE WHEN g.category = 'unknown' THEN 0 ELSE 1 END
    """).arrow()
    con.close()
    return [[table, "none"]]
