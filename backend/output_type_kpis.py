import pyarrow as pa
from database import get_connection


# ---------------------------------------------------------------------------
# KPI 23 — Output Type Publish Rate
# ---------------------------------------------------------------------------
def get_kpi23_output_type_publish_rate() -> tuple[pa.Table, str]:
    con = get_connection()
    table = con.execute("""
        SELECT
            output_type,
            ROUND(
                100.0 * SUM(published_count) / NULLIF(SUM(created_count), 0),
                2
            ) AS publish_rate_pct
        FROM (
            SELECT
                "Output Type" AS output_type,
                "Created Count" AS created_count,
                "Published Count" AS published_count
            FROM raw_output_type
        )
        GROUP BY output_type
        ORDER BY publish_rate_pct DESC
    """).arrow()
    con.close()
    return table, "KPI-23 – Output Type Publish Rate (%)"


# ---------------------------------------------------------------------------
# KPI 24 — Output Type Amplification Ratio
# ---------------------------------------------------------------------------
def get_kpi24_output_type_amplification_ratio() -> tuple[pa.Table, str]:
    con = get_connection()
    table = con.execute("""
        SELECT
            output_type,
            ROUND(
                1.0 * SUM(created_count) / NULLIF(SUM(uploaded_count), 0),
                2
            ) AS amplification_ratio
        FROM (
            SELECT
                "Output Type" AS output_type,
                "Created Count" AS created_count,
                "Uploaded Count" AS uploaded_count
            FROM raw_output_type
        )
        GROUP BY output_type
        ORDER BY amplification_ratio DESC
    """).arrow()
    con.close()
    return table, "KPI-24 – Output Type Amplification Ratio"


# ---------------------------------------------------------------------------
# KPI 25 — Unknown Output Type Rate
# ---------------------------------------------------------------------------
def get_kpi25_unknown_output_type_rate() -> tuple[pa.Table, str]:
    con = get_connection()
    table = con.execute("""
        WITH totals AS (
            SELECT SUM("Uploaded Count") AS total_uploaded
            FROM raw_output_type
        ),
        grouped AS (
            SELECT
                CASE
                    WHEN LOWER(TRIM("Output Type")) = 'unknown' THEN 'unknown'
                    ELSE 'known'
                END AS category,
                SUM("Uploaded Count") AS uploaded_count
            FROM raw_output_type
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
    return table, "KPI-25 – Unknown Output Type Rate (%)"


# ---------------------------------------------------------------------------
# KPI 26 — Avg Published Duration
# ---------------------------------------------------------------------------
def get_kpi26_avg_publish_duration() -> tuple[pa.Table, str]:
    con = get_connection()
    table = con.execute("""
        SELECT
            "Output Type" AS output_type,
            AVG("Published Duration (hh:mm:ss)") AS avg_publish_duration
        FROM raw_output_type
        GROUP BY 1
        ORDER BY avg_publish_duration DESC
    """).arrow()
    con.close()
    return table, "KPI-26 – Avg Published Duration"
