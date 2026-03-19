import pyarrow as pa
from database import get_connection


# ---------------------------------------------------------------------------
# KPI-11 — Output Format Publish Rate
# ---------------------------------------------------------------------------
def get_kpi11_output_format_publish_rate():
    con = get_connection()
    table = con.execute("""
        SELECT
            "Output Type" AS output_type,
            ROUND(
                100.0 * SUM("Published Count") / NULLIF(SUM("Created Count"), 0),
                2
            ) AS publish_rate_pct
        FROM raw_output_type
        GROUP BY "Output Type"
        ORDER BY publish_rate_pct DESC
    """).arrow()
    con.close()
    return [[table, "none"]]


# ---------------------------------------------------------------------------
# KPI-12 — Output Format Mix Distribution
# ---------------------------------------------------------------------------
def get_kpi12_output_format_mix_distribution():
    con = get_connection()
    table = con.execute("""
        WITH total AS (
            SELECT SUM("Created Count") AS total_created
            FROM raw_output_type
        )
        SELECT
            r."Output Type" AS output_type,
            r."Created Count" AS created_count,
            ROUND(
                100.0 * r."Created Count" / NULLIF(t.total_created, 0),
                2
            ) AS share_pct
        FROM raw_output_type r
        CROSS JOIN total t
        ORDER BY share_pct DESC
    """).arrow()
    con.close()
    return [[table, "none"]]
