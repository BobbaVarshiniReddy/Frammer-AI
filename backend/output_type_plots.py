import pyarrow as pa
from database import get_connection


# ---------------------------------------------------------------------------
# Plot 1 — Output Format Publish Rate (Bar)
# ---------------------------------------------------------------------------
def get_output_format_publish_rate_plot():
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
    return [[table, "bar"]]


# ---------------------------------------------------------------------------
# Plot 2 — Output Format Mix Distribution (Donut)
# ---------------------------------------------------------------------------
def get_output_format_mix_distribution_plot():
    con = get_connection()
    table = con.execute("""
        WITH total AS (
            SELECT SUM("Created Count") AS total_created
            FROM raw_output_type
        )
        SELECT
            r."Output Type" AS category,
            ROUND(
                100.0 * r."Created Count" / NULLIF(t.total_created, 0),
                2
            ) AS value
        FROM raw_output_type r
        CROSS JOIN total t
        ORDER BY value DESC
    """).arrow()
    con.close()
    return [[table, "donut"]]
