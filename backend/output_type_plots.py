import pyarrow as pa
from database import get_connection


# ---------------------------------------------------------------------------
# Plot 1 — Output Type Publish Rate (Bar)
# ---------------------------------------------------------------------------
def get_output_type_publish_rate_plot() -> tuple[pa.Table, str]:
    con = get_connection()
    table = con.execute("""
        SELECT
            output_type,
            ROUND(
                100.0 * SUM(published_count) / NULLIF(SUM(created_count), 0),
                2
            ) AS publish_rate
        FROM (
            SELECT
                "Output Type" AS output_type,
                "Created Count" AS created_count,
                "Published Count" AS published_count
            FROM raw_output_type
        )
        GROUP BY output_type
        ORDER BY publish_rate DESC
    """).arrow()
    con.close()
    return table, "bar"


# ---------------------------------------------------------------------------
# Plot 2 — Output Type Amplification Ratio (Line)
# ---------------------------------------------------------------------------
def get_output_type_amplification_ratio_plot() -> tuple[pa.Table, str]:
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
    return table, "line"


# ---------------------------------------------------------------------------
# Plot 3 — Unknown Output Type Contribution (Donut)
# ---------------------------------------------------------------------------
def get_unknown_output_type_contribution_plot() -> tuple[pa.Table, str]:
    con = get_connection()
    table = con.execute("""
        SELECT
            CASE 
                WHEN LOWER(TRIM("Output Type")) = 'unknown' THEN 'Unknown'
                ELSE 'Known'
            END AS category,
            SUM("Uploaded Count") AS value
        FROM raw_output_type
        GROUP BY category
    """).arrow()
    con.close()
    return table, "donut"


# ---------------------------------------------------------------------------
# Plot 4 — Publish Rate vs Amplification Ratio (Scatter)
# ---------------------------------------------------------------------------
def get_output_type_publish_vs_amplification_scatter() -> tuple[pa.Table, str]:
    con = get_connection()
    table = con.execute("""
        SELECT
            output_type,
            ROUND(
                100.0 * SUM(published_count) / NULLIF(SUM(created_count), 0),
                2
            ) AS publish_rate,
            ROUND(
                1.0 * SUM(created_count) / NULLIF(SUM(uploaded_count), 0),
                2
            ) AS amplification_ratio
        FROM (
            SELECT
                "Output Type" AS output_type,
                "Created Count" AS created_count,
                "Published Count" AS published_count,
                "Uploaded Count" AS uploaded_count
            FROM raw_output_type
        )
        GROUP BY output_type
    """).arrow()
    con.close()
    return table, "scatter"
