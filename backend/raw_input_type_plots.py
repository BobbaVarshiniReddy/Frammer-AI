import pyarrow as pa
from database import get_connection

# ---------------------------------------------------------------------------
# Plot 1 — Input Type Publish Rate (Bar)
# ---------------------------------------------------------------------------

def get_input_type_publish_rate_plot() -> tuple[pa.Table, str]:
    """
    Plot: Bar – publish rate (%) per input type
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "input_type",
            ROUND(
                100.0 * SUM("published_count") / NULLIF(SUM("created_count"), 0),
                2
            ) AS "input_type_publish_rate"
        FROM raw_input_type
        GROUP BY "input_type"
        ORDER BY "input_type_publish_rate" DESC
    """).arrow()
    con.close()
    return table, "bar"

# ---------------------------------------------------------------------------
# Plot 2 — Input Type Amplification Ratio (Line)
# ---------------------------------------------------------------------------

def get_input_type_amplification_ratio_plot() -> tuple[pa.Table, str]:
    """
    Plot: Line – amplification ratio per input type
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "input_type",
            ROUND(
                1.0 * SUM("created_count") / NULLIF(SUM("uploaded_count"), 0),
                2
            ) AS "input_type_amplification_ratio"
        FROM raw_input_type
        GROUP BY "input_type"
        ORDER BY "input_type_amplification_ratio" DESC
    """).arrow()
    con.close()
    return table, "line"

# ---------------------------------------------------------------------------
# Plot 3 — Unknown Input Type Contribution (Donut)
# ---------------------------------------------------------------------------

def get_unknown_input_type_contribution_plot() -> tuple[pa.Table, str]:
    """
    Plot: Donut – unknown vs known uploaded contribution
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            CASE 
                WHEN LOWER(TRIM("input_type")) = 'unknown' THEN 'Unknown'
                ELSE 'Known'
            END AS "category",
            SUM("uploaded_count") AS "value"
        FROM raw_input_type
        GROUP BY "category"
    """).arrow()
    con.close()
    return table, "donut"
