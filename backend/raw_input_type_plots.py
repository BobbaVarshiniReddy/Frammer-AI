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
        FROM (
            SELECT
                "Input Type"      AS input_type,
                "Created Count"   AS created_count,
                "Published Count" AS published_count
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
        FROM (
            SELECT
                "Input Type"      AS input_type)
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
                WHEN LOWER(TRIM("input_type")) = 'Unknown' THEN 'Unknown'
                ELSE 'Known'
            END AS "category",
            SUM("uploaded_count") AS "value"
        FROM (
            SELECT
                "Input Type"      AS input_type
            FROM raw_input_type
        GROUP BY "category"
    """).arrow()
    con.close()
    return table, "donut"

# ---------------------------------------------------------------------------
# Plot 4 — Publish Rate vs Amplification Ratio
# ---------------------------------------------------------------------------

def get_publish_vs_amplification_scatter() -> tuple[pa.Table, str]:
    """
    Scatter plot: Each point = one content Type.
    X-axis : publish_rate_pct
    Y-axis : amplification_ratio
    Bubble label : Type
    Returns
    -------
    table      : columns ["Type", "publish_rate_pct", "amplification_ratio", "video_count"]
    chart_type : "scatter"
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "Type"                                              AS "Type",
            ROUND(
                100.0 * SUM(CASE WHEN "Published" = TRUE THEN 1 ELSE 0 END)
                / NULLIF(COUNT(*), 0),
                2
            )                                                   AS "publish_rate_pct",
            ROUND(
                100.0 * SUM("Shares") / NULLIF(SUM("Views"), 0),
                4
            )                                                   AS "amplification_ratio"
        FROM raw_video_list
        GROUP BY "Type"
        ORDER BY "publish_rate_pct" DESC
    """).arrow()
    con.close()
    return table, "scatter"
