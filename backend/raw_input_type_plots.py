import pyarrow as pa
from database import get_connection


# ---------------------------------------------------------------------------
# Plot 1 — Input Type Publish Rate
# ---------------------------------------------------------------------------
def get_input_type_publish_rate_plot():
    con = get_connection()
    table = con.execute("""
        SELECT
            input_type,
            ROUND(
                100.0 * SUM(published_count) / NULLIF(SUM(created_count), 0),
                2
            ) AS input_type_publish_rate
        FROM (
            SELECT
                "Input Type"      AS input_type,
                "Created Count"   AS created_count,
                "Published Count" AS published_count
            FROM raw_input_type
        )
        GROUP BY input_type
        ORDER BY input_type_publish_rate DESC
    """).arrow()
    con.close()

    # multiple chart options possible
    return [
        [table, "bar"],
        [table, "line"]
    ]


# ---------------------------------------------------------------------------
# Plot 2 — Input Type Amplification Ratio
# ---------------------------------------------------------------------------
def get_input_type_amplification_ratio_plot():
    con = get_connection()
    table = con.execute("""
        SELECT
            input_type,
            ROUND(
                1.0 * SUM(created_count) / NULLIF(SUM(uploaded_count), 0),
                2
            ) AS input_type_amplification_ratio
        FROM (
            SELECT
                "Input Type"      AS input_type,
                "Created Count"   AS created_count,
                "Uploaded Count"  AS uploaded_count
            FROM raw_input_type
        )
        GROUP BY input_type
        ORDER BY input_type_amplification_ratio DESC
    """).arrow()
    con.close()

    return [
        [table, "line"],
        [table, "bar"]
    ]


# ---------------------------------------------------------------------------
# Plot 3 — Unknown Input Type Contribution
# ---------------------------------------------------------------------------
def get_unknown_input_type_contribution_plot():
    con = get_connection()
    table = con.execute("""
        SELECT
            CASE 
                WHEN LOWER(TRIM("Input Type")) = 'unknown' THEN 'Unknown'
                ELSE 'Known'
            END AS category,
            SUM("Uploaded Count") AS value
        FROM raw_input_type
        GROUP BY category
    """).arrow()
    con.close()

    return [
        [table, "donut"],
        [table, "pie"]
    ]


# ---------------------------------------------------------------------------
# Plot 4 — Publish Rate vs Amplification Ratio
# ---------------------------------------------------------------------------
def get_publish_vs_amplification_scatter():
    con = get_connection()
    table = con.execute("""
        SELECT
            "Type" AS Type,
            ROUND(
                100.0 * SUM(CASE WHEN "Published" = TRUE THEN 1 ELSE 0 END)
                / NULLIF(COUNT(*), 0),
                2
            ) AS publish_rate_pct,
            ROUND(
                100.0 * SUM("Shares") / NULLIF(SUM("Views"), 0),
                4
            ) AS amplification_ratio
        FROM raw_video_list
        GROUP BY "Type"
        ORDER BY publish_rate_pct DESC
    """).arrow()
    con.close()

    return [
        [table, "scatter"]
    ]