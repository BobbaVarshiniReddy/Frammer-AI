import pyarrow as pa
from database import get_connection


# Plot 1 — Publish Rate by User
def get_plot_publish_rate() -> tuple[pa.Table, str]:

    con = get_connection()
    table = con.execute("""
        SELECT
            "User",
            ROUND(
                100.0 * "Published Count" / NULLIF("Created Count", 0),
                2
            ) AS "publish_rate_pct"
        FROM combined_by_user
        ORDER BY "publish_rate_pct" DESC
        LIMIT 10
    """).arrow()

    con.close()
    return table, "line"


# Plot 2 — Top Uploading Users
def get_plot_top_uploaders() -> tuple[pa.Table, str]:

    con = get_connection()
    table = con.execute("""
        SELECT
            "User",
            "Uploaded Count"
        FROM combined_by_user
        ORDER BY "Uploaded Count" DESC
        LIMIT 10
    """).arrow()

    con.close()
    return table, "bar"
