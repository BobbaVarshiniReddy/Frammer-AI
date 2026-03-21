import pyarrow as pa
from database import get_connection

# ⚠️ Check actual table name at http://127.0.0.1:8000/debug/tables
TABLE = "raw_user"


# Plot 1 — Publish Rate by User
def get_plot_publish_rate() -> tuple[pa.Table, str]:
    con = get_connection()
    table = con.execute(f"""
        SELECT
            "User",
            ROUND(
                100.0 * SUM("Published Count") / NULLIF(SUM("Created Count"), 0),
                2
            ) AS "publish_rate_pct"
        FROM {TABLE}
        GROUP BY "User"
        HAVING SUM("Created Count") >= 10     -- min threshold for validity
        ORDER BY "publish_rate_pct" DESC
        LIMIT 10
    """).arrow()
    con.close()
    return table, "bar"   # ✅ bar not line


# Plot 2 — Top Uploading Users
def get_plot_top_uploaders() -> tuple[pa.Table, str]:
    con = get_connection()
    table = con.execute(f"""
        SELECT
            "User",
            SUM("Uploaded Count") AS "Uploaded Count"
        FROM {TABLE}
        GROUP BY "User"
        ORDER BY "Uploaded Count" DESC
        LIMIT 10
    """).arrow()
    con.close()
    return table, "bar"