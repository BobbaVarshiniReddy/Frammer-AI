import pyarrow as pa
from database import get_connection


# KPI 16 — User Publish Rate
def get_user_publish_rate() -> tuple[pa.Table, str]:

    con = get_connection()
    table = con.execute("""
        SELECT
            "User",
            "Published Count",
            "Created Count",
            ROUND(
                100.0 * "Published Count" / NULLIF("Created Count", 0),
                2
            ) AS "publish_rate_pct"
        FROM combined_by_user
        ORDER BY "publish_rate_pct" DESC
    """).arrow()

    con.close()
    return table, "line"


# KPI 17 — Top Uploading Users
def get_top_uploaders(top_n: int = 5) -> tuple[pa.Table, str]:

    con = get_connection()
    table = con.execute(f"""
        SELECT
            "User",
            "Uploaded Count"
        FROM combined_by_user
        ORDER BY "Uploaded Count" DESC
        LIMIT {top_n}
    """).arrow()

    con.close()
    return table, "bar"


# KPI 21 — QA/Test Activity %
def get_qa_activity_percent() -> tuple[pa.Table, str]:

    con = get_connection()
    table = con.execute("""
        SELECT
            ROUND(
                100.0 *
                SUM(CASE
                    WHEN "User" IN (
                        'QA-Purushottam',
                        'QA-Bhargavi',
                        'QA-Ankith',
                        'QA-Aniket',
                        'QA-Amit',
                        'deleteme@frammer.com',
                        'Test User',
                        'Auto Upload'
                    )
                    THEN "Uploaded Count"
                    ELSE 0
                END)
                /
                SUM("Uploaded Count"),
                2
            ) AS "qa_activity_pct"
        FROM combined_by_user
    """).arrow()

    con.close()
    return table, None