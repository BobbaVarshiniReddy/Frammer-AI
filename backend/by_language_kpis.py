"""
kpi_language.py
"""
import pyarrow as pa
from database import get_connection


def get_kpi13_language_publish_rate() -> tuple[pa.Table, str]:
    """
    Language Publish Rate:
    Published Count ÷ Created Count × 100
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "Language",
            "Published Count",
            "Created Count",
            ROUND(
                100.0 * "Published Count" / NULLIF("Created Count", 0),
                2
            ) AS "Language_Publish_Rate_%"
        FROM by_language
        ORDER BY "Language_Publish_Rate_%" DESC NULLS LAST
    """).arrow()
    con.close()
    return table, "KPI 13 - Language Publish Rate (%)"


def get_kpi15_language_upload_share() -> tuple[pa.Table, str]:
    """
    Language Upload Share:
    Uploaded Count ÷ SUM(All Uploaded) × 100
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "Language",
            "Uploaded Count",
            ROUND(
                100.0 * "Uploaded Count" / NULLIF(SUM("Uploaded Count") OVER (), 0),
                2
            ) AS "Language_Upload_Share_%"
        FROM by_language
        ORDER BY "Language_Upload_Share_%" DESC
    """).arrow()
    con.close()
    return table, "KPI 15 - Language Upload Share (%)"
