"""
by_language_kpis.py
"""
import pyarrow as pa
from database import get_connection


def get_kpi13_language_publish_rate():
    """
    KPI-13: Language Publish Rate
    Published Count ÷ Created Count × 100
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            language_name                                               AS "Language",
            published_count                                             AS "Published Count",
            created_count                                               AS "Created Count",
            ROUND(
                100.0 * published_count / NULLIF(created_count, 0),
                2
            )                                                           AS "Language_Publish_Rate_%"
        FROM summary_language
        ORDER BY ROUND(100.0 * published_count / NULLIF(created_count, 0), 2) DESC NULLS LAST
    """).arrow()
    con.close()
    return [[table, "KPI 13 - Language Publish Rate (%)"]]


def get_kpi15_language_upload_share():
    """
    KPI-15: Language Upload Share
    Uploaded Count ÷ SUM(All Uploaded) × 100
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            language_name                                               AS "Language",
            uploaded_count                                              AS "Uploaded Count",
            ROUND(
                100.0 * uploaded_count / NULLIF(SUM(uploaded_count) OVER (), 0),
                2
            )                                                           AS "Language_Upload_Share_%"
        FROM summary_language
        ORDER BY ROUND(100.0 * uploaded_count / NULLIF(SUM(uploaded_count) OVER (), 0), 2) DESC
    """).arrow()
    con.close()
    return [[table, "KPI 15 - Language Upload Share (%)"]]