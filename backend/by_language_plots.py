"""
by_language_plots.py
"""
import pyarrow as pa
from database import get_connection


def get_language_publish_rate():
    """
    KPI-13: Language Publish Rate — bar chart.
    Formula: published_count ÷ created_count × 100
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
    return [[table, "bar"]]


def get_language_upload_share():
    """
    KPI-15: Language Upload Share — bar chart.
    Formula: uploaded_count ÷ SUM(all uploaded) × 100
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
    return [[table, "bar"]]