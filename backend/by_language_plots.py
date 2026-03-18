"""
plots_language.py
"""

import pyarrow as pa
from database import get_connection


# ---------------------------------------------------------------------------
# Plot — KPI-13 Language Publish Rate
# ---------------------------------------------------------------------------

def get_language_publish_rate() -> tuple[pa.Table, str]:
    """
    KPI-13: Language Publish Rate — bar chart of publish rate per language.
    Formula: Published Count ÷ Created Count × 100

    Excludes languages with zero Created Count (NULLIF guard).
    Sorted descending by publish rate; zero-publish languages at bottom.

    Returns
    -------
    table      : columns ["Language", "Published Count", "Created Count",
                          "Language_Publish_Rate_%"]
    chart_type : "bar"
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "Language"                                                      AS "Language",
            "Published Count"                                               AS "Published Count",
            "Created Count"                                                 AS "Created Count",
            ROUND(
                100.0 * "Published Count" / NULLIF("Created Count", 0),
                2
            )                                                               AS "Language_Publish_Rate_%"
        FROM by_language
        ORDER BY "Language_Publish_Rate_%" DESC NULLS LAST
    """).arrow()
    con.close()
    return table, "bar"


# ---------------------------------------------------------------------------
# Plot — KPI-15 Language Upload Share
# ---------------------------------------------------------------------------

def get_language_upload_share() -> tuple[pa.Table, str]:
    """
    KPI-15: Language Upload Share — bar chart of each language's share
    of total platform uploads.
    Formula: Uploaded Count ÷ SUM(All Uploaded) × 100

    Uses a window function so shares are computed in a single pass.
    Sorted descending by upload share.

    Returns
    -------
    table      : columns ["Language", "Uploaded Count",
                          "Language_Upload_Share_%"]
    chart_type : "bar"
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "Language"                                                      AS "Language",
            "Uploaded Count"                                                AS "Uploaded Count",
            ROUND(
                100.0 * "Uploaded Count" / NULLIF(SUM("Uploaded Count") OVER (), 0),
                2
            )                                                               AS "Language_Upload_Share_%"
        FROM by_language
        ORDER BY "Language_Upload_Share_%" DESC
    """).arrow()
    con.close()
    return table, "bar"
