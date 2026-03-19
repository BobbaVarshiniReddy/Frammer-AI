"""
plots.py
"""

import pyarrow as pa
from database import get_connection

#-------------------------------------------------------------------------
# Plot 1 - Grouped Bar Chart (Durations in Hours)
#-------------------------------------------------------------------------
def get_grouped_duration_vs_month() -> list:
    """
    Plot 6 : Grouped Bar Chart (Uploaded vs Created vs Published)
    Values are in HOURS
    Returns multiple chart options in list format
    """
    con = get_connection()
    table = con.execute("""
        WITH parsed AS (
            SELECT
                "Month",

                -- Uploaded → Hours
                (split_part("Total Uploaded Duration", ':', 1)::FLOAT +
                 split_part("Total Uploaded Duration", ':', 2)::FLOAT / 60 +
                 split_part("Total Uploaded Duration", ':', 3)::FLOAT / 3600)
                 AS "Uploaded_hours",

                -- Created → Hours
                (split_part("Total Created Duration", ':', 1)::FLOAT +
                 split_part("Total Created Duration", ':', 2)::FLOAT / 60 +
                 split_part("Total Created Duration", ':', 3)::FLOAT / 3600)
                 AS "Created_hours",

                -- Published → Hours
                (split_part("Total Published Duration", ':', 1)::FLOAT +
                 split_part("Total Published Duration", ':', 2)::FLOAT / 60 +
                 split_part("Total Published Duration", ':', 3)::FLOAT / 3600)
                 AS "Published_hours",

                strptime("Month", '%b, %Y') AS month_date

            FROM raw_monthly_duration
        )
        SELECT
            "Month",
            "Uploaded_hours",
            "Created_hours",
            "Published_hours"
        FROM parsed
        ORDER BY month_date
    """).arrow()
    con.close()

    return [
        [table, "grouped_bar"],  # comparison view
        [table, "line"]          # trend view
    ]


#-------------------------------------------------------------------------
# Plot 2 - Top 5 Months with Highest Upload Duration (Hours)
#-------------------------------------------------------------------------
def get_top5_uploaded_duration() -> list:
    """
    Plot 7 : Top 5 Months by Upload Duration
    Values are in HOURS
    Returns multiple chart options in list format
    """
    con = get_connection()
    table = con.execute("""
        WITH parsed AS (
            SELECT
                "Month",
                (split_part("Total Uploaded Duration", ':', 1)::FLOAT +
                 split_part("Total Uploaded Duration", ':', 2)::FLOAT / 60 +
                 split_part("Total Uploaded Duration", ':', 3)::FLOAT / 3600)
                 AS "Uploaded_hours"
            FROM raw_monthly_duration
        )
        SELECT *
        FROM parsed
        ORDER BY "Uploaded_hours" DESC
        LIMIT 5
    """).arrow()
    con.close()

    return [
        [table, "bar"],  # top comparison
        [table, "pie"]   # distribution view
    ]


#-------------------------------------------------------------------------
# Plot 3 - Top 5 Months with Highest Published Duration (Hours)
#-------------------------------------------------------------------------
def get_top5_published_duration() -> list:
    """
    Plot 8 : Top 5 Months by Published Duration
    Values are in HOURS
    Returns multiple chart options in list format
    """
    con = get_connection()
    table = con.execute("""
        WITH parsed AS (
            SELECT
                "Month",
                (split_part("Total Published Duration", ':', 1)::FLOAT +
                 split_part("Total Published Duration", ':', 2)::FLOAT / 60 +
                 split_part("Total Published Duration", ':', 3)::FLOAT / 3600)
                 AS "Published_hours"
            FROM raw_monthly_duration
        )
        SELECT *
        FROM parsed
        ORDER BY "Published_hours" DESC
        LIMIT 5
    """).arrow()
    con.close()

    return [
        [table, "bar"],  # top comparison
        [table, "pie"]   # distribution view
    ]
