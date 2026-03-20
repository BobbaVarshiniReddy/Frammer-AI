"""
plots.py
"""

import pyarrow as pa
from database import get_connection

#-------------------------------------------------------------------------
# Plot 1 - Total Created vs Month
#-------------------------------------------------------------------------
def get_total_created_vs_month() -> list:
    """
    Plot 1 : Total Created vs Month (time series)
    Returns multiple chart options
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "Month"            AS "Month",
            "Total Created"    AS "Total Created"
        FROM raw_monthly_count
        ORDER BY strptime("Month", '%b, %y')
    """).arrow()
    con.close()

    return [
        [table, "line"],   # trend line
        [table, "bar"],    # bar comparison
        [table, "area"]    # area view
    ]


#-------------------------------------------------------------------------
# Plot 2 - Total Uploaded vs Month
#-------------------------------------------------------------------------
def get_total_uploaded_vs_month() -> list:
    """
    Plot 2 : Total Uploaded vs Month (time series)
    Returns multiple chart options
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "Month"             AS "Month",
            "Total Uploaded"    AS "Total Uploaded"
        FROM raw_monthly_count
        ORDER BY strptime("Month", '%b, %y')
    """).arrow()
    con.close()

    return [
        [table, "line"],
        [table, "bar"],
        [table, "area"]
    ]


#-------------------------------------------------------------------------
# Plot 3 - Total Published vs Month
#-------------------------------------------------------------------------
def get_total_published_vs_month() -> list:
    """
    Plot 3 : Total Published vs Month (time series)
    Returns multiple chart options
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "Month"              AS "Month",
            "Total Published"    AS "Total Published"
        FROM raw_monthly_count
        ORDER BY strptime("Month", '%b, %y')
    """).arrow()
    con.close()

    return [
        [table, "line"],
        [table, "bar"],
        [table, "area"]
    ]


#-------------------------------------------------------------------------
# Plot 4 - Month over Month Increase in Published
#-------------------------------------------------------------------------
def get_mom_increase_published() -> list:
    """
    Plot 4 : Month-over-Month increase in Published
    Returns multiple chart options
    """
    con = get_connection()
    table = con.execute("""
        WITH ordered_data AS (
            SELECT
                "Month",
                "Total Published",
                strptime("Month", '%b, %y') AS month_date
            FROM raw_monthly_count
        )
        SELECT
            "Month",
            "Total Published"
            - LAG("Total Published") OVER (ORDER BY month_date)
            AS "MoM_Published_Increase"
        FROM ordered_data
        ORDER BY month_date
    """).arrow()
    con.close()

    return [
        [table, "line"],
        [table, "bar"],
        [table, "area"]
    ]


#-------------------------------------------------------------------------
# Plot 5 - Month over Month Increase in Uploaded
#-------------------------------------------------------------------------
def get_mom_increase_uploaded() -> list:
    """
    Plot 5 : Month-over-Month increase in Uploads
    Returns multiple chart options
    """
    con = get_connection()
    table = con.execute("""
        WITH ordered_data AS (
            SELECT
                "Month",
                "Total Uploaded",
                strptime("Month", '%b, %y') AS month_date
            FROM raw_monthly_count
        )
        SELECT
            "Month",
            "Total Uploaded"
            - LAG("Total Uploaded") OVER (ORDER BY month_date)
            AS "MoM_Upload_Increase"
        FROM ordered_data
        ORDER BY month_date
    """).arrow()
    con.close()

    return [
        [table, "line"],
        [table, "bar"],
        [table, "area"]
    ]
