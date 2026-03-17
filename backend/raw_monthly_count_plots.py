"""
plots.py
"""

import pyarrow as pa
from database import get_connection

#-------------------------------------------------------------------------
# Plot 1 - Total Created vs Month
#-------------------------------------------------------------------------

def get_total_created_vs_month() -> tuple[pa.Table, str]:
    """
    Plot 1 : Total Created vs Month (time series)

    Returns
    -------
    table      : columns ["Month", "Total Created"]
    chart_type : "line"
    
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "Month"            AS "Month",
            "Total Created"    AS "Total Created"
        FROM raw_monthly_count
        ORDER BY "Month"
    """).arrow()
    con.close()
    return table, "line"


#-------------------------------------------------------------------------
# Plot 2 - Total Uploaded vs Month
#-------------------------------------------------------------------------


def get_total_uploaded_vs_month() -> tuple[pa.Table, str]:
    """
    Plot 2 : Total Uploaded vs Month (time series)

    Returns
    -------
    table      : columns ["Month", "Total Uploaded"]
    chart_type : "line"

    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "Month"             AS "Month",
            "Total Uploaded"    AS "Total Uploaded"
        FROM raw_monthly_count
        ORDER BY "Month"
    """).arrow()
    con.close()
    return table, "line"

#-------------------------------------------------------------------------
# Plot 3 - Total Published vs Month
#-------------------------------------------------------------------------

def get_total_published_vs_month() -> tuple[pa.Table, str]:
    """
    Plot 3 : Total Published vs Month (time series)

    Returns
    -------
    table      : columns ["Month", "Total Published"]
    chart_type : "line"

    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "Month"              AS "Month",
            "Total Published"    AS "Total Published"
        FROM raw_monthly_count
        ORDER BY "Month"
    """).arrow()
    con.close()
    return table, "line"

#-------------------------------------------------------------------------
# Plot 4 - Month over Month Increase in Published
#-------------------------------------------------------------------------

def get_mom_increase_published() -> tuple[pa.Table, str]:
    """
    Plot 4 : Month-over-Month increase in Published

    Returns
    -------
    table      : columns ["Month", "MoM_Published_Increase"]
    chart_type : "line"

    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "Month"   AS "Month",
            "Total Published" - LAG("Total Published") OVER (ORDER BY "Month")     AS "MoM_Published_Increase"
        FROM raw_monthly_count
        ORDER BY "Month"
    """).arrow()
    con.close()
    return table, "line"

#-------------------------------------------------------------------------
# Plot 5 - Month over Month Increase in Uploaded
#-------------------------------------------------------------------------

def get_mom_increase_uploaded() -> tuple[pa.Table, str]:
    """
    Plot: Month-over-Month increase in Uploads

     Returns
    -------
    table      : columns ["Month", "MoM_Upload_Increase"]
    chart_type : "line"

    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "Month"  AS "Month",
            "Total Uploaded"
            - LAG("Total Uploaded") OVER (ORDER BY "Month")
            AS "MoM_Upload_Increase"
        FROM raw_monthly_count
        ORDER BY "Month"
    """).arrow()
    con.close()
    return table, "line"