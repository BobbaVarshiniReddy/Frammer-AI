"""
raw_monthly_duration_plots.py
"""

import pyarrow as pa
from database import get_connection


def _dur_to_hours(col: str) -> str:
    """Convert hh:mm:ss column (may be TIME type) to hours as FLOAT."""
    return f"""(
        split_part(CAST({col} AS VARCHAR), ':', 1)::FLOAT +
        split_part(CAST({col} AS VARCHAR), ':', 2)::FLOAT / 60 +
        split_part(CAST({col} AS VARCHAR), ':', 3)::FLOAT / 3600
    )"""


# ─────────────────────────────────────────────────────────────────────────────
# Plot 1 — Grouped duration vs month
# ─────────────────────────────────────────────────────────────────────────────

def get_grouped_duration_vs_month() -> list:
    con = get_connection()
    table = con.execute(f"""
        SELECT
            "Month",
            ROUND({_dur_to_hours('"Total Uploaded Duration"')},  2) AS "Uploaded_hours",
            ROUND({_dur_to_hours('"Total Created Duration"')},   2) AS "Created_hours",
            ROUND({_dur_to_hours('"Total Published Duration"')}, 2) AS "Published_hours"
        FROM raw_monthly_duration
        ORDER BY strptime("Month", '%b, %Y')
    """).arrow()
    con.close()
    return [
        [table, "grouped_bar"],
        [table, "line"],
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Plot 2 — Top 5 months by upload duration
# ─────────────────────────────────────────────────────────────────────────────

def get_top5_uploaded_duration() -> list:
    con = get_connection()
    table = con.execute(f"""
        SELECT
            "Month",
            ROUND({_dur_to_hours('"Total Uploaded Duration"')}, 2) AS "Uploaded_hours"
        FROM raw_monthly_duration
        ORDER BY "Uploaded_hours" DESC
        LIMIT 5
    """).arrow()
    con.close()
    return [
        [table, "bar"],
        [table, "pie"],
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Plot 3 — Top 5 months by published duration
# ─────────────────────────────────────────────────────────────────────────────

def get_top5_published_duration() -> list:
    con = get_connection()
    table = con.execute(f"""
        SELECT
            "Month",
            ROUND({_dur_to_hours('"Total Published Duration"')}, 2) AS "Published_hours"
        FROM raw_monthly_duration
        ORDER BY "Published_hours" DESC
        LIMIT 5
    """).arrow()
    con.close()
    return [
        [table, "bar"],
        [table, "pie"],
    ]