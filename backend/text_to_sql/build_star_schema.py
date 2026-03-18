"""
build_tables.py  —  Create all DuckDB tables from raw sources
=============================================================
Run once (or whenever raw data changes) to (re)build the
dimension, bridge, and summary tables used by nlq_pipeline.

Usage:
    python build_tables.py
"""

import os
import duckdb
from config import DB_PATH


os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
conn = duckdb.connect(DB_PATH)


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def create_table(conn: duckdb.DuckDBPyConnection, table_name: str, sql: str) -> None:
    try:
        conn.execute(sql)
        count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"OK  {table_name:30s} {count:>6} rows")
    except Exception as e:
        print(f"ERROR creating table {table_name}: {e}")


def dur_to_secs(col: str) -> str:
    """Convert a "hh:mm:ss" text column expression → total integer seconds."""
    return f"""(
        CAST(SPLIT_PART(CAST({col} AS VARCHAR), chr(58), 1) AS INTEGER) * 3600
      + CAST(SPLIT_PART(CAST({col} AS VARCHAR), chr(58), 2) AS INTEGER) * 60
      + CAST(SPLIT_PART(CAST({col} AS VARCHAR), chr(58), 3) AS INTEGER)
    )"""


# ─────────────────────────────────────────────────────────────────────────────
# DIMENSION TABLES
# ─────────────────────────────────────────────────────────────────────────────

create_table(conn, "dim_channel", """
    CREATE OR REPLACE TABLE dim_channel AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY Channel) AS channel_id,
        Channel                              AS channel_name
    FROM (
        SELECT DISTINCT Channel
        FROM raw_channel_summary
        WHERE Channel IS NOT NULL
    )
    ORDER BY Channel
""")

create_table(conn, "dim_user", """
    CREATE OR REPLACE TABLE dim_user AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY "User") AS user_id,
        "User"                              AS user_name
    FROM (
        SELECT DISTINCT "User"
        FROM raw_channel_user
        WHERE "User" IS NOT NULL
    )
    ORDER BY "User"
""")

create_table(conn, "dim_input_type", """
    CREATE OR REPLACE TABLE dim_input_type AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY "Input Type") AS input_type_id,
        "Input Type"                               AS input_type
    FROM (
        SELECT DISTINCT "Input Type"
        FROM raw_input_type
        WHERE "Input Type" IS NOT NULL
    )
    ORDER BY "Input Type"
""")

create_table(conn, "dim_output_type", """
    CREATE OR REPLACE TABLE dim_output_type AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY "Output Type") AS output_type_id,
        "Output Type"                               AS output_type
    FROM (
        SELECT DISTINCT "Output Type"
        FROM raw_output_type
        WHERE "Output Type" IS NOT NULL
    )
    ORDER BY "Output Type"
""")

create_table(conn, "dim_language", """
    CREATE OR REPLACE TABLE dim_language AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY Language) AS language_id,
        Language                              AS language_code,
        CASE Language
            WHEN 'en'  THEN 'English'
            WHEN 'hi'  THEN 'Hindi'
            WHEN 'mix' THEN 'Mixed'
            WHEN 'es'  THEN 'Spanish'
            WHEN 'ar'  THEN 'Arabic'
            WHEN 'mr'  THEN 'Marathi'
            ELSE Language
        END                                   AS language_name
    FROM (
        SELECT DISTINCT Language
        FROM raw_language
        WHERE Language IS NOT NULL
    )
""")


# ─────────────────────────────────────────────────────────────────────────────
# BRIDGE TABLE  —  user × channel
# ─────────────────────────────────────────────────────────────────────────────

create_table(conn, "bridge_user_channel", f"""
    CREATE OR REPLACE TABLE bridge_user_channel AS
    SELECT
        du.user_id,
        dc.channel_id,
        cu."User"    AS user_name,
        cu.Channel   AS channel_name,

        cu."Uploaded Count"   AS uploaded_count,
        cu."Created Count"    AS created_count,
        cu."Published Count"  AS published_count,

        cu."Created Count" - cu."Published Count"  AS drop_off_count,

        ROUND(
            cu."Published Count" * 100.0
            / NULLIF(cu."Created Count", 0)
        , 2)                                        AS publish_rate_pct,

        cu."Uploaded Duration (hh:mm:ss)"   AS uploaded_duration_text,
        cu."Created Duration (hh:mm:ss)"    AS created_duration_text,
        cu."Published Duration (hh:mm:ss)"  AS published_duration_text,

        {dur_to_secs('cu."Uploaded Duration (hh:mm:ss)"')}  AS uploaded_secs,
        {dur_to_secs('cu."Created Duration (hh:mm:ss)"')}   AS created_secs,
        {dur_to_secs('cu."Published Duration (hh:mm:ss)"')} AS published_secs,

        ROUND(
            {dur_to_secs('cu."Uploaded Duration (hh:mm:ss)"')} / 3600.0
        , 2)                                        AS uploaded_hours,
        ROUND(
            {dur_to_secs('cu."Created Duration (hh:mm:ss)"')} / 3600.0
        , 2)                                        AS created_hours,
        ROUND(
            {dur_to_secs('cu."Published Duration (hh:mm:ss)"')} / 3600.0
        , 2)                                        AS published_hours

    FROM raw_channel_user cu
    JOIN dim_user    du ON cu."User"   = du.user_name
    JOIN dim_channel dc ON cu.Channel  = dc.channel_name
    WHERE cu."User"   IS NOT NULL
      AND cu.Channel  IS NOT NULL
    ORDER BY uploaded_secs DESC
""")


# ─────────────────────────────────────────────────────────────────────────────
# SUMMARY TABLES
# ─────────────────────────────────────────────────────────────────────────────

create_table(conn, "summary_user", """
    CREATE OR REPLACE TABLE summary_user AS
    SELECT
        user_id,
        user_name,

        COUNT(DISTINCT channel_id)              AS channels_count,

        SUM(uploaded_count)                     AS total_uploaded_count,
        SUM(created_count)                      AS total_created_count,
        SUM(published_count)                    AS total_published_count,
        SUM(drop_off_count)                     AS total_drop_off,

        ROUND(
            SUM(published_count) * 100.0
            / NULLIF(SUM(created_count), 0)
        , 2)                                    AS publish_rate_pct,

        SUM(uploaded_secs)                      AS total_uploaded_secs,
        SUM(created_secs)                       AS total_created_secs,
        SUM(published_secs)                     AS total_published_secs,

        ROUND(SUM(uploaded_secs)  / 3600.0, 2) AS total_uploaded_hours,
        ROUND(SUM(created_secs)   / 3600.0, 2) AS total_created_hours,
        ROUND(SUM(published_secs) / 3600.0, 2) AS total_published_hours,

        ROUND(
            SUM(uploaded_secs) / 3600.0
            / NULLIF(COUNT(DISTINCT channel_id), 0)
        , 2)                                    AS avg_hours_per_channel

    FROM bridge_user_channel
    GROUP BY user_id, user_name
    ORDER BY total_uploaded_hours DESC
""")

create_table(conn, "summary_channel", f"""
    CREATE OR REPLACE TABLE summary_channel AS
    SELECT
        dc.channel_id,
        cs.Channel                              AS channel_name,

        cs."Uploaded Count"                     AS uploaded_count,
        cs."Created Count"                      AS created_count,
        cs."Published Count"                    AS published_count,

        cs."Created Count"
            - cs."Published Count"              AS drop_off_count,

        ROUND(
            cs."Published Count" * 100.0
            / NULLIF(cs."Created Count", 0)
        , 2)                                    AS publish_rate_pct,

        ROUND(
            cs."Created Count" * 1.0
            / NULLIF(cs."Uploaded Count", 0)
        , 2)                                    AS creation_ratio,

        cs."Uploaded Duration (hh:mm:ss)"       AS uploaded_duration_text,
        cs."Created Duration (hh:mm:ss)"        AS created_duration_text,
        cs."Published Duration (hh:mm:ss)"      AS published_duration_text,

        {dur_to_secs('cs."Uploaded Duration (hh:mm:ss)"')}  AS uploaded_secs,
        {dur_to_secs('cs."Created Duration (hh:mm:ss)"')}   AS created_secs,
        {dur_to_secs('cs."Published Duration (hh:mm:ss)"')} AS published_secs,

        ROUND(
            {dur_to_secs('cs."Uploaded Duration (hh:mm:ss)"')} / 3600.0
        , 2)                                    AS uploaded_hours,
        ROUND(
            {dur_to_secs('cs."Created Duration (hh:mm:ss)"')} / 3600.0
        , 2)                                    AS created_hours,
        ROUND(
            {dur_to_secs('cs."Published Duration (hh:mm:ss)"')} / 3600.0
        , 2)                                    AS published_hours

    FROM raw_channel_summary cs
    LEFT JOIN dim_channel dc ON cs.Channel = dc.channel_name
    ORDER BY cs."Uploaded Count" DESC
""")

create_table(conn, "summary_language", f"""
    CREATE OR REPLACE TABLE summary_language AS
    SELECT
        rl.Language                             AS language_code,
        dl.language_name,

        rl."Uploaded Count"                     AS uploaded_count,
        rl."Created Count"                      AS created_count,
        rl."Published Count"                    AS published_count,

        rl."Created Count"
            - rl."Published Count"              AS drop_off_count,

        ROUND(
            rl."Published Count" * 100.0
            / NULLIF(rl."Created Count", 0)
        , 2)                                    AS publish_rate_pct,

        rl."Uploaded Duration (hh:mm:ss)"       AS uploaded_duration_text,
        rl."Created Duration (hh:mm:ss)"        AS created_duration_text,
        rl."Published Duration (hh:mm:ss)"      AS published_duration_text,

        {dur_to_secs('rl."Uploaded Duration (hh:mm:ss)"')}  AS uploaded_secs,
        {dur_to_secs('rl."Created Duration (hh:mm:ss)"')}   AS created_secs,
        {dur_to_secs('rl."Published Duration (hh:mm:ss)"')} AS published_secs,

        ROUND(
            {dur_to_secs('rl."Uploaded Duration (hh:mm:ss)"')} / 3600.0
        , 2)                                    AS uploaded_hours,
        ROUND(
            {dur_to_secs('rl."Created Duration (hh:mm:ss)"')} / 3600.0
        , 2)                                    AS created_hours,
        ROUND(
            {dur_to_secs('rl."Published Duration (hh:mm:ss)"')} / 3600.0
        , 2)                                    AS published_hours

    FROM raw_language rl
    LEFT JOIN dim_language dl ON rl.Language = dl.language_code
    ORDER BY rl."Uploaded Count" DESC
""")

create_table(conn, "summary_output_type", f"""
    CREATE OR REPLACE TABLE summary_output_type AS
    SELECT
        "Output Type"                           AS output_type,

        "Uploaded Count"                        AS uploaded_count,
        "Created Count"                         AS created_count,
        "Published Count"                       AS published_count,

        "Created Count" - "Published Count"     AS drop_off_count,

        ROUND(
            "Published Count" * 100.0
            / NULLIF("Created Count", 0)
        , 2)                                    AS publish_rate_pct,

        "Uploaded Duration (hh:mm:ss)"          AS uploaded_duration_text,
        "Created Duration (hh:mm:ss)"           AS created_duration_text,
        "Published Duration (hh:mm:ss)"         AS published_duration_text,

        {dur_to_secs('"Uploaded Duration (hh:mm:ss)"')}  AS uploaded_secs,
        {dur_to_secs('"Created Duration (hh:mm:ss)"')}   AS created_secs,
        {dur_to_secs('"Published Duration (hh:mm:ss)"')} AS published_secs,

        ROUND(
            {dur_to_secs('"Uploaded Duration (hh:mm:ss)"')} / 3600.0
        , 2)                                    AS uploaded_hours,
        ROUND(
            {dur_to_secs('"Created Duration (hh:mm:ss)"')} / 3600.0
        , 2)                                    AS created_hours,
        ROUND(
            {dur_to_secs('"Published Duration (hh:mm:ss)"')} / 3600.0
        , 2)                                    AS published_hours

    FROM raw_output_type
    ORDER BY "Created Count" DESC
""")

create_table(conn, "summary_input_type", f"""
    CREATE OR REPLACE TABLE summary_input_type AS
    SELECT
        "Input Type"                            AS input_type,

        "Uploaded Count"                        AS uploaded_count,
        "Created Count"                         AS created_count,
        "Published Count"                       AS published_count,

        "Created Count" - "Published Count"     AS drop_off_count,

        ROUND(
            "Published Count" * 100.0
            / NULLIF("Created Count", 0)
        , 2)                                    AS publish_rate_pct,

        "Uploaded Duration (hh:mm:ss)"          AS uploaded_duration_text,
        "Created Duration (hh:mm:ss)"           AS created_duration_text,
        "Published Duration (hh:mm:ss)"         AS published_duration_text,

        {dur_to_secs('"Uploaded Duration (hh:mm:ss)"')}  AS uploaded_secs,
        {dur_to_secs('"Created Duration (hh:mm:ss)"')}   AS created_secs,
        {dur_to_secs('"Published Duration (hh:mm:ss)"')} AS published_secs,

        ROUND(
            {dur_to_secs('"Uploaded Duration (hh:mm:ss)"')} / 3600.0
        , 2)                                    AS uploaded_hours,
        ROUND(
            {dur_to_secs('"Created Duration (hh:mm:ss)"')} / 3600.0
        , 2)                                    AS created_hours,
        ROUND(
            {dur_to_secs('"Published Duration (hh:mm:ss)"')} / 3600.0
        , 2)                                    AS published_hours

    FROM raw_input_type
    ORDER BY "Uploaded Count" DESC
""")

create_table(conn, "summary_monthly", f"""
    CREATE OR REPLACE TABLE summary_monthly AS
    SELECT
        mc.Month                                AS month,

        mc."Total Uploaded"                     AS uploaded_count,
        mc."Total Created"                      AS created_count,
        mc."Total Published"                    AS published_count,

        mc."Total Created"
            - mc."Total Published"              AS drop_off_count,

        ROUND(
            mc."Total Published" * 100.0
            / NULLIF(mc."Total Created", 0)
        , 2)                                    AS publish_rate_pct,

        md."Total Uploaded Duration"            AS uploaded_duration_text,
        md."Total Created Duration"             AS created_duration_text,
        md."Total Published Duration"           AS published_duration_text,

        {dur_to_secs('md."Total Uploaded Duration"')}  AS uploaded_secs,
        {dur_to_secs('md."Total Created Duration"')}   AS created_secs,
        {dur_to_secs('md."Total Published Duration"')} AS published_secs,

        ROUND(
            {dur_to_secs('md."Total Uploaded Duration"')} / 3600.0
        , 2)                                    AS uploaded_hours,
        ROUND(
            {dur_to_secs('md."Total Created Duration"')} / 3600.0
        , 2)                                    AS created_hours,
        ROUND(
            {dur_to_secs('md."Total Published Duration"')} / 3600.0
        , 2)                                    AS published_hours

    FROM raw_monthly_count mc
    LEFT JOIN raw_monthly_duration md ON mc.Month = md.Month
    ORDER BY mc.Month
""")


conn.close()
print("\nAll tables created successfully.")