import duckdb
from src.utils.config import DB_PATH

conn = duckdb.connect(DB_PATH)

# ─────────────────────────────────────────────
# HELPER FUNCTIONS
# Defined at top before any usage
# ─────────────────────────────────────────────

def create_table(conn, table_name, sql):
    try:
        conn.execute(sql)
        count = conn.execute(
            f"SELECT COUNT(*) FROM {table_name}"
        ).fetchone()[0]
        print(f"OK  {table_name:35s} {count:>6} rows")
    except Exception as e:
        print(f"Error creating table {table_name}: {e}")


def dur_to_secs(col):
    # Converts hh:mm:ss to total seconds
    # CAST to VARCHAR prevents TIME type error
    return f"""(
        CAST(SPLIT_PART(CAST({col} AS VARCHAR), chr(58), 1) AS INTEGER) * 3600
        + CAST(SPLIT_PART(CAST({col} AS VARCHAR), chr(58), 2) AS INTEGER) * 60
        + CAST(SPLIT_PART(CAST({col} AS VARCHAR), chr(58), 3) AS INTEGER)
    )"""


# ─────────────────────────────────────────────
# DIMENSION TABLES
# ─────────────────────────────────────────────
print("\n--- DIMENSION TABLES ---")

create_table(conn, "dim_channel", """
    CREATE OR REPLACE TABLE dim_channel AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY channel_name) AS channel_id,
        channel_name
    FROM (
        SELECT DISTINCT Channel AS channel_name
        FROM raw_channel_summary
        WHERE Channel IS NOT NULL
    )
""")

create_table(conn, "dim_user", """
    CREATE OR REPLACE TABLE dim_user AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY user_name) AS user_id,
        user_name
    FROM (
        SELECT DISTINCT User AS user_name
        FROM raw_channel_user
        WHERE User IS NOT NULL
    )
""")

create_table(conn, "dim_input_type", """
    CREATE OR REPLACE TABLE dim_input_type AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY input_type) AS input_type_id,
        input_type
    FROM (
        SELECT DISTINCT "Input Type" AS input_type
        FROM raw_input_type
        WHERE "Input Type" IS NOT NULL
    )
""")

create_table(conn, "dim_output_type", """
    CREATE OR REPLACE TABLE dim_output_type AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY output_type) AS output_type_id,
        output_type
    FROM (
        SELECT DISTINCT "Output Type" AS output_type
        FROM raw_output_type
        WHERE "Output Type" IS NOT NULL
    )
""")

create_table(conn, "dim_language", """
    CREATE OR REPLACE TABLE dim_language AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY language_code) AS language_id,
        language_code,
        CASE language_code
            WHEN 'en'  THEN 'English'
            WHEN 'hi'  THEN 'Hindi'
            WHEN 'mix' THEN 'Mixed'
            WHEN 'es'  THEN 'Spanish'
            WHEN 'ar'  THEN 'Arabic'
            WHEN 'mr'  THEN 'Marathi'
            ELSE language_code
        END AS language_name
    FROM (
        SELECT DISTINCT Language AS language_code
        FROM raw_language
        WHERE Language IS NOT NULL
    )
""")

# ─────────────────────────────────────────────
# FACT TABLE
# ─────────────────────────────────────────────
print("\n--- FACT TABLE ---")

create_table(conn, "fact_video_jobs", """
    CREATE OR REPLACE TABLE fact_video_jobs AS
    SELECT
        v."Video ID"                       AS video_id,
        du.user_id                         AS user_id,
        CASE
            WHEN v.Published = 'Yes' THEN 1
            ELSE 0
        END                                AS is_published,
        COALESCE(v."Team Name", 'Unknown') AS team_name,
        COALESCE(v.Type, 'Unknown')        AS input_type,
        v.Headline                         AS headline,
        v.Source                           AS source_url,
        v."Published Platform"             AS published_platform,
        v."Published URL"                  AS published_url,
        v."Uploaded By"                    AS uploaded_by
    FROM raw_video_list v
    LEFT JOIN dim_user du
        ON du.user_name = v."Uploaded By"
""")

# ─────────────────────────────────────────────
# BRIDGE TABLE
# ─────────────────────────────────────────────
print("\n--- BRIDGE TABLE ---")

# Pre-compute duration expressions
upl = dur_to_secs('cu."Uploaded Duration (hh:mm:ss)"')
cre = dur_to_secs('cu."Created Duration (hh:mm:ss)"')
pub = dur_to_secs('cu."Published Duration (hh:mm:ss)"')

create_table(conn, "bridge_user_channel", f"""
    CREATE OR REPLACE TABLE bridge_user_channel AS
    SELECT
        du.user_id,
        dc.channel_id,
        cu.User                             AS user_name,
        cu.Channel                          AS channel_name,
        cu."Uploaded Count"                 AS uploaded_count,
        cu."Created Count"                  AS created_count,
        cu."Published Count"                AS published_count,
        cu."Created Count"
            - cu."Published Count"          AS drop_off_count,
        ROUND(
            cu."Published Count" * 100.0
            / NULLIF(cu."Created Count", 0)
        , 2)                                AS publish_rate_pct,
        CAST(cu."Uploaded Duration (hh:mm:ss)"
            AS VARCHAR)                     AS uploaded_duration_text,
        CAST(cu."Created Duration (hh:mm:ss)"
            AS VARCHAR)                     AS created_duration_text,
        CAST(cu."Published Duration (hh:mm:ss)"
            AS VARCHAR)                     AS published_duration_text,
        {upl}                               AS uploaded_secs,
        {cre}                               AS created_secs,
        {pub}                               AS published_secs,
        ROUND({upl} / 3600.0, 2)           AS uploaded_hours,
        ROUND({cre} / 3600.0, 2)           AS created_hours,
        ROUND({pub} / 3600.0, 2)           AS published_hours
    FROM raw_channel_user cu
    JOIN dim_user du
        ON cu.User = du.user_name
    JOIN dim_channel dc
        ON cu.Channel = dc.channel_name
    WHERE cu.User    IS NOT NULL
    AND   cu.Channel IS NOT NULL
    ORDER BY uploaded_secs DESC
""")

# ─────────────────────────────────────────────
# SUMMARY TABLES
# ─────────────────────────────────────────────
print("\n--- SUMMARY TABLES ---")

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

cs_upl = dur_to_secs('cs."Uploaded Duration (hh:mm:ss)"')
cs_cre = dur_to_secs('cs."Created Duration (hh:mm:ss)"')
cs_pub = dur_to_secs('cs."Published Duration (hh:mm:ss)"')

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
        CAST(cs."Uploaded Duration (hh:mm:ss)"
            AS VARCHAR)                         AS uploaded_duration_text,
        CAST(cs."Created Duration (hh:mm:ss)"
            AS VARCHAR)                         AS created_duration_text,
        CAST(cs."Published Duration (hh:mm:ss)"
            AS VARCHAR)                         AS published_duration_text,
        {cs_upl}                                AS uploaded_secs,
        {cs_cre}                                AS created_secs,
        {cs_pub}                                AS published_secs,
        ROUND({cs_upl} / 3600.0, 2)            AS uploaded_hours,
        ROUND({cs_cre} / 3600.0, 2)            AS created_hours,
        ROUND({cs_pub} / 3600.0, 2)            AS published_hours
    FROM raw_channel_summary cs
    LEFT JOIN dim_channel dc
        ON cs.Channel = dc.channel_name
    ORDER BY cs."Uploaded Count" DESC
""")

mc_upl = dur_to_secs('md."Total Uploaded Duration"')
mc_cre = dur_to_secs('md."Total Created Duration"')
mc_pub = dur_to_secs('md."Total Published Duration"')

create_table(conn, "summary_monthly", f"""
    CREATE OR REPLACE TABLE summary_monthly AS
    SELECT
        mc.Month                                AS month,
        -- month_order: integer 1-12 for correct
        -- chronological sorting in window functions.
        -- Always use ORDER BY month_order, never
        -- ORDER BY month (month is text, sorts
        -- alphabetically not chronologically).
        CASE mc.Month
            WHEN 'Mar, 2025' THEN 1
            WHEN 'Apr, 2025' THEN 2
            WHEN 'May, 2025' THEN 3
            WHEN 'Jun, 2025' THEN 4
            WHEN 'Jul, 2025' THEN 5
            WHEN 'Aug, 2025' THEN 6
            WHEN 'Sep, 2025' THEN 7
            WHEN 'Oct, 2025' THEN 8
            WHEN 'Nov, 2025' THEN 9
            WHEN 'Dec, 2025' THEN 10
            WHEN 'Jan, 2026' THEN 11
            WHEN 'Feb, 2026' THEN 12
            ELSE 99
        END                                     AS month_order,
        mc."Total Uploaded"                     AS uploaded_count,
        mc."Total Created"                      AS created_count,
        mc."Total Published"                    AS published_count,
        mc."Total Created"
            - mc."Total Published"              AS drop_off_count,
        ROUND(
            mc."Total Published" * 100.0
            / NULLIF(mc."Total Created", 0)
        , 2)                                    AS publish_rate_pct,
        CAST(md."Total Uploaded Duration"
            AS VARCHAR)                         AS uploaded_duration_text,
        CAST(md."Total Created Duration"
            AS VARCHAR)                         AS created_duration_text,
        CAST(md."Total Published Duration"
            AS VARCHAR)                         AS published_duration_text,
        {mc_upl}                                AS uploaded_secs,
        {mc_cre}                                AS created_secs,
        {mc_pub}                                AS published_secs,
        ROUND({mc_upl} / 3600.0, 2)            AS uploaded_hours,
        ROUND({mc_cre} / 3600.0, 2)            AS created_hours,
        ROUND({mc_pub} / 3600.0, 2)            AS published_hours
    FROM raw_monthly_count mc
    LEFT JOIN raw_monthly_duration md
        ON mc.Month = md.Month
    ORDER BY month_order
""")

it_upl = dur_to_secs('"Uploaded Duration (hh:mm:ss)"')
it_cre = dur_to_secs('"Created Duration (hh:mm:ss)"')
it_pub = dur_to_secs('"Published Duration (hh:mm:ss)"')

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
        CAST("Uploaded Duration (hh:mm:ss)"
            AS VARCHAR)                         AS uploaded_duration_text,
        CAST("Created Duration (hh:mm:ss)"
            AS VARCHAR)                         AS created_duration_text,
        CAST("Published Duration (hh:mm:ss)"
            AS VARCHAR)                         AS published_duration_text,
        {it_upl}                                AS uploaded_secs,
        {it_cre}                                AS created_secs,
        {it_pub}                                AS published_secs,
        ROUND({it_upl} / 3600.0, 2)            AS uploaded_hours,
        ROUND({it_cre} / 3600.0, 2)            AS created_hours,
        ROUND({it_pub} / 3600.0, 2)            AS published_hours
    FROM raw_input_type
    ORDER BY "Uploaded Count" DESC
""")

ot_upl = dur_to_secs('"Uploaded Duration (hh:mm:ss)"')
ot_cre = dur_to_secs('"Created Duration (hh:mm:ss)"')
ot_pub = dur_to_secs('"Published Duration (hh:mm:ss)"')

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
        CAST("Uploaded Duration (hh:mm:ss)"
            AS VARCHAR)                         AS uploaded_duration_text,
        CAST("Created Duration (hh:mm:ss)"
            AS VARCHAR)                         AS created_duration_text,
        CAST("Published Duration (hh:mm:ss)"
            AS VARCHAR)                         AS published_duration_text,
        {ot_upl}                                AS uploaded_secs,
        {ot_cre}                                AS created_secs,
        {ot_pub}                                AS published_secs,
        ROUND({ot_upl} / 3600.0, 2)            AS uploaded_hours,
        ROUND({ot_cre} / 3600.0, 2)            AS created_hours,
        ROUND({ot_pub} / 3600.0, 2)            AS published_hours
    FROM raw_output_type
    ORDER BY "Created Count" DESC
""")

la_upl = dur_to_secs('rl."Uploaded Duration (hh:mm:ss)"')
la_cre = dur_to_secs('rl."Created Duration (hh:mm:ss)"')
la_pub = dur_to_secs('rl."Published Duration (hh:mm:ss)"')

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
        CAST(rl."Uploaded Duration (hh:mm:ss)"
            AS VARCHAR)                         AS uploaded_duration_text,
        CAST(rl."Created Duration (hh:mm:ss)"
            AS VARCHAR)                         AS created_duration_text,
        CAST(rl."Published Duration (hh:mm:ss)"
            AS VARCHAR)                         AS published_duration_text,
        {la_upl}                                AS uploaded_secs,
        {la_cre}                                AS created_secs,
        {la_pub}                                AS published_secs,
        ROUND({la_upl} / 3600.0, 2)            AS uploaded_hours,
        ROUND({la_cre} / 3600.0, 2)            AS created_hours,
        ROUND({la_pub} / 3600.0, 2)            AS published_hours
    FROM raw_language rl
    LEFT JOIN dim_language dl
        ON rl.Language = dl.language_code
    ORDER BY rl."Uploaded Count" DESC
""")

# ─────────────────────────────────────────────
# STANDALONE TABLE 1: PLATFORM PUBLISHING
# Source: channel-wise-publishing.csv
# Grain: 1 row per channel × platform pair
# Used for: Platform Distribution KPI only
# NOT for general channel/user questions
# ─────────────────────────────────────────────
print("\n--- STANDALONE TABLES ---")

create_table(conn, "summary_platform", """
    CREATE OR REPLACE TABLE summary_platform AS
    SELECT
        TRIM(Channels)          AS channel_name,
        platform_name,
        publish_count
    FROM raw_channel_platform
    UNPIVOT (
        publish_count FOR platform_name IN (
            Facebook,
            Instagram,
            Linkedin,
            Reels,
            Shorts,
            X,
            Youtube,
            Threads
        )
    )
    WHERE Channels IS NOT NULL
    ORDER BY channel_name, publish_count DESC
""")

# ─────────────────────────────────────────────
# STANDALONE TABLE 2: VIDEO-LEVEL FACT TABLE
# Source: video_list_data_obfuscated.csv
# Grain: 1 row per video output (14,918 rows)
# Used for: Data quality KPIs only
# NOT for general upload/publish questions
# ─────────────────────────────────────────────

create_table(conn, "fact_video_jobs", """
    CREATE OR REPLACE TABLE fact_video_jobs AS
    SELECT
        v."Video ID"                       AS video_id,
        du.user_id                         AS user_id,
        CASE
            WHEN v.Published = 'Yes' THEN 1
            ELSE 0
        END                                AS is_published,
        COALESCE(v."Team Name", 'Unknown') AS team_name,
        COALESCE(v.Type, 'Unknown')        AS input_type,
        v.Headline                         AS headline,
        v.Source                           AS source_url,
        v."Published Platform"             AS published_platform,
        v."Published URL"                  AS published_url,
        v."Uploaded By"                    AS uploaded_by
    FROM raw_video_list v
    LEFT JOIN dim_user du
        ON du.user_name = v."Uploaded By"
""")

# ─────────────────────────────────────────────
# DATA QUALITY TABLE
# Pre-aggregated from fact_video_jobs
# Single-row summary — query with SELECT *
# ─────────────────────────────────────────────
print("\n--- DATA QUALITY TABLE ---")

create_table(conn, "data_quality", """
    CREATE OR REPLACE TABLE data_quality AS
    SELECT
        COUNT(*)                                AS total_videos,
        COUNT_IF(team_name = 'Unknown')         AS unknown_team_count,
        ROUND(
            COUNT_IF(team_name = 'Unknown')
            * 100.0 / COUNT(*)
        , 2)                                    AS unknown_team_pct,
        COUNT_IF(is_published = 1)              AS total_published,
        COUNT_IF(
            is_published = 1
            AND published_url IS NOT NULL
        )                                       AS published_with_url,
        ROUND(
            COUNT_IF(
                is_published = 1
                AND published_url IS NOT NULL
            ) * 100.0
            / NULLIF(COUNT_IF(is_published = 1), 0)
        , 2)                                    AS url_completeness_pct,
        COUNT(*) - COUNT(DISTINCT video_id)     AS duplicate_video_ids
    FROM fact_video_jobs
""")

# ─────────────────────────────────────────────
# VERIFICATION
# ─────────────────────────────────────────────
print("\n" + "=" * 50)
print("VERIFICATION")
print("=" * 50)

print("\nTop 5 users by uploaded hours:")
print(conn.execute("""
    SELECT user_name, channels_count,
           total_uploaded_hours, publish_rate_pct
    FROM summary_user
    ORDER BY total_uploaded_hours DESC
    LIMIT 5
""").df().to_string(index=False))

print("\nChannel drop-off:")
print(conn.execute("""
    SELECT channel_name, uploaded_count,
           drop_off_count, publish_rate_pct,
           uploaded_hours
    FROM summary_channel
    ORDER BY drop_off_count DESC
    LIMIT 5
""").df().to_string(index=False))

print("\nPlatform publish totals:")
print(conn.execute("""
    SELECT platform_name,
           SUM(publish_count) AS total_published
    FROM summary_platform
    GROUP BY platform_name
    ORDER BY total_published DESC
""").df().to_string(index=False))

print("\nData quality:")
print(conn.execute("""
    SELECT * FROM data_quality
""").df().to_string(index=False))

print("\nAll tables:")
tables = conn.execute("SHOW TABLES").df()
clean  = tables[~tables['name'].str.startswith('raw_')]
print(clean.to_string(index=False))

conn.close()
print("\nStep 2 Complete.")