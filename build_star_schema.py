import duckdb
from config import DB_PATH

conn = duckdb.connect(DB_PATH)


def create_table(conn, table_name, sql):
    try:
        conn.execute(sql)
        count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"OK  {table_name:30s} {count:>6} rows")
    except Exception as e:
        print(f"Error creating table {table_name}: {e}")


def dur_to_secs(col):
    return f"""(
        CAST(SPLIT_PART(CAST({col} AS VARCHAR), chr(58), 1) AS INTEGER) * 3600
        + CAST(SPLIT_PART(CAST({col} AS VARCHAR), chr(58), 2) AS INTEGER) * 60
        + CAST(SPLIT_PART(CAST({col} AS VARCHAR), chr(58), 3) AS INTEGER)
    )"""


# ── Dimensions ────────────────────────────────────────────────────────────────

create_table(conn, "dim_channel", """
    CREATE OR REPLACE TABLE dim_channel AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY Channel) AS channel_id,
        Channel AS channel_name
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
        ROW_NUMBER() OVER (ORDER BY User) AS user_id,
        User AS user_name
    FROM (
        SELECT DISTINCT User
        FROM raw_channel_user
        WHERE User IS NOT NULL
    )
    ORDER BY User
""")

create_table(conn, "dim_input_type", """
    CREATE OR REPLACE TABLE dim_input_type AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY "Input Type") AS input_type_id,
        "Input Type" AS input_type
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
        "Output Type" AS output_type
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
        Language AS language_code,
        CASE Language
            WHEN 'en'  THEN 'English'
            WHEN 'hi'  THEN 'Hindi'
            WHEN 'mix' THEN 'Mixed'
            WHEN 'es'  THEN 'Spanish'
            WHEN 'ar'  THEN 'Arabic'
            WHEN 'mr'  THEN 'Marathi'
            ELSE Language
        END AS language_name
    FROM (
        SELECT DISTINCT Language
        FROM raw_language
        WHERE Language IS NOT NULL
    )
""")

create_table(conn, "dim_platform", """
    CREATE OR REPLACE TABLE dim_platform AS
    SELECT
        ROW_NUMBER() OVER () AS platform_id,
        platform_name
    FROM (
        VALUES
            ('YouTube'),
            ('Facebook'),
            ('Shorts'),
            ('Facebook-Reels'),
            ('Instagram'),
            ('LinkedIn'),
            ('X'),
            ('Threads')
    ) AS t(platform_name)
""")


# ── Fact ──────────────────────────────────────────────────────────────────────

create_table(conn, "fact_video_jobs", """
    CREATE OR REPLACE TABLE fact_video_jobs AS
    SELECT
        v."Video ID"                                        AS video_id,
        dc.channel_id                                       AS channel_id,
        du.user_id                                          AS user_id,
        dp.platform_id                                      AS platform_id,
        CASE WHEN v.Published = 'Yes' THEN 1 ELSE 0 END    AS is_published,
        COALESCE(v."Team Name", 'Unknown')                  AS team_name,
        COALESCE(v.Type, 'Unknown')                         AS input_type,
        v.headline                                          AS headline,
        v.Source                                            AS source,
        v."Published Platform"                              AS published_platform,
        v."Published URL"                                   AS published_url
    FROM raw_video_list v
    LEFT JOIN dim_user     du ON du.user_name     = v."Uploaded By"
    LEFT JOIN dim_channel  dc ON dc.channel_name  = v."Uploaded By"
    LEFT JOIN dim_platform dp ON dp.platform_name = v."Published Platform"
""")


# ── Bridge ────────────────────────────────────────────────────────────────────

create_table(conn, "bridge_user_channel", f"""
    CREATE OR REPLACE TABLE bridge_user_channel AS
    SELECT
        du.user_id,
        dc.channel_id,
        cu.User                                             AS user_name,
        cu.Channel                                          AS channel_name,

        cu."Uploaded Count"                                 AS uploaded_count,
        cu."CREATED COUNT"                                  AS created_count,
        cu."Published Count"                                AS published_count,
        cu."Created Count" - cu."Published Count"           AS drop_off_count,

        ROUND(cu."Published Count" * 100.0 / NULLIF(cu."Created Count", 0), 2)
                                                            AS published_rate_pct,

        cu."Uploaded Duration (hh:mm:ss)"                  AS uploaded_duration,
        cu."CREATED Duration (hh:mm:ss)"                   AS created_duration,
        cu."Published Duration (hh:mm:ss)"                 AS published_duration,

        {dur_to_secs('cu."Uploaded Duration (hh:mm:ss)"')} AS uploaded_secs,
        {dur_to_secs('cu."Created Duration (hh:mm:ss)"')}  AS created_secs,
        {dur_to_secs('cu."Published Duration (hh:mm:ss)"')} AS published_secs,

        ROUND({dur_to_secs('cu."Uploaded Duration (hh:mm:ss)"')} / 3600.0, 2) AS uploaded_hours,
        ROUND({dur_to_secs('cu."Created Duration (hh:mm:ss)"')}  / 3600.0, 2) AS created_hours,
        ROUND({dur_to_secs('cu."Published Duration (hh:mm:ss)"')} / 3600.0, 2) AS published_hours

    FROM raw_channel_user cu
    JOIN dim_user    du ON cu.User    = du.user_name
    JOIN dim_channel dc ON cu.Channel = dc.channel_name
    WHERE cu.User    IS NOT NULL
    AND   cu.Channel IS NOT NULL
    ORDER BY uploaded_secs DESC
""")


# ── Summaries ─────────────────────────────────────────────────────────────────

create_table(conn, "summary_user", """
    CREATE OR REPLACE TABLE summary_user AS
    SELECT
        user_id,
        user_name,
        COUNT(DISTINCT channel_id)  AS channels_count,
        SUM(uploaded_count)         AS total_uploaded_count,
        SUM(created_count)          AS total_created_count,
        SUM(published_count)        AS total_published_count,
        SUM(drop_off_count)         AS total_drop_off_count
    FROM bridge_user_channel
    GROUP BY user_id, user_name
    ORDER BY total_uploaded_count DESC
""")

create_table(conn, "summary_user_totals", f"""
    CREATE OR REPLACE TABLE summary_user_totals AS
    SELECT
        du.user_id,
        ru.User                                                 AS user_name,
        ru."Uploaded Count"                                     AS uploaded_count,
        ru."Created Count"                                      AS created_count,
        ru."Published Count"                                    AS published_count,
        ru."Created Count" - ru."Published Count"               AS drop_off_count,
        ROUND(ru."Published Count" * 100.0 / NULLIF(ru."Created Count", 0), 2)
                                                                AS published_rate_pct,
        {dur_to_secs('ru."Uploaded Duration (hh:mm:ss)"')}     AS uploaded_secs,
        {dur_to_secs('ru."Created Duration (hh:mm:ss)"')}      AS created_secs,
        {dur_to_secs('ru."Published Duration (hh:mm:ss)"')}    AS published_secs,
        ROUND({dur_to_secs('ru."Uploaded Duration (hh:mm:ss)"')} / 3600.0, 2) AS uploaded_hours,
        ROUND({dur_to_secs('ru."Created Duration (hh:mm:ss)"')}  / 3600.0, 2) AS created_hours,
        ROUND({dur_to_secs('ru."Published Duration (hh:mm:ss)"')} / 3600.0, 2) AS published_hours
    FROM raw_user ru
    LEFT JOIN dim_user du ON du.user_name = ru.User
""")

create_table(conn, "summary_channel_duration", f"""
    CREATE OR REPLACE TABLE summary_channel_duration AS
    SELECT
        dc.channel_id,
        cd.Channels                                             AS channel_name,

        {dur_to_secs('cd."Facebook Duration"')}                AS facebook_secs,
        {dur_to_secs('cd."Instagram Duration"')}               AS instagram_secs,
        {dur_to_secs('cd."Linkedin Duration"')}                AS linkedin_secs,
        {dur_to_secs('cd."Reels Duration"')}                   AS reels_secs,
        {dur_to_secs('cd."Shorts Duration"')}                  AS shorts_secs,
        {dur_to_secs('cd."X Duration"')}                       AS x_secs,
        {dur_to_secs('cd."Youtube Duration"')}                 AS youtube_secs,
        {dur_to_secs('cd."Threads Duration"')}                 AS threads_secs,

        ROUND({dur_to_secs('cd."Facebook Duration"')}  / 3600.0, 2) AS facebook_hours,
        ROUND({dur_to_secs('cd."Instagram Duration"')} / 3600.0, 2) AS instagram_hours,
        ROUND({dur_to_secs('cd."Linkedin Duration"')}  / 3600.0, 2) AS linkedin_hours,
        ROUND({dur_to_secs('cd."Reels Duration"')}     / 3600.0, 2) AS reels_hours,
        ROUND({dur_to_secs('cd."Shorts Duration"')}    / 3600.0, 2) AS shorts_hours,
        ROUND({dur_to_secs('cd."X Duration"')}         / 3600.0, 2) AS x_hours,
        ROUND({dur_to_secs('cd."Youtube Duration"')}   / 3600.0, 2) AS youtube_hours,
        ROUND({dur_to_secs('cd."Threads Duration"')}   / 3600.0, 2) AS threads_hours

    FROM raw_channel_duration cd
    LEFT JOIN dim_channel dc ON dc.channel_name = cd.Channels
""")


# ── Done ──────────────────────────────────────────────────────────────────────

tables = conn.execute("SHOW TABLES").df()
print("\nTables in database:")
print(tables.to_string(index=False))

conn.close()
print("\nDone.")