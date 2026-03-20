import duckdb
import os

conn = duckdb.connect("./database/frammer.db")
print("connected to database")


def extract_table_info(conn, table_name):
    """
    Reads one table and returns all
    column information as a dictionary
    """

    info = {}

    columns = conn.execute(
        f"DESCRIBE {table_name}"
    ).df()

    for _, row in columns.iterrows():

        col_name = row['column_name']
        col_type = row['column_type']
        col_info = {'type': col_type}

        # Text columns → get exact unique values
        if 'VARCHAR' in col_type:
            try:
                unique = conn.execute(f"""
                    SELECT DISTINCT {col_name}
                    FROM {table_name}
                    WHERE {col_name} IS NOT NULL
                    ORDER BY {col_name}
                    LIMIT 30
                """).df()[col_name].tolist()

                nulls = conn.execute(f"""
                    SELECT COUNT(*) FROM {table_name}
                    WHERE {col_name} IS NULL
                """).fetchone()[0]

                col_info['unique_values'] = unique
                col_info['null_count']    = nulls

            except Exception as e:
                col_info['note'] = str(e)

        # Integer columns → unique values if small, else range
        elif 'INTEGER' in col_type or 'BIGINT' in col_type:
            try:
                count_unique = conn.execute(f"""
                    SELECT COUNT(DISTINCT {col_name})
                    FROM {table_name}
                """).fetchone()[0]

                if count_unique <= 10:
                    unique = conn.execute(f"""
                        SELECT DISTINCT {col_name}
                        FROM {table_name}
                        WHERE {col_name} IS NOT NULL
                        ORDER BY {col_name}
                    """).df()[col_name].tolist()
                    col_info['unique_values'] = unique
                else:
                    stats = conn.execute(f"""
                        SELECT
                            MIN({col_name}) AS min_val,
                            MAX({col_name}) AS max_val
                        FROM {table_name}
                    """).fetchone()
                    col_info['min'] = stats[0]
                    col_info['max'] = stats[1]

            except Exception as e:
                col_info['note'] = str(e)

        # Double/Float columns → range + avg
        elif 'DOUBLE' in col_type or 'FLOAT' in col_type:
            try:
                stats = conn.execute(f"""
                    SELECT
                        ROUND(MIN({col_name}), 2) AS min_val,
                        ROUND(MAX({col_name}), 2) AS max_val,
                        ROUND(AVG({col_name}), 2) AS avg_val
                    FROM {table_name}
                    WHERE {col_name} IS NOT NULL
                """).fetchone()
                col_info['min'] = stats[0]
                col_info['max'] = stats[1]
                col_info['avg'] = stats[2]

            except Exception as e:
                col_info['note'] = str(e)

        info[col_name] = col_info

    return info


def build_table_text(table_name, table_info, row_count):
    text  = f"\nTable: {table_name}"
    text += f" ({row_count} rows)\n"
    text += "-" * 45 + "\n"
    for col_name, info in table_info.items():

        text += f"\n {col_name} ({info['type']})"

        if 'unique_values' in info:
            text += f"\n  Exact values: {info['unique_values']}"
            if info.get('null_count', 0) > 0:
                text += f"\n  Nulls: {info['null_count']}"

        elif 'min' in info:
            text += f"\n  Range: {info['min']} to {info['max']}"
            if 'avg' in info:
                text += f", avg: {info['avg']}"
        text += "\n"
    return text


def build_complete_schema(conn):

    schema = """
================================================
DATABASE: DuckDB — Frammer AI Analytics
DATE RANGE: March 2025 to February 2026
================================================

PLATFORM CONTEXT:
Frammer AI converts long-form video content
into short-form outputs.

TERMINOLOGY:
Uploaded   = raw content uploaded to Frammer AI
Created    = output content created by Frammer AI
             (Full package, Key moments, Chapters,
              My Key moments, Summary)
Published  = content published by users on
             social media platforms

KEY METRIC DEFINITIONS:
publish_rate_pct  = published / created * 100
drop_off_count    = created - published
creation_ratio    = created / uploaded
uploaded_hours    = total hours of uploaded content
created_hours     = total hours of created content

================================================
KPI REFERENCE — NAME TO COMPUTATION MAPPING
================================================

When a user mentions any of these KPI names,
keyword phrases, or asks a semantically similar
question, compute using the formula shown and
the table indicated.

─────────────────────────────────────────
OVERALL PUBLISH RATE
─────────────────────────────────────────
Also triggered by: "how much content gets
published", "what % is published",
"overall publishing performance"
Formula: SUM(published_count) * 100.0
         / NULLIF(SUM(created_count), 0)
Table: summary_channel (sum across all rows)

─────────────────────────────────────────
CONTENT AMPLIFICATION RATIO
─────────────────────────────────────────
Also triggered by: "amplification", "how many
outputs per upload", "creation multiplier",
"how much does frammer multiply content"
Formula: SUM(created_count)
         / NULLIF(SUM(uploaded_count), 0)
Table: summary_channel (sum across all rows)

─────────────────────────────────────────
MONTH-OVER-MONTH UPLOAD GROWTH RATE
─────────────────────────────────────────
Also triggered by: "MoM growth", "monthly growth",
"upload trend", "how uploads changed month to month"
Formula: (this_month_uploaded - prev_month_uploaded)
         / NULLIF(prev_month_uploaded, 0) * 100
         Use LAG() window function over month order
Table: summary_monthly

─────────────────────────────────────────
DROP-OFF VOLUME
─────────────────────────────────────────
Also triggered by: "how many not published",
"unpublished content", "content that never went live",
"gap between created and published"
Formula: SUM(created_count) - SUM(published_count)
Table: summary_channel

─────────────────────────────────────────
TOTAL CONTENT HOURS
─────────────────────────────────────────
Also triggered by: "total hours processed",
"how many hours of content", "content volume in hours"
Formula: SUM(created_hours) or SUM(uploaded_hours)
         depending on context
Table: summary_channel (sum across all rows)

─────────────────────────────────────────
PUBLISH RATE BY CHANNEL
─────────────────────────────────────────
Also triggered by: "channel publish rate",
"which channel publishes most", "channel-wise
publishing performance"
Formula: published_count * 100.0
         / NULLIF(created_count, 0) per channel
Table: summary_channel

─────────────────────────────────────────
MONTHLY PUBLISH RATE TREND
─────────────────────────────────────────
Also triggered by: "how publish rate changed over
time", "monthly publishing trend", "best month
for publishing"
Formula: published_count * 100.0
         / NULLIF(created_count, 0) per month
Table: summary_monthly

─────────────────────────────────────────
ZERO-PUBLISH CHANNEL COUNT
─────────────────────────────────────────
Also triggered by: "channels with no publishes",
"inactive channels", "channels that never published"
Formula: COUNT(*) WHERE published_count = 0
Table: summary_channel

─────────────────────────────────────────
INPUT TYPE PUBLISH RATE
─────────────────────────────────────────
Also triggered by: "which content type publishes
most", "publish rate by input", "interview vs speech
publishing"
Formula: published_count * 100.0
         / NULLIF(created_count, 0) per input_type
Table: summary_input_type

─────────────────────────────────────────
INPUT TYPE AMPLIFICATION RATIO
─────────────────────────────────────────
Also triggered by: "which input type creates most
outputs", "content type multiplier",
"amplification by content type"
Formula: created_count / NULLIF(uploaded_count, 0)
         per input_type
Table: summary_input_type

─────────────────────────────────────────
OUTPUT FORMAT PUBLISH RATE
─────────────────────────────────────────
Also triggered by: "which output format gets
published most", "reels vs chapters publishing",
"best performing output type"
Formula: published_count * 100.0
         / NULLIF(created_count, 0) per output_type
Table: summary_output_type

─────────────────────────────────────────
OUTPUT FORMAT MIX DISTRIBUTION
─────────────────────────────────────────
Also triggered by: "output type breakdown",
"which format is created most", "mix of outputs",
"share of each output type"
Formula: created_count * 100.0
         / NULLIF(SUM(created_count) OVER (), 0)
         per output_type
Table: summary_output_type

─────────────────────────────────────────
LANGUAGE PUBLISH RATE
─────────────────────────────────────────
Also triggered by: "hindi vs english publishing",
"which language publishes more",
"publish rate by language"
Formula: published_count * 100.0
         / NULLIF(created_count, 0) per language
Table: summary_language

─────────────────────────────────────────
LANGUAGE UPLOAD SHARE
─────────────────────────────────────────
Also triggered by: "language breakdown of uploads",
"how much content is in hindi", "language mix",
"share of each language"
Formula: uploaded_count * 100.0
         / NULLIF(SUM(uploaded_count) OVER (), 0)
         per language
Table: summary_language

─────────────────────────────────────────
PLATFORM DISTRIBUTION
─────────────────────────────────────────
Also triggered by: "which platform gets most
content", "youtube vs reels vs shorts",
"where is content being published",
"social media platform breakdown",
"publishing destinations"
Formula: SUM(publish_count) per platform_name,
         then share = count / NULLIF(total, 0) * 100
Table: summary_platform
⚠ STANDALONE TABLE — only use for platform
  questions. Do NOT use for channel/user questions.

─────────────────────────────────────────
USER PUBLISH RATE
─────────────────────────────────────────
Also triggered by: "which user publishes most",
"user-wise publishing", "top publishers by user"
Formula: publish_rate_pct column directly
Table: summary_user (total across all channels)
       OR bridge_user_channel (within a channel)

─────────────────────────────────────────
USER UPLOAD VOLUME RANK
─────────────────────────────────────────
Also triggered by: "top uploaders", "who uploads
most", "user upload ranking", "most active users"
Formula: ORDER BY total_uploaded_count DESC
         or total_uploaded_hours DESC
Table: summary_user

─────────────────────────────────────────
CHANNEL × USER METRIC
─────────────────────────────────────────
Also triggered by: "user activity in a channel",
"top users in channel X", "how does user Y perform
in each channel", "channel and user breakdown"
Formula: row-level publish_rate_pct per
         channel_name + user_name pair
Table: bridge_user_channel

─────────────────────────────────────────
UNKNOWN TEAM NAME RATE
─────────────────────────────────────────
Also triggered by: "unclassified content",
"missing team names", "what % has unknown team",
"team name data quality"
Formula: unknown_team_pct column directly
Table: data_quality (single-row summary)
       OR: COUNT_IF(team_name = 'Unknown') * 100.0
           / COUNT(*) FROM fact_video_jobs
⚠ STANDALONE TABLE — use data_quality or
  fact_video_jobs only. Not available in
  summary tables.

─────────────────────────────────────────
PUBLISHED URL COMPLETENESS RATE
─────────────────────────────────────────
Also triggered by: "missing published URLs",
"url data quality", "published videos without
a link", "how complete is the URL data"
Formula: url_completeness_pct column directly
Table: data_quality (single-row summary)
⚠ STANDALONE TABLE — use data_quality or
  fact_video_jobs only.

─────────────────────────────────────────
QA / TEST ACCOUNT ACTIVITY
─────────────────────────────────────────
Also triggered by: "QA account uploads",
"test account activity", "non-production users",
"how much content came from QA accounts"
Formula: COUNT(*) WHERE uploaded_by LIKE 'QA-%'
         OR uploaded_by IN known test accounts
Table: fact_video_jobs
⚠ STANDALONE TABLE — use fact_video_jobs only.

─────────────────────────────────────────
DUPLICATE VIDEO ID COUNT
─────────────────────────────────────────
Also triggered by: "duplicate videos",
"repeated video IDs", "data integrity check",
"how many duplicate records"
Formula: duplicate_video_ids column directly
Table: data_quality (single-row summary)
⚠ STANDALONE TABLE — use data_quality only.

================================================
TABLES
================================================
"""

    # ── Core tables: always available ──────────
    core_tables = [
        'bridge_user_channel',
        'dim_channel',
        'dim_user',
        'dim_input_type',
        'dim_output_type',
        'dim_language',
        'summary_user',
        'summary_channel',
        'summary_monthly',
        'summary_input_type',
        'summary_output_type',
        'summary_language',
    ]

    # ── Standalone tables: specialist use only ──
    standalone_tables = [
        'summary_platform',
        'fact_video_jobs',
        'data_quality',
    ]

    all_tables = conn.execute(
        "SHOW TABLES"
    ).df()['name'].tolist()

    schema += "\n--- CORE TABLES ---\n"
    schema += "(Use for all general questions)\n"

    for table_name in core_tables:
        if table_name not in all_tables:
            continue
        row_count = conn.execute(
            f"SELECT COUNT(*) FROM {table_name}"
        ).fetchone()[0]
        table_info = extract_table_info(conn, table_name)
        schema += build_table_text(
            table_name, table_info, row_count
        )
        print(f"Processed schema: {table_name}")

    schema += """
--- STANDALONE TABLES ---
(Use ONLY when question is specifically about
 platforms, data quality, or video-level detail.
 Never use these for general channel/user questions.)
"""

    for table_name in standalone_tables:
        if table_name not in all_tables:
            continue
        row_count = conn.execute(
            f"SELECT COUNT(*) FROM {table_name}"
        ).fetchone()[0]
        table_info = extract_table_info(conn, table_name)
        schema += build_table_text(
            table_name, table_info, row_count
        )
        print(f"Processed schema: {table_name}")

    schema += """
================================================
HOW TABLES CONNECT
================================================

bridge_user_channel.user_id
    → dim_user.user_id

bridge_user_channel.channel_id
    → dim_channel.channel_id

summary_channel.channel_id
    → dim_channel.channel_id

summary_user is aggregated FROM
    bridge_user_channel
    (totals per user across all channels)

summary_channel is aggregated FROM
    raw_channel_summary
    (totals per channel across all users)

summary_platform is aggregated FROM
    raw_channel_platform (unpivoted)
    Links to dim_channel via channel_name

data_quality is aggregated FROM
    fact_video_jobs (single summary row)

================================================
TABLE SELECTION RULES — VERY IMPORTANT
================================================

Use summary_user when:
→ Question about user TOTAL across all channels
→ "who uploaded most" → summary_user
→ "top users by hours" → summary_user
→ "user publish rate overall" → summary_user
→ User Upload Volume Rank KPI
→ User Publish Rate KPI (overall)

Use bridge_user_channel when:
→ Question needs BOTH user AND channel
→ "Neha in channel A" → bridge_user_channel
→ "top users IN channel B" → bridge_user_channel
→ "which channel does X work most in"
→ Channel × User Metric KPI

Use summary_channel when:
→ Question about channel TOTAL across all users
→ "which channel most active" → summary_channel
→ "channel drop-off" → summary_channel
→ Drop-off Volume KPI
→ Zero-Publish Channel Count KPI
→ Publish Rate by Channel KPI
→ Overall Publish Rate KPI (sum all rows)
→ Content Amplification Ratio KPI (sum all rows)
→ Total Content Hours KPI (sum all rows)

Use summary_monthly when:
→ Any question about time or month
→ "monthly trend" → summary_monthly
→ Month-over-Month Upload Growth Rate KPI
→ Monthly Publish Rate Trend KPI

Use summary_input_type when:
→ Question about content type
→ Input Type Publish Rate KPI
→ Input Type Amplification Ratio KPI

Use summary_output_type when:
→ Question about output format
→ Output Format Publish Rate KPI
→ Output Format Mix Distribution KPI

Use summary_language when:
→ Question about language
→ Language Publish Rate KPI
→ Language Upload Share KPI

Use summary_platform ONLY when:
→ Question explicitly mentions a social platform
  (YouTube, Reels, Shorts, Instagram, Facebook,
   LinkedIn, X, Threads)
→ Platform Distribution KPI
→ "where is content being published"
→ NEVER use for general channel questions

Use fact_video_jobs ONLY when:
→ QA / Test Account Activity KPI
→ Need row-level video data not in summaries
→ NEVER use for upload/publish volume questions
  (use summary tables instead — they are faster
   and already aggregated)

Use data_quality ONLY when:
→ Unknown Team Name Rate KPI
→ Published URL Completeness Rate KPI
→ Duplicate Video ID Count KPI
→ Any data integrity question
→ Single-row table — always SELECT *

NEVER join summary_monthly with
summary_channel or summary_user.
Monthly data has NO channel or user breakdown.

NEVER join summary_platform with summary tables.
Platform data has no time or user breakdown.

================================================
METRIC FORMULAS
================================================

Publish rate:
→ ROUND(published_count * 100.0
        / NULLIF(created_count, 0), 2)

Drop-off:
→ created_count - published_count

Content Amplification Ratio:
→ ROUND(created_count * 1.0
        / NULLIF(uploaded_count, 0), 2)

Month-over-Month growth:
→ ALWAYS use month_order (INTEGER) not month (TEXT)
  month is text — sorts alphabetically — WRONG
  month_order is 1-12 — sorts chronologically — RIGHT
→ ROUND((uploaded_count
         - LAG(uploaded_count) OVER (ORDER BY month_order))
        * 100.0
        / NULLIF(LAG(uploaded_count)
                 OVER (ORDER BY month_order), 0), 2)

Duration in hours from seconds:
→ ROUND(SUM(uploaded_secs) / 3600.0, 2)

Platform share:
→ ROUND(publish_count * 100.0
        / NULLIF(SUM(publish_count) OVER (), 0), 2)

================================================
CRITICAL RULES — NEVER BREAK THESE
================================================

RULE 1: channel_name exact values
    Single capital letters ONLY:
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
    'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
    'Q', 'R'
    NEVER: 'channel_a', 'Channel A', 'ch_a'

RULE 2: input_type exact values
    'interview', 'news bulletin', 'debate',
    'speech', 'special reports',
    'press conference', 'discussion-show',
    'podcast', 'sports show', 'drama',
    'in-brief', 'Unknown'

RULE 3: output_type exact values
    'Full package', 'Key moments', 'Chapters',
    'My Key moments', 'Summary'

RULE 4: language_code exact values
    'en', 'hi', 'mix', 'es', 'ar', 'mr'

RULE 5: language_name exact values
    'English', 'Hindi', 'Mixed',
    'Spanish', 'Arabic', 'Marathi'

RULE 6: month exact format
    'Apr, 2025', 'Aug, 2025', 'Dec, 2025',
    'Feb, 2026', 'Jan, 2026', 'Jul, 2025',
    'Jun, 2025', 'Mar, 2025', 'May, 2025',
    'Nov, 2025', 'Oct, 2025', 'Sep, 2025'

RULE 6b: month ordering — CRITICAL
    month column is TEXT — alphabetical sort is WRONG
    ALWAYS use month_order column for ORDER BY
    month_order values: Mar2025=1, Apr2025=2,
    May2025=3, Jun2025=4, Jul2025=5, Aug2025=6,
    Sep2025=7, Oct2025=8, Nov2025=9, Dec2025=10,
    Jan2026=11, Feb2026=12
    USE: ORDER BY month_order
    NEVER: ORDER BY month

RULE 7: platform_name exact values
    'Facebook', 'Instagram', 'Linkedin',
    'Reels', 'Shorts', 'X', 'Youtube', 'Threads'

RULE 8: Always use NULLIF for division
    ROUND(value * 100.0 / NULLIF(total, 0), 2)

RULE 9: Always alias columns
    COUNT(*) AS total_count

RULE 10: Always ORDER BY main metric DESC
    Unless user asks for ascending

RULE 11: Always LIMIT 10
    Unless user asks for more or less

RULE 12: Duration columns are TEXT
    uploaded_duration_text is "hh:mm:ss"
    Cannot do math on text columns
    Use uploaded_secs or uploaded_hours instead

RULE 13: No date filtering on fact tables
    Only summary_monthly has month column
    Cannot filter by specific date elsewhere

RULE 14: data_quality is a single-row table
    Always SELECT * FROM data_quality
    Never GROUP BY or aggregate on it

RULE 15: summary_platform channel_name
    has TRIM applied — values match dim_channel
    exactly ('A', 'B' etc.)

================================================
EXAMPLE QUERIES
================================================

Example 1: Overall Publish Rate KPI
    SELECT
        SUM(uploaded_count)  AS total_uploaded,
        SUM(created_count)   AS total_created,
        SUM(published_count) AS total_published,
        ROUND(SUM(published_count) * 100.0
              / NULLIF(SUM(created_count), 0), 2)
              AS overall_publish_rate_pct
    FROM summary_channel;

Example 2: Content Amplification Ratio KPI
    SELECT
        SUM(uploaded_count) AS total_uploaded,
        SUM(created_count)  AS total_created,
        ROUND(SUM(created_count) * 1.0
              / NULLIF(SUM(uploaded_count), 0), 2)
              AS content_amplification_ratio
    FROM summary_channel;

Example 3: Drop-off Volume KPI
    SELECT channel_name, created_count,
           published_count, drop_off_count,
           publish_rate_pct
    FROM summary_channel
    ORDER BY drop_off_count DESC;

Example 4: Month-over-Month Upload Growth Rate KPI
    SELECT month, month_order, uploaded_count,
        ROUND(
            (uploaded_count
             - LAG(uploaded_count)
               OVER (ORDER BY month_order))
            * 100.0
            / NULLIF(LAG(uploaded_count)
                     OVER (ORDER BY month_order), 0)
        , 2) AS mom_growth_pct
    FROM summary_monthly
    ORDER BY month_order;

Example 5: Monthly Publish Rate Trend KPI
    SELECT month, month_order, uploaded_count,
           created_count, published_count,
           publish_rate_pct
    FROM summary_monthly
    ORDER BY month_order;

Example 6: Zero-Publish Channel Count KPI
    SELECT COUNT(*) AS zero_publish_channels
    FROM summary_channel
    WHERE published_count = 0;

Example 7: Input Type Amplification Ratio KPI
    SELECT input_type,
           uploaded_count, created_count,
           ROUND(created_count * 1.0
                 / NULLIF(uploaded_count, 0), 2)
                 AS amplification_ratio
    FROM summary_input_type
    ORDER BY amplification_ratio DESC;

Example 8: Output Format Mix Distribution KPI
    SELECT output_type, created_count,
        ROUND(created_count * 100.0
              / NULLIF(SUM(created_count) OVER (), 0)
        , 2) AS share_pct
    FROM summary_output_type
    ORDER BY created_count DESC;

Example 9: Language Upload Share KPI
    SELECT language_name, uploaded_count,
        ROUND(uploaded_count * 100.0
              / NULLIF(SUM(uploaded_count) OVER (), 0)
        , 2) AS upload_share_pct,
        publish_rate_pct
    FROM summary_language
    ORDER BY uploaded_count DESC;

Example 10: Platform Distribution KPI
    SELECT platform_name,
           SUM(publish_count) AS total_published,
           ROUND(SUM(publish_count) * 100.0
                 / NULLIF(SUM(SUM(publish_count))
                           OVER (), 0)
           , 2) AS platform_share_pct
    FROM summary_platform
    GROUP BY platform_name
    ORDER BY total_published DESC;

Example 11: Unknown Team Name Rate KPI
    SELECT unknown_team_count,
           total_videos,
           unknown_team_pct
    FROM data_quality;

Example 12: Published URL Completeness Rate KPI
    SELECT published_with_url,
           total_published,
           url_completeness_pct
    FROM data_quality;

Example 13: Duplicate Video ID Count KPI
    SELECT duplicate_video_ids
    FROM data_quality;

Example 14: QA / Test Account Activity KPI
    SELECT uploaded_by,
           COUNT(*) AS total_videos,
           SUM(is_published) AS published_count,
           ROUND(SUM(is_published) * 100.0
                 / NULLIF(COUNT(*), 0), 2)
                 AS publish_rate_pct
    FROM fact_video_jobs
    WHERE uploaded_by LIKE 'QA-%'
       OR uploaded_by IN (
           'deleteme@frammer.com',
           'Test User',
           'Auto Upload'
       )
    GROUP BY uploaded_by
    ORDER BY total_videos DESC;

Example 15: Channel × User Metric KPI
    SELECT channel_name, user_name,
           uploaded_count, created_count,
           published_count, publish_rate_pct
    FROM bridge_user_channel
    ORDER BY uploaded_count DESC
    LIMIT 10;

Example 16: User Upload Volume Rank KPI
    SELECT user_name, channels_count,
           total_uploaded_count,
           total_uploaded_hours,
           publish_rate_pct
    FROM summary_user
    ORDER BY total_uploaded_count DESC
    LIMIT 10;

Example 17: Total Content Hours KPI
    SELECT
        ROUND(SUM(uploaded_hours), 2)
              AS total_uploaded_hours,
        ROUND(SUM(created_hours), 2)
              AS total_created_hours,
        ROUND(SUM(published_hours), 2)
              AS total_published_hours
    FROM summary_channel;

"""

    return schema


schema_text = build_complete_schema(conn)

with open("llm_schema.txt", "w", encoding="utf-8") as f:
    f.write(schema_text)

print("LLM schema built and saved to llm_schema.txt")
print("Schema preview:")
lines = schema_text.split('\n')
for line in lines[:30]:
    print(line)

conn.close()
print("complete")