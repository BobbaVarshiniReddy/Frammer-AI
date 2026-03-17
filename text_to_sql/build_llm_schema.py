import duckdb
import os

conn=duckdb.connect("./database/frammer.db")
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
        # This is critical for preventing
        # hallucination on categorical values
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

        # Integer columns → show unique values
        # if small set, otherwise show range
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

        # Double/Float columns → show range
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


def build_table_text(table_name,table_info,row_count):
    text=f"\nTable:{table_name}"
    text+=f"({row_count} rows)\n"
    text+="-"*45 +"\n"
    for col_name,info in table_info.items():

        text+=f"\n {col_name} ({info['type']})"

        if 'unique_values' in info:
            text+=f"\n Exact values:{info['unique_values']}"
            if info.get('null_count',0)>0:
                text+=f"\n Nulls:{info['null_count']}"

        elif 'min' in info:
            text+=f"\n Range: {info['min']} to {info['max']}"
            if 'avg' in info:
                text+=f", avg: {info['avg']}"
        text+="\n"
    return text


def build_complete_schema(conn):

    schema="""
    ================================================
DATABASE: DuckDB — Frammer AI Analytics
DATE RANGE: March 2025 to February 2026
================================================

PLATFORM CONTEXT:
Frammer AI  converts long-form video content into short-form outputs.

TERMINOLOGY:
Uploaded=raw content uploaded to Frammer AI
Created=output content created by Frammer AI (Full package,
Key moments,
Chapters,
My Key moments,
Summary
)
Pulished=content published by users on social media platforms

KEY METRICS:
publish_rate_pct = published/created * 100
drop_off_count   = created - published
creation_ratio   = created/uploaded
uploaded_hours   = total hours of uploaded content
created_hours    = total hours of created content

================================================
TABLES
================================================
"""
    preferred_order = [
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
            'summary_language'
        ]

    all_tables=conn.execute("SHOW TABLES").df()['name'].tolist()

    clean_tables = [t for t in preferred_order if t in all_tables]
    for table_name in clean_tables:
        row_count=conn.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()[0]
        
        table_info=extract_table_info(conn,table_name)
        table_text=build_table_text(table_name,table_info,row_count)
        schema +=table_text
        print(f"Processed schema for table: {table_name}")

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

================================================
TABLE SELECTION RULES — VERY IMPORTANT
================================================

Use summary_user when:
→ Question about user TOTAL across all channels
→ "who uploaded most" → summary_user
→ "top users by hours" → summary_user
→ "user publish rate" → summary_user

Use bridge_user_channel when:
→ Question needs BOTH user AND channel
→ "Neha in channel A" → bridge_user_channel
→ "top users IN channel B" → bridge_user_channel
→ "which channel does X work most in"
   → bridge_user_channel

Use summary_channel when:
→ Question about channel TOTAL across all users
→ "which channel most active" → summary_channel
→ "channel drop-off" → summary_channel
→ "channel publish rate" → summary_channel

Use summary_monthly when:
→ Any question about time or month
→ "monthly trend" → summary_monthly
→ "which month had most uploads"
   → summary_monthly

Use summary_input_type when:
→ Question about content type
→ "interview vs speech" → summary_input_type
→ "which input type most uploaded"
   → summary_input_type

Use summary_output_type when:
→ Question about output format
→ "reels vs chapters" → summary_output_type
→ "which output type published most"
   → summary_output_type

Use summary_language when:
→ Question about language
→ "hindi vs english" → summary_language
→ "which language most active"
   → summary_language

NEVER join summary_monthly with
summary_channel or summary_user.
Monthly data has NO channel or user breakdown.

================================================
METRIC FORMULAS
================================================

Publish rate:
→ ROUND(published_count * 100.0
        / NULLIF(created_count, 0), 2)

Drop-off:
→ created_count - published_count

Duration in hours from seconds:
→ ROUND(SUM(uploaded_secs) / 3600.0, 2)

Total uploaded hours across all channels:
→ SUM(uploaded_hours) FROM summary_channel

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

RULE 7: Always use NULLIF for division
    ROUND(value * 100.0 / NULLIF(total, 0), 2)

RULE 8: Always alias columns
    COUNT(*) AS total_count (not just COUNT(*))

RULE 9: Always ORDER BY main metric DESC
    Unless user asks for ascending

RULE 10: Always LIMIT 10
    Unless user asks for more or less

RULE 11: Duration columns are TEXT
    uploaded_duration_text is "hh:mm:ss"
    Cannot do math on text columns
    Use uploaded_secs or uploaded_hours
    for calculations instead

RULE 12: No date filtering possible
    fact tables have no date columns
    Only summary_monthly has month column
    Cannot filter by specific date


    


================================================
EXAMPLE QUERIES
================================================

Example 1: Top channels by uploaded hours
    SELECT channel_name, uploaded_count,
           uploaded_hours, publish_rate_pct
    FROM summary_channel
    ORDER BY uploaded_hours DESC
    LIMIT 10;

Example 2: Top users by uploaded hours
    SELECT user_name, channels_count,
           total_uploaded_count,
           total_uploaded_hours,
           publish_rate_pct
    FROM summary_user
    ORDER BY total_uploaded_hours DESC
    LIMIT 10;

Example 3: Channel drop-off analysis
    SELECT channel_name, created_count,
           published_count, drop_off_count,
           publish_rate_pct
    FROM summary_channel
    ORDER BY drop_off_count DESC;

Example 4: Monthly trend
    SELECT month, uploaded_count,
           created_count, published_count,
           uploaded_hours, publish_rate_pct
    FROM summary_monthly
    ORDER BY month;

Example 5: Input type breakdown
    SELECT input_type, uploaded_count,
           uploaded_hours, publish_rate_pct,
           drop_off_count
    FROM summary_input_type
    ORDER BY uploaded_hours DESC;

Example 6: Neha across all channels
    SELECT channel_name, uploaded_count,
           uploaded_hours, published_count,
           publish_rate_pct
    FROM bridge_user_channel
    WHERE user_name = 'Neha'
    ORDER BY uploaded_hours DESC;

Example 7: Top users in channel A
    SELECT user_name, uploaded_count,
           uploaded_hours, published_count
    FROM bridge_user_channel
    WHERE channel_name = 'A'
    ORDER BY uploaded_hours DESC
    LIMIT 10;

Example 8: Language breakdown
    SELECT language_name, uploaded_count,
           uploaded_hours, publish_rate_pct
    FROM summary_language
    ORDER BY uploaded_count DESC;

Example 9: Overall KPIs
    SELECT
        SUM(uploaded_count) AS total_uploaded,
        SUM(created_count)  AS total_created,
        SUM(published_count) AS total_published,
        SUM(drop_off_count) AS total_drop_off,
        ROUND(SUM(published_count)*100.0
              /NULLIF(SUM(created_count),0),2)
              AS overall_publish_rate,
        ROUND(SUM(uploaded_secs)/3600.0,2)
              AS total_uploaded_hours
    FROM summary_channel;

Example 10: Which user works in most channels
    SELECT user_name, channels_count,
           total_uploaded_hours
    FROM summary_user
    ORDER BY channels_count DESC
    LIMIT 10;

"""

    return schema  

schema_text=build_complete_schema(conn)

with open("llm_schema.txt","w",encoding="utf-8") as f:
    f.write(schema_text)

print("LLM schema built and saved to llm_schema.txt")
print("Schema preview:")
lines=schema_text.split('\n')
for line in lines[:30]:
    print(line)

conn.close()
print("complete")


        

