import duckdb
import google.generativeai as genai
import os
import re
import time
from config import DB_PATH,SCHEMA_PATH,GEMINI_API_KEY,GEMINI_MODEL

genai.configure(api_key=GEMINI_API_KEY)

conn = duckdb.connect(DB_PATH)

with open(SCHEMA_PATH, 'r',encoding="utf-8") as f:
    SCHEMA_TEXT = f.read()

print("pipeline ready")

def build_prompt(user_question):
    prompt=f"""
{SCHEMA_TEXT}

================================================
YOUR TASK
================================================

Convert the following question to DuckDB SQL.

QUESTION: "{user_question}"

STRICT RULES:
1. Return ONLY raw SQL
2. No explanation before or after
3. No markdown formatting
4. No backticks or code blocks
5. End with exactly one semicolon
6. Use exact table and column names
7. Use exact values shown in schema
8. Always alias columns e.g. COUNT(*) AS total
9. Always ORDER BY main metric DESC
10. Always LIMIT 10 unless user specifies
11. Use NULLIF for all division operations
12. Duration text columns cannot be used
    in math — use _secs or _hours columns

SQL:
"""
    return prompt

def call_gemini(prompt):
    model=genai.GenerativeModel(model_name=GEMINI_MODEL,
                                system_instruction=(
            "You are an expert DuckDB SQL analyst. "
            "You only output raw SQL queries. "
            "Never add explanations or markdown. "
            "Never use backticks. "
            "Always end with a semicolon."
        ))
    
    response = model.generate_content(prompt,
                                      generation_config=genai.GenerationConfig(
                                          temperature=0,
                                            max_output_tokens=500
                                      ))
    
    sql=response.text.strip()
    return sql


def validate_sql(sql):
    errors=[]
    sql_upper=sql.upper().strip()

    if not sql_upper.startswith("SELECT"):
        errors.append("SQL must start with SELECT")
    dangerous=[
        'DELETE', 'DROP', 'INSERT',
        'UPDATE', 'TRUNCATE', 'ALTER',
        'CREATE', 'EXEC', 'EXECUTE'
    ]
    for word in dangerous:
        pattern = r'\b' + word + r'\b'
        if re.search(pattern, sql_upper):
            errors.append(
                f"Forbidden keyword: {word}"
            )

    if sql.count(';') >1:
        errors.append("Only one SQL statement allowed")

    valid_tables=[
        'BRIDGE_USER_CHANNEL',
        'DIM_CHANNEL',
        'DIM_USER',
        'DIM_INPUT_TYPE',
        'DIM_OUTPUT_TYPE',
        'DIM_LANGUAGE',
        'SUMMARY_USER',
        'SUMMARY_CHANNEL',
        'SUMMARY_MONTHLY',
        'SUMMARY_INPUT_TYPE',
        'SUMMARY_OUTPUT_TYPE',
        'SUMMARY_LANGUAGE',
        # ── Standalone tables ──────────────────
        # Used only for platform, data quality,
        # and video-level KPI questions
        'SUMMARY_PLATFORM',
        'FACT_VIDEO_JOBS',
        'DATA_QUALITY',
    ]

    tables_used = re.findall(
        r'(?:FROM|JOIN)\s+(\w+)',
        sql_upper
    )

    for table in tables_used:
        if table not in valid_tables:
            errors.append(
                f"Unknown table: {table.lower()}"
            )

    if errors:
        return False, errors

    return True, []


def execute_sql(sql):
    try:
        result=conn.execute(sql).df()
        return result,None
    except Exception as e:
        return None,str(e)
    

def build_explaination(sql,result_df):
    explanation={}

    tables = re.findall(
        r'(?:FROM|JOIN)\s+(\w+)',
        sql.upper()
    )
    explanation['tables_used']=list(set(tables))

    where_match = re.search(
        r'WHERE\s+(.+?)(?:GROUP|ORDER|LIMIT|$)',
        sql,
        re.IGNORECASE | re.DOTALL
    )
    if where_match:
        explanation['filters_applied'] = \
            where_match.group(1).strip()
    else:
        explanation['filters_applied'] = "None"
        
    group_match = re.search(
        r'GROUP BY\s+(.+?)(?:ORDER|LIMIT|$)',
        sql,
        re.IGNORECASE | re.DOTALL
    )
    if group_match:
        explanation['grouped_by'] = \
            group_match.group(1).strip()
    else:
        explanation['grouped_by'] = "None"

    # Rows returned
    explanation['rows_returned'] = \
        len(result_df) if result_df is not None else 0

    return explanation


def retry_with_error(user_question,failed_sql,error):
    retry_prompt=f"""
{SCHEMA_TEXT}
================================================
FIX THIS SQL ERROR
================================================
Original question: "{user_question}"

SQL that failed:
{failed_sql}

Error received:
{error}

Fix the SQL query and return corrected version.

STRICT RULES:
1. Return ONLY raw SQL
2. No explanation
3. No markdown
4. No backticks
5. End with semicolon

Fixed SQL:
"""
    return call_gemini(retry_prompt)

def clean_sql(raw_output):
    # ─────────────────────────────────────────
    # What this does:
    # Takes whatever Gemini returned
    # Strips out markdown, prefixes, extra text
    # Returns clean executable SQL
    # ─────────────────────────────────────────

    # Step 1: Remove leading/trailing spaces
    sql = raw_output.strip()

    # Step 2: Remove markdown code blocks
    # Gemini sometimes wraps SQL in:
    # ```sql
    # SELECT ...
    # ```
    sql = sql.replace("```sql", "")
    sql = sql.replace("```", "")

    # Step 3: Remove "SQL:" prefix
    # Gemini sometimes starts with "SQL:"
    if sql.upper().startswith("SQL:"):
        sql = sql[4:]

    # Step 4: Remove "Here is the query:" type text
    # If Gemini adds explanation before SQL
    # find where SELECT starts and cut before it
    select_pos = sql.upper().find("SELECT")
    if select_pos > 0:
        sql = sql[select_pos:]

    # Step 5: Strip again after cleaning
    sql = sql.strip()

    # Step 6: Ensure semicolon at end
    # DuckDB needs semicolon to execute
    if not sql.endswith(";"):
        sql += ";"

    return sql
def nlq_query(user_question):
    """
    Complete pipeline:
    question → prompt → gemini → sql
    → validate → execute → explain → result

    Returns dictionary with:
    - question: original question
    - sql: generated SQL
    - data: pandas dataframe result
    - explanation: what was applied
    - error: error message if failed
    """

    result={
        "question"   : user_question,
        "sql"        : "",
        "data"       : None,
        "explanation": {},
        "error"      : None,
        "retried"    : False
    }

    prompt=build_prompt(user_question)

    try:
        raw_sql=call_gemini(prompt)
    except Exception as e:
        result['error']=f"Gemini error: {str(e)}"
        return result

    sql= clean_sql(raw_sql)
    result['sql']=sql

    is_valid,errors=validate_sql(sql)
    if not is_valid:
        result['error']=f"Validation errors: {', '.join(errors)}"
        return result
    
    data,error=execute_sql(sql)
    if error:
        print(f"first attempt failed: {error}")
        print("retrying with error feedback...")
        try:
            retry_sql_raw=retry_with_error(user_question,sql,error)
            retry_sql=clean_sql(retry_sql_raw)
            is_valid,errors=validate_sql(retry_sql)

            if is_valid:
                data,error=execute_sql(retry_sql)
                if  not error:
                    result['sql']=retry_sql
    
                    result['retried']=True

                else:
                    result['error']=(f"Retry failed: {error}")
                    return result
                
            else:
                result['error']=(f"Invalid sql after retry:" f"{', '.join(errors)}")
                return result
            
        except Exception as e:
            result['error']=f"Gemini error during retry: {str(e)}"
            return result
        
    
    result['data']=data
    result['explanation']=build_explaination(result['sql'],data)
    return result
                    

if __name__ == "__main__":

    test_questions = [
        "Which channel has the biggest drop off?",
        "Who uploaded the most hours overall?",
        "Show monthly trend of uploads",
        "What is the publish rate by input type?",
        "Which language has most content?",
        "Top 5 users by created hours",
        "Which channels have zero published videos?",
        "Show overall KPIs",
        "Neha activity across all channels",
        "Which output type has highest publish rate?"
    ]
    

    print("\n" + "=" * 55)
    print("TESTING NLQ PIPELINE")
    print("=" * 55)

    passed = 0
    failed = 0

    for i, question in enumerate(test_questions, 1):

        # Free tier limit: 5 requests per minute
        # Wait 15 seconds between each call to stay safe
        if i > 1:
            print(f"  (waiting 15s for rate limit...)")
            time.sleep(15)

        print(f"\nQuestion {i}/{len(test_questions)}: {question}")
        print("-" * 40)

        result = nlq_query(question)

        if result["error"]:
            print(f"ERROR: {result['error']}")
            failed += 1

        else:
            print(f"SQL: {result['sql']}")

            exp = result["explanation"]
            print(
                f"Tables: "
                f"{exp.get('tables_used', [])}"
            )
            print(
                f"Filters: "
                f"{exp.get('filters_applied', 'None')}"
            )
            print(
                f"Rows: "
                f"{exp.get('rows_returned', 0)}"
            )

            if result["retried"]:
                print("Note: Required retry to fix SQL")

            print("\nResult:")
            print(
                result["data"]
                .head(3)
                .to_string(index=False)
            )

            passed += 1

    print("\n" + "=" * 55)
    print(f"Tests passed: {passed}/{len(test_questions)}")
    print(f"Tests failed: {failed}/{len(test_questions)}")
    print("=" * 55)