"""
nlq_pipeline.py  —  Natural Language → SQL pipeline
=====================================================
Converts a user question into a DuckDB SQL query,
executes it, and returns a JSON-serializable result dict.

Result dict keys:
    question    : str
    sql         : str
    data        : dict | None   → { columns: [...], rows: [[...], ...] }
    explanation : dict
    error       : str | None
    retried     : bool
    chart       : str | None    → base64 data-URI (png or html)
"""

import re
import duckdb
import google.generativeai as genai

from config import DB_PATH, SCHEMA_PATH, GEMINI_API_KEY, GEMINI_MODEL
from visualize import generate_chart_base64

# ── Setup ─────────────────────────────────────────────────────────────────────

genai.configure(api_key=GEMINI_API_KEY)
conn = duckdb.connect(DB_PATH)

with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
    SCHEMA_TEXT = f.read()

print("Pipeline ready.")


# ─────────────────────────────────────────────────────────────────────────────
# PROMPT BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def build_prompt(user_question: str) -> str:
    return f"""
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
12. Duration text columns cannot be used in math
    — use _secs or _hours columns instead

SQL:
"""


# ─────────────────────────────────────────────────────────────────────────────
# GEMINI CALLER
# ─────────────────────────────────────────────────────────────────────────────

def call_gemini(prompt: str) -> str:
    model = genai.GenerativeModel(
        model_name=GEMINI_MODEL,
        system_instruction=(
            "You are an expert DuckDB SQL analyst. "
            "You only output raw SQL queries. "
            "Never add explanations or markdown. "
            "Never use backticks. "
            "Always end with a semicolon."
        ),
    )
    response = model.generate_content(
        prompt,
        generation_config=genai.GenerationConfig(
            temperature=0,
            max_output_tokens=500,
        ),
    )
    return response.text.strip()


# ─────────────────────────────────────────────────────────────────────────────
# SQL CLEANER
# ─────────────────────────────────────────────────────────────────────────────

def clean_sql(raw_output: str) -> str:
    sql = raw_output.strip()
    sql = sql.replace("```sql", "").replace("```", "")

    if sql.upper().startswith("SQL:"):
        sql = sql[4:]

    select_pos = sql.upper().find("SELECT")
    if select_pos > 0:
        sql = sql[select_pos:]

    sql = sql.strip().rstrip(";").strip() + ";"
    return sql


# ─────────────────────────────────────────────────────────────────────────────
# SQL VALIDATOR
# ─────────────────────────────────────────────────────────────────────────────

VALID_TABLES = {
    "BRIDGE_USER_CHANNEL",
    "DIM_CHANNEL",
    "DIM_USER",
    "DIM_INPUT_TYPE",
    "DIM_OUTPUT_TYPE",
    "DIM_LANGUAGE",
    "SUMMARY_USER",
    "SUMMARY_CHANNEL",
    "SUMMARY_MONTHLY",
    "SUMMARY_INPUT_TYPE",
    "SUMMARY_OUTPUT_TYPE",
    "SUMMARY_LANGUAGE",
}

DANGEROUS_KEYWORDS = {
    "DELETE", "DROP", "INSERT", "UPDATE",
    "TRUNCATE", "ALTER", "CREATE", "EXEC", "EXECUTE",
}


def validate_sql(sql: str) -> tuple[bool, list[str]]:
    errors = []
    sql_upper = sql.upper().strip()

    if not sql_upper.startswith("SELECT"):
        errors.append("SQL must start with SELECT")

    for word in DANGEROUS_KEYWORDS:
        if re.search(r"\b" + word + r"\b", sql_upper):
            errors.append(f"Forbidden keyword: {word}")

    if sql.count(";") > 1:
        errors.append("Only one SQL statement allowed")

    for table in re.findall(r"(?:FROM|JOIN)\s+(\w+)", sql_upper):
        if table not in VALID_TABLES:
            errors.append(f"Unknown table: {table.lower()}")

    return (False, errors) if errors else (True, [])


# ─────────────────────────────────────────────────────────────────────────────
# SQL EXECUTOR  —  returns (rows_dict | None, error_str | None)
# ─────────────────────────────────────────────────────────────────────────────

def execute_sql(sql: str) -> tuple[dict | None, str | None]:
    try:
        df = conn.execute(sql).df()
        # Sanitise: replace NaN/Inf so JSON serialisation never fails
        df = df.where(df.notna(), other=None)
        return {
            "columns": df.columns.tolist(),
            "rows":    df.values.tolist(),
        }, None
    except Exception as e:
        return None, str(e)


# ─────────────────────────────────────────────────────────────────────────────
# EXPLANATION BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def build_explanation(sql: str, data: dict | None) -> dict:
    tables = list(set(re.findall(r"(?:FROM|JOIN)\s+(\w+)", sql.upper())))

    where_match = re.search(
        r"WHERE\s+(.+?)(?:GROUP|ORDER|LIMIT|;|$)",
        sql, re.IGNORECASE | re.DOTALL,
    )
    group_match = re.search(
        r"GROUP BY\s+(.+?)(?:ORDER|LIMIT|;|$)",
        sql, re.IGNORECASE | re.DOTALL,
    )

    return {
        "tables_used":     tables,
        "filters_applied": where_match.group(1).strip() if where_match else "None",
        "grouped_by":      group_match.group(1).strip() if group_match else "None",
        "rows_returned":   len(data["rows"]) if data else 0,
    }


# ─────────────────────────────────────────────────────────────────────────────
# RETRY WITH ERROR FEEDBACK
# ─────────────────────────────────────────────────────────────────────────────

def retry_with_error(user_question: str, failed_sql: str, error: str) -> str:
    retry_prompt = f"""
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


# ─────────────────────────────────────────────────────────────────────────────
# MAIN PIPELINE  —  fully JSON-serializable return value
# ─────────────────────────────────────────────────────────────────────────────

def nlq_query(user_question: str) -> dict:
    """
    Full pipeline: question → Gemini → SQL → validate → execute → chart.

    Returns a dict that FastAPI / JSONResponse can serialise directly:
        question    str
        sql         str
        data        { columns, rows } | None
        explanation dict
        error       str | None
        retried     bool
        chart       base64 data-URI str | None
    """
    result = {
        "question":    user_question,
        "sql":         "",
        "data":        None,
        "explanation": {},
        "error":       None,
        "retried":     False,
        "chart":       None,
    }

    # ── Step 1: Generate SQL ──────────────────────────────────────────────────
    try:
        raw_sql = call_gemini(build_prompt(user_question))
    except Exception as e:
        result["error"] = f"Gemini error: {e}"
        return result

    sql = clean_sql(raw_sql)
    result["sql"] = sql

    # ── Step 2: Validate ──────────────────────────────────────────────────────
    is_valid, errors = validate_sql(sql)
    if not is_valid:
        result["error"] = f"Validation errors: {', '.join(errors)}"
        return result

    # ── Step 3: Execute (with one retry on failure) ───────────────────────────
    data, error = execute_sql(sql)

    if error:
        print(f"First attempt failed: {error}\nRetrying…")
        try:
            retry_sql = clean_sql(retry_with_error(user_question, sql, error))
            is_valid, errors = validate_sql(retry_sql)

            if not is_valid:
                result["error"] = f"Invalid SQL after retry: {', '.join(errors)}"
                return result

            data, error = execute_sql(retry_sql)
            if error:
                result["error"] = f"Retry failed: {error}"
                return result

            result["sql"]     = retry_sql
            result["retried"] = True

        except Exception as e:
            result["error"] = f"Gemini error during retry: {e}"
            return result

    # ── Step 4: Build explanation & chart ────────────────────────────────────
    result["data"]        = data
    result["explanation"] = build_explanation(result["sql"], data)
    result["chart"]       = generate_chart_base64(user_question, data)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# TEST RUNNER
# ─────────────────────────────────────────────────────────────────────────────

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
        "Which output type has highest publish rate?",
    ]

    print("\n" + "=" * 55)
    print("TESTING NLQ PIPELINE")
    print("=" * 55)

    passed = failed = 0

    for question in test_questions:
        print(f"\nQuestion: {question}")
        print("-" * 40)
        result = nlq_query(question)

        if result["error"]:
            print(f"ERROR: {result['error']}")
            failed += 1
        else:
            exp = result["explanation"]
            print(f"SQL:     {result['sql']}")
            print(f"Tables:  {exp.get('tables_used', [])}")
            print(f"Filters: {exp.get('filters_applied', 'None')}")
            print(f"Rows:    {exp.get('rows_returned', 0)}")
            if result["retried"]:
                print("Note: Required retry to fix SQL")
            if result["chart"]:
                print(f"Chart:   [base64 data-URI, {len(result['chart'])} chars]")
            # Print first 3 rows
            d = result["data"]
            if d and d["rows"]:
                print("Preview:")
                for row in d["rows"][:3]:
                    print(" ", dict(zip(d["columns"], row)))
            passed += 1

    print("\n" + "=" * 55)
    print(f"Tests passed: {passed}/{len(test_questions)}")
    print(f"Tests failed: {failed}/{len(test_questions)}")
    print("=" * 55)