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
import logging
import duckdb
import google.generativeai as genai

from text_to_sql.config import DB_PATH, SCHEMA_PATH, GEMINI_API_KEY, GEMINI_MODEL
from text_to_sql.visualize import generate_chart_base64

# ── Logging ───────────────────────────────────────────────────────────────────

logger = logging.getLogger(__name__)

# ── Setup ─────────────────────────────────────────────────────────────────────

genai.configure(api_key=GEMINI_API_KEY)
conn = duckdb.connect(DB_PATH)

with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
    SCHEMA_TEXT = f.read()

logger.info("Pipeline ready.")


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

    # Strip markdown fences
    sql = sql.replace("```sql", "").replace("```", "")

    # Strip "SQL:" prefix Gemini sometimes adds
    if sql.upper().startswith("SQL:"):
        sql = sql[4:]

    # If Gemini added preamble, find where SELECT starts
    select_pos = sql.upper().find("SELECT")
    if select_pos > 0:
        sql = sql[select_pos:]

    sql = sql.strip()

    # ── KEY FIX: remove any semicolons that appear mid-query ─────────────────
    # Gemini sometimes emits "SELECT ...\ncreated;" with a stray semicolon
    # after a column alias or subquery. Strip ALL semicolons then add one back.
    sql = sql.replace(";", "").strip() + ";"

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

    # No semicolon count check — clean_sql guarantees exactly one trailing ";"

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
        logger.warning("First attempt failed: %s — retrying…", error)
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
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    )

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

    logger.info("=" * 55)
    logger.info("TESTING NLQ PIPELINE")
    logger.info("=" * 55)

    passed = failed = 0

    for question in test_questions:
        logger.info("Question: %s", question)
        result = nlq_query(question)

        if result["error"]:
            logger.error("FAILED — %s", result["error"])
            failed += 1
        else:
            exp = result["explanation"]
            logger.info("SQL:     %s", result["sql"])
            logger.info("Tables:  %s", exp.get("tables_used", []))
            logger.info("Filters: %s", exp.get("filters_applied", "None"))
            logger.info("Rows:    %s", exp.get("rows_returned", 0))
            if result["retried"]:
                logger.info("Note: required retry to fix SQL")
            if result["chart"]:
                logger.info("Chart:   base64 data-URI (%d chars)", len(result["chart"]))
            d = result["data"]
            if d and d["rows"]:
                for row in d["rows"][:3]:
                    logger.info("  %s", dict(zip(d["columns"], row)))
            passed += 1

    logger.info("=" * 55)
    logger.info("Tests passed: %d/%d", passed, len(test_questions))
    logger.info("Tests failed: %d/%d", failed, len(test_questions))
    logger.info("=" * 55)