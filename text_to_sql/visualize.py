"""
visualize.py  —  Chart generation module
=========================================
Generates a Plotly chart from a query result and returns it
as a base64-encoded data-URI string (PNG preferred, HTML fallback).

This module is imported by nlq_pipeline.py.
Do NOT run directly.

Install:
    pip install plotly kaleido google-generativeai
"""

import re
import base64
import logging
import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import google.generativeai as genai

from text_to_sql.config import GEMINI_API_KEY, GEMINI_MODEL

genai.configure(api_key=GEMINI_API_KEY)

logger = logging.getLogger(__name__)

# ── Brand colour palette ──────────────────────────────────────────────────────
COLOURS = ["#CC0000", "#FF3131", "#FF9A9A", "#FFD6D6", "#7A0000"]


# ─────────────────────────────────────────────────────────────────────────────
# INTERNAL HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _df_from_data(data: dict) -> pd.DataFrame:
    """Reconstruct a DataFrame from the { columns, rows } dict."""
    return pd.DataFrame(data["rows"], columns=data["columns"])


def _build_chart_prompt(question: str, df: pd.DataFrame) -> str:
    cols   = df.columns.tolist()
    sample = df.head(8).to_string(index=False)
    dtypes = df.dtypes.to_string()

    return f"""
You are a Python Plotly data visualisation expert.
Write Plotly code to answer this question visually:

QUESTION: {question}

DATAFRAME INFO:
Columns : {cols}
Dtypes  :
{dtypes}

Sample data (first 8 rows):
{sample}

COLOUR PALETTE (use these only, in this order):
{COLOURS}

STRICT RULES:
1. The dataframe is already loaded as variable `df`
2. Do NOT import pandas or load any data
3. Start with: import plotly.graph_objects as go
4. You may also use: import plotly.express as px
5. Background color: #0f0f0f (paper and plot)
6. Font color: #FFD6D6
7. Gridline color: #3a0000
8. Use the colour palette above for bars/lines/markers
9. Add a clear descriptive title
10. Add axis labels
11. Add value annotations on bars if it is a bar chart
12. The final line MUST assign the figure to a variable called `fig`
    e.g.  fig = go.Figure(...)  OR  fig = px.bar(...)
13. Do NOT call fig.show() or fig.write_image()
14. Return ONLY raw Python code — no markdown, no backticks, no explanation

Python code:
"""


def _extract_code(raw: str) -> str:
    code = raw.strip()
    code = re.sub(r"^```python\s*", "", code, flags=re.IGNORECASE)
    code = re.sub(r"^```\s*",       "", code, flags=re.IGNORECASE)
    code = re.sub(r"\s*```$",       "", code)
    code = re.sub(r"^\s*fig\.show\(.*\)\s*$",        "", code, flags=re.MULTILINE)
    code = re.sub(r"^\s*fig\.write_image\(.*\)\s*$", "", code, flags=re.MULTILINE)
    code = re.sub(r"^\s*fig\.write_html\(.*\)\s*$",  "", code, flags=re.MULTILINE)
    return code.strip()


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC API
# ─────────────────────────────────────────────────────────────────────────────

def generate_chart_base64(question: str, data: dict | None) -> str | None:
    """
    Generate a Plotly chart and return it as a base64 data-URI.

    Parameters
    ----------
    question : str   — original user question
    data     : dict  — { columns: [...], rows: [[...], ...] }
                       as returned by nlq_pipeline.execute_sql()

    Returns
    -------
    str  — "data:image/png;base64,..." or "data:text/html;base64,..."
    None — on any failure
    """
    if not data or not data.get("rows"):
        logger.debug("Chart skipped — empty data.")
        return None

    df = _df_from_data(data)

    # ── Step 1: Ask Gemini to write Plotly code ───────────────────────────────
    try:
        model = genai.GenerativeModel(
            model_name=GEMINI_MODEL,
            system_instruction=(
                "You are a Python Plotly expert. "
                "Output only raw executable Python code. "
                "Never use markdown or backticks. "
                "Always assign the final figure to a variable called `fig`."
            ),
        )
        response = model.generate_content(
            _build_chart_prompt(question, df),
            generation_config=genai.GenerationConfig(
                temperature=0.2,
                max_output_tokens=1000,
            ),
        )
        raw_code = response.text
    except Exception as e:
        logger.error("Chart: Gemini code generation failed — %s", e)
        return None

    code = _extract_code(raw_code)

    # ── Step 2: Execute the generated code ───────────────────────────────────
    try:
        import plotly.graph_objects as go
        import plotly.express as px

        exec_globals = {"df": df, "go": go, "px": px}
        exec(code, exec_globals)
        fig = exec_globals.get("fig")

        if fig is None:
            logger.error("Chart: code ran but `fig` was not assigned.")
            return None

    except Exception as e:
        logger.error("Chart: code execution failed — %s\nGenerated code:\n%s", e, code)
        return None

    # ── Step 3: Encode to base64 in memory (no disk writes) ──────────────────
    try:
        png_bytes = fig.to_image(format="png", width=1000, height=600, scale=2)
        b64 = base64.b64encode(png_bytes).decode()
        logger.info("Chart: PNG encoded to base64 (%d bytes).", len(png_bytes))
        return f"data:image/png;base64,{b64}"

    except Exception as png_err:
        logger.warning("Chart: PNG encoding failed (%s) — falling back to HTML.", png_err)

    try:
        html_str = fig.to_html(full_html=False, include_plotlyjs="cdn")
        b64      = base64.b64encode(html_str.encode()).decode()
        logger.info("Chart: HTML encoded to base64.")
        return f"data:text/html;base64,{b64}"

    except Exception as html_err:
        logger.error("Chart: HTML encoding also failed — %s", html_err)
        return None