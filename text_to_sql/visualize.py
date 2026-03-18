"""
visualize.py  —  Chart generation module
=========================================
Import this in nlq_pipeline.py.
Do not run directly.

Install:
    pip install plotly kaleido google-generativeai
"""

import os
import re
import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import google.generativeai as genai
from config import GEMINI_API_KEY, GEMINI_MODEL

genai.configure(api_key=GEMINI_API_KEY)

CHARTS_DIR = "./charts"

# ── Brand colour palette ──────────────────────────────────────────────────────
COLOURS = ["#CC0000", "#FF3131", "#FF9A9A", "#FFD6D6", "#7A0000"]


# ─────────────────────────────────────────────────────────────────────────────
# INTERNAL HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _safe_filename(question: str) -> str:
    name = re.sub(r'[^\w\s-]', '', question.lower())
    name = re.sub(r'\s+', '_', name)[:60]
    return name


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
    """Strip markdown fences Gemini sometimes adds."""
    code = raw.strip()
    code = re.sub(r'^```python\s*', '', code, flags=re.IGNORECASE)
    code = re.sub(r'^```\s*',       '', code, flags=re.IGNORECASE)
    code = re.sub(r'\s*```$',       '', code)
    # Remove any show/write calls — we handle saving
    code = re.sub(r'^\s*fig\.show\(.*\)\s*$',        '', code, flags=re.MULTILINE)
    code = re.sub(r'^\s*fig\.write_image\(.*\)\s*$', '', code, flags=re.MULTILINE)
    code = re.sub(r'^\s*fig\.write_html\(.*\)\s*$',  '', code, flags=re.MULTILINE)
    return code.strip()


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC API  —  called by nlq_pipeline.py
# ─────────────────────────────────────────────────────────────────────────────

def generate_chart(question: str, df: pd.DataFrame) -> str | None:
    """
    Generate a Plotly chart for the query result and save it as a PNG.

    Parameters
    ----------
    question : str          — original user question
    df       : pd.DataFrame — result from nlq_query

    Returns
    -------
    filepath : str | None   — path to saved .png, or None on failure
    """

    if df is None or df.empty:
        print("  [Chart] Skipping — empty dataframe.")
        return None

    os.makedirs(CHARTS_DIR, exist_ok=True)

    # ── Step 1: Ask Gemini to write Plotly code ───────────────────────────────
    try:
        model = genai.GenerativeModel(
            model_name=GEMINI_MODEL,
            system_instruction=(
                "You are a Python Plotly expert. "
                "Output only raw executable Python code. "
                "Never use markdown or backticks. "
                "Always assign the final figure to a variable called `fig`."
            )
        )
        response = model.generate_content(
            _build_chart_prompt(question, df),
            generation_config=genai.GenerationConfig(
                temperature=0.2,
                max_output_tokens=1000,
            )
        )
        raw_code = response.text

    except Exception as e:
        print(f"  [Chart] Gemini code generation failed: {e}")
        return None

    code = _extract_code(raw_code)

    # ── Step 2: Execute the generated code ───────────────────────────────────
    try:
        import plotly.graph_objects as go
        import plotly.express as px

        exec_globals = {
            "df"  : df,
            "go"  : go,
            "px"  : px,
        }
        exec(code, exec_globals)
        fig = exec_globals.get("fig")

        if fig is None:
            print("  [Chart] Code ran but `fig` was not assigned.")
            print(f"  [Chart] Generated code:\n{code}")
            return None

    except Exception as e:
        print(f"  [Chart] Code execution failed: {e}")
        print(f"  [Chart] Generated code:\n{code}")
        return None

    # ── Step 3: Save as PNG via kaleido ──────────────────────────────────────
    filepath = os.path.join(CHARTS_DIR, f"{_safe_filename(question)}.png")
    try:
        fig.write_image(filepath, width=1000, height=600, scale=2)
        print(f"  [Chart] Saved → {filepath}")
        return filepath

    except Exception as e:
        # kaleido not installed — fall back to saving as HTML
        print(f"  [Chart] PNG save failed ({e}), saving as HTML instead.")
        html_path = filepath.replace(".png", ".html")
        try:
            fig.write_html(html_path)
            print(f"  [Chart] Saved → {html_path}")
            return html_path
        except Exception as e2:
            print(f"  [Chart] HTML save also failed: {e2}")
            return None