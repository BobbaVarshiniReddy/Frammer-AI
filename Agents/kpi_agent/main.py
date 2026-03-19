from fastapi import FastAPI, UploadFile, File, Form
import json

from schema_analyzer import analyze_schema
from kpi_planner import plan_kpis
from kpi_engine import compute_kpis
from insight_engine import generate_insight
from query_engine import run_sql
from utils import load_csv

app = FastAPI()


@app.post("/analyze")
async def analyze(
    file: UploadFile = File(...),
    descriptions: str = Form(...)
):
    """
    descriptions = JSON string
    """

    df = load_csv(file.file)
    column_descriptions = json.loads(descriptions)

    # 1. Schema
    schema = analyze_schema(df)

    # 2. AI KPI Planning
    kpis = plan_kpis(column_descriptions)

    # 3. Compute KPIs
    results = compute_kpis(df, kpis)

    # 4. Add insights
    for r in results:
        r["insight"] = generate_insight(r["name"], r["value"])

    return {
        "schema": schema,
        "kpis": results[:10]
    }


@app.post("/query")
async def query(
    file: UploadFile = File(...),
    query: str = Form(...)
):
    df = load_csv(file.file)

    result = run_sql(df, query)

    return {"result": result}