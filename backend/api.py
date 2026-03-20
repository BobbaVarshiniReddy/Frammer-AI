"""
api.py  —  Frammer AI — Unified FastAPI Backend
================================================
Place this file inside the backend/ folder.

Folder structure:
    Frammer-AI/
    ├── backend/
    │   ├── api.py               ← this file
    │   ├── database.py          ← create this (see below)
    │   ├── text_to_sql/
    │   │   ├── __init__.py      ← empty file
    │   │   ├── config.py
    │   │   ├── nlq_pipeline.py
    │   │   └── visualize.py
    │   ├── raw_video_list_kpis.py
    │   ├── raw_monthly_duration_kpis.py
    │   └── ... (all other kpi/plot files)
    └── src/

Usage:
    cd backend
    uvicorn api:app --reload --port 8000
"""

import logging
import os
from functools import lru_cache

import pandas as pd
import pyarrow as pa
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# ── Chatbot (text_to_sql) ─────────────────────────────────────────────────────
from text_to_sql.nlq_pipeline import nlq_query

# ── Video list KPIs ───────────────────────────────────────────────────────────
from raw_video_list_kpis import (
    get_kpi14_published_platform_distribution,
    get_kpi16_published_rate_per_uploader,
    get_kpi17_top_uploaders,
    get_kpi19_unknown_team_name_rate,
    get_kpi20_published_url_completeness,
    get_kpi21_overall_published_rate,
    get_kpi22_duplicate_video_id_count,
)
from raw_video_list_plots import (
    get_published_vs_content_type,
    get_published_vs_team,
    get_data_completeness_heatmap,
    get_top_uploaders,
    get_published_pct_by_type,
    get_published_pct_by_uploader,
)

# ── Monthly (Usage & Trends) ──────────────────────────────────────────────────
from raw_monthly_duration_kpis import (
    get_kpi05_total_content_hours_processed,
    get_kpi05_monthly_created_duration_trend,
    get_kpi03_mom_duration_growth,
)
from raw_monthly_duration_plots import (
    get_grouped_duration_vs_month,
    get_top5_uploaded_duration,
    get_top5_published_duration,
)

# ── Language KPIs ─────────────────────────────────────────────────────────────
from by_language_kpis import (
    get_kpi13_language_publish_rate,
    get_kpi15_language_upload_share,
)

# ── Output type KPIs ──────────────────────────────────────────────────────────
from output_type_kpis import (
    get_kpi11_output_format_publish_rate,
    get_kpi12_output_format_mix_distribution,
)

# ── Input type KPIs ───────────────────────────────────────────────────────────
from raw_input_type_kpis import (
    get_kpi09_input_type_publish_rate,
    get_kpi10_input_type_amplification_ratio,
    get_kpi19_unknown_input_type_rate,
)

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# App + CORS
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(title="Frammer AI Analytics API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:5173",
        "http://127.0.0.1:3000",
    ],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────────────────────────

def _to_df(table) -> pd.DataFrame:
    """Convert pyarrow Table or RecordBatchReader → pandas DataFrame."""
    if isinstance(table, pa.lib.RecordBatchReader):
        table = table.read_all()
    return table.to_pandas().fillna(0)


def _kpi_response(kpi_fn) -> dict:
    """
    Standard wrapper for KPI functions.
    Handles both return styles:
      - (table, chart_type)          — direct tuple
      - [[table, chart_type], ...]   — list of lists
    """
    try:
        results = kpi_fn()
        if not results:
            return {"data": [], "chart_type": None}
        # Direct tuple: (table, chart_type)
        if isinstance(results, tuple):
            table, chart_type = results
        # List of lists: [[table, chart_type], ...]
        else:
            table, chart_type = results[0][0], results[0][1]
        return {"data": _to_df(table).to_dict(orient="records"), "chart_type": chart_type}
    except Exception as e:
        logger.error("KPI error in %s: %s", kpi_fn.__name__, e)
        return {"error": str(e)}


def _plot_response(plot_fn, preferred=("bar", "line", "heatmap")) -> dict:
    """
    Standard wrapper for plot functions.
    Handles both return styles:
      - (table, chart_type)          — direct tuple
      - [[table, chart_type], ...]   — list of lists
    """
    try:
        result = plot_fn()
        if not result:
            return {"data": [], "chart_type": None}
        # Direct tuple: (table, chart_type)
        if isinstance(result, tuple):
            return {"data": _to_df(result[0]).to_dict(orient="records"), "chart_type": result[1]}
        # List of lists: [[table, chart_type], ...]
        selected = None
        for pref in preferred:
            for item in result:
                if item[1] == pref:
                    selected = item
                    break
            if selected:
                break
        if selected is None:
            selected = result[0]
        return {"data": _to_df(selected[0]).to_dict(orient="records"), "chart_type": selected[1]}
    except Exception as e:
        logger.error("Plot error in %s: %s", plot_fn.__name__, e)
        return {"error": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# Health check
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}


# ─────────────────────────────────────────────────────────────────────────────
# Chatbot — NLQ → SQL
# ─────────────────────────────────────────────────────────────────────────────

class QueryRequest(BaseModel):
    question: str


@app.post("/query")
def query(req: QueryRequest):
    if not req.question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty.")
    logger.info("Chatbot question: %s", req.question)
    result = nlq_query(req.question)
    if result["error"]:
        logger.warning("Pipeline error: %s", result["error"])
    else:
        logger.info("OK — %d rows", result["explanation"].get("rows_returned", 0))
    return JSONResponse(content=result)


# ─────────────────────────────────────────────────────────────────────────────
# Video list KPI endpoints
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/kpi/platform_distribution")
def kpi_platform_distribution():
    return _kpi_response(get_kpi14_published_platform_distribution)

@app.get("/kpi/published_rate_per_uploader")
def kpi_published_rate_per_uploader():
    return _kpi_response(get_kpi16_published_rate_per_uploader)

@app.get("/kpi/top_uploaders")
def kpi_top_uploaders():
    return _kpi_response(get_kpi17_top_uploaders)

@app.get("/kpi/unknown_team_rate")
def kpi_unknown_team_rate():
    return _kpi_response(get_kpi19_unknown_team_name_rate)

@app.get("/kpi/published_url_completeness")
def kpi_published_url_completeness():
    return _kpi_response(get_kpi20_published_url_completeness)

@app.get("/kpi/overall_published_rate")
def kpi_overall_published_rate():
    return _kpi_response(get_kpi21_overall_published_rate)

@app.get("/kpi/duplicate_video_id_count")
def kpi_duplicate_video_id_count():
    return _kpi_response(get_kpi22_duplicate_video_id_count)

@app.get("/kpi/all")
def get_all_kpis():
    try:
        kpi_functions = [
            get_kpi11_output_format_publish_rate,
            get_kpi12_output_format_mix_distribution,
            get_kpi09_input_type_publish_rate,
            get_kpi10_input_type_amplification_ratio,
            get_kpi19_unknown_input_type_rate,
        ]
        all_kpis = []
        for fn in kpi_functions:
            try:
                results = fn()
            except Exception as e:
                logger.warning("Skipping %s: %s", fn.__name__, e)
                continue
            for table, chart_type in results:
                df = _to_df(table)
                if df.empty:
                    continue
                all_kpis.append({
                    "data": df.to_dict(orient="records"),
                    "chart_type": chart_type,
                })
        return {"kpis": all_kpis}
    except Exception as e:
        return {"error": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# Video list plot endpoints
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/plot/custom_plot")
def custom_plot(plot_name: str):
    plot_map = {
        "published_vs_content_type": get_published_vs_content_type,
        "published_vs_team":         get_published_vs_team,
        "data_completeness_heatmap": get_data_completeness_heatmap,
        "top_uploaders":             get_top_uploaders,
        "published_pct_by_type":     get_published_pct_by_type,
        "published_pct_by_uploader": get_published_pct_by_uploader,
    }
    if plot_name not in plot_map:
        return {"error": f"Invalid plot_name: {plot_name}"}
    return _plot_response(plot_map[plot_name])


# ─────────────────────────────────────────────────────────────────────────────
# Monthly KPI endpoints (Usage & Trends)
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_month_value(df: pd.DataFrame, metric_col: str) -> list[dict]:
    if "Month" in df.columns:
        return df[["Month", metric_col]].rename(columns={metric_col: "value"}).to_dict(orient="records")
    return df[[metric_col]].rename(columns={metric_col: "value"}).to_dict(orient="records")


@app.get("/monthly/kpi/{kpi_name}")
def monthly_kpi(kpi_name: str):
    kpi_map = {
        "total_content_hours_processed":    (get_kpi05_total_content_hours_processed,    "total_content_hours",   "kpi_card"),
        "monthly_created_duration_trend":   (get_kpi05_monthly_created_duration_trend,   "total_created_hours",   "line"),
        "mom_duration_growth":              (get_kpi03_mom_duration_growth,              "mom_growth_pct",        "line"),
    }
    if kpi_name not in kpi_map:
        return {"error": f"Invalid kpi_name: {kpi_name}"}
    try:
        fn, metric_col, preferred_chart = kpi_map[kpi_name]
        options = fn()
        selected = next(((t, c) for t, c in options if c == preferred_chart), options[0])
        df = _to_df(selected[0])
        if metric_col not in df.columns:
            return {"error": f"Column '{metric_col}' not found"}
        return {"data": _normalize_month_value(df, metric_col), "chart_type": selected[1]}
    except Exception as e:
        return {"error": str(e)}


@app.get("/monthly/plot/custom_plot")
def monthly_plot(plot_name: str):
    try:
        duration_plots = {
            "uploaded_duration_vs_month":  "Uploaded_hours",
            "created_duration_vs_month":   "Created_hours",
            "published_duration_vs_month": "Published_hours",
        }
        if plot_name in duration_plots:
            metric_col = duration_plots[plot_name]
            options = get_grouped_duration_vs_month()
            table, _ = next(((t, c) for t, c in options if c == "line"), options[0])
            df = _to_df(table)
            if metric_col not in df.columns:
                return {"error": f"Column '{metric_col}' not found"}
            return {
                "data": df[["Month", metric_col]].rename(columns={metric_col: "value"}).to_dict(orient="records"),
                "chart_type": "line",
            }

        plot_map = {
            "top5_uploaded_duration":  (get_top5_uploaded_duration,  "Uploaded_hours",  "bar"),
            "top5_published_duration": (get_top5_published_duration, "Published_hours", "bar"),
        }
        if plot_name not in plot_map:
            return {"error": f"Invalid plot_name: {plot_name}"}

        fn, metric_col, preferred = plot_map[plot_name]
        options = fn()
        selected = next(((t, c) for t, c in options if c == preferred), options[0])
        df = _to_df(selected[0])
        if metric_col not in df.columns:
            return {"error": f"Column '{metric_col}' not found"}
        return {"data": _normalize_month_value(df, metric_col), "chart_type": selected[1]}

    except Exception as e:
        return {"error": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# Funnel endpoints (Output / Language / Input type)
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/funnel/kpi/{kpi_name}")
def funnel_kpi(kpi_name: str):
    kpi_map = {
        "output_format_publish_rate":      get_kpi11_output_format_publish_rate,
        "output_format_mix_distribution":  get_kpi12_output_format_mix_distribution,
        "language_publish_rate":           get_kpi13_language_publish_rate,
        "language_upload_share":           get_kpi15_language_upload_share,
        "input_type_publish_rate":         get_kpi09_input_type_publish_rate,
        "input_type_amplification_ratio":  get_kpi10_input_type_amplification_ratio,
        "unknown_input_type_rate":         get_kpi19_unknown_input_type_rate,
    }
    if kpi_name not in kpi_map:
        return {"error": f"Invalid kpi_name: {kpi_name}"}
    return _plot_response(kpi_map[kpi_name])


@app.get("/funnel/plot/{plot_name}")
def funnel_plot(plot_name: str):
    from output_type_plots import (
        get_output_format_publish_rate_plot,
        get_output_format_mix_distribution_plot,
    )
    from by_language_plots import (
        get_language_publish_rate,
        get_language_upload_share,
    )
    from raw_input_type_plots import (
        get_input_type_publish_rate_plot,
        get_input_type_amplification_ratio_plot,
        get_unknown_input_type_contribution_plot,
    )

    plot_map = {
        "output_format_publish_rate":          get_output_format_publish_rate_plot,
        "output_format_mix_distribution":      get_output_format_mix_distribution_plot,
        "language_publish_rate":               get_language_publish_rate,
        "language_upload_share":               get_language_upload_share,
        "input_type_publish_rate":             get_input_type_publish_rate_plot,
        "input_type_amplification_ratio":      get_input_type_amplification_ratio_plot,
        "unknown_input_type_contribution":     get_unknown_input_type_contribution_plot,
    }
    if plot_name not in plot_map:
        return {"error": f"Invalid plot_name: {plot_name}"}
    return _plot_response(plot_map[plot_name])


# ─────────────────────────────────────────────────────────────────────────────
# Video Explorer endpoints
# ─────────────────────────────────────────────────────────────────────────────

VIDEO_FILE = os.path.join(os.path.dirname(__file__), "../data/video_list_data_obfuscated.csv")


@lru_cache(maxsize=1)
def _load_video_df() -> pd.DataFrame:
    df = pd.read_csv(VIDEO_FILE)
    df["Headline"]   = df["Headline"].fillna("")
    df["Type"]       = df["Type"].fillna("")
    df["Uploaded By"] = df["Uploaded By"].fillna("")
    df["Published"]  = df["Published"].fillna("")
    df["Video ID"]   = pd.to_numeric(df["Video ID"], errors="coerce")
    return df


def _thumbnail_url(video_id, fallback_seed) -> str:
    seed = str(int(video_id)) if video_id is not None and not pd.isna(video_id) else str(fallback_seed)
    return f"https://picsum.photos/seed/{seed}/400/200"


@app.get("/videos/meta")
def videos_meta():
    df = _load_video_df()
    types     = sorted([t for t in df["Type"].dropna().unique() if str(t).strip() and str(t).lower() != "unknown"])
    uploaders = sorted([u for u in df["Uploaded By"].dropna().unique() if str(u).strip() and str(u).lower() != "unknown"])
    return {"types": types, "uploaded_by": uploaders}


@app.get("/videos")
def videos(
    name:        str = "",
    type:        str = "",
    uploaded_by: str = "",
    video_id:    str = "",
    offset:      int = 0,
    limit:       int = 12,
):
    df = _load_video_df()

    if name:
        df = df[df["Headline"].str.contains(name, case=False, na=False)]
    if type:
        df = df[df["Type"].str.lower() == type.lower()]
    if uploaded_by:
        df = df[df["Uploaded By"].str.lower() == uploaded_by.lower()]
    if video_id:
        vid_num = pd.to_numeric(video_id, errors="coerce")
        if not pd.isna(vid_num):
            df = df[df["Video ID"] == vid_num]
        else:
            df = df[df["Video ID"].astype("string").str.contains(video_id, case=False, na=False)]

    df = df.copy()
    df["_sort_vid"] = df["Video ID"].fillna(-1)
    df = df.sort_values(by=["Headline", "_sort_vid"], ascending=True, kind="mergesort").drop(columns=["_sort_vid"])

    offset   = max(0, offset)
    limit    = max(1, min(limit, 50))
    page_df  = df.iloc[offset: offset + limit]
    has_more = (offset + len(page_df)) < len(df)

    def _s(x):
        return "" if pd.isna(x) else str(x)

    result = []
    for idx, row in page_df.reset_index(drop=True).iterrows():
        vid = row.get("Video ID")
        result.append({
            "video_id":          None if pd.isna(vid) else int(vid),
            "title":             _s(row.get("Headline", "")),
            "type":              _s(row.get("Type", "")),
            "uploaded_by":       _s(row.get("Uploaded By", "")),
            "published":         _s(row.get("Published", "")),
            "published_platform": _s(row.get("Published Platform", "")),
            "published_url":     _s(row.get("Published URL", "")),
            "source":            _s(row.get("Source", "")),
            "thumbnail":         _thumbnail_url(vid, idx),
        })

    return {"data": result, "has_more": has_more, "next_offset": offset + len(page_df)}