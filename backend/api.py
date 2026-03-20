# api.py  —  Frammer AI Analytics  |  Unified FastAPI Server
# =============================================================
# Place this file inside:  backend/api.py
#
# Run from the PROJECT ROOT with:
#   uvicorn backend.api:app --reload --port 8000
#
# Folder structure assumed:
#   Frammer-AI/
#   ├── backend/
#   │   ├── api.py                        ← THIS FILE
#   │   ├── raw_video_list_kpis.py
#   │   ├── raw_video_list_plots.py
#   │   ├── raw_monthly_duration_kpis.py
#   │   ├── raw_monthly_duration_plots.py
#   │   ├── by_language_kpis.py
#   │   ├── by_language_plots.py
#   │   ├── output_type_kpis.py
#   │   ├── output_type_plots.py
#   │   ├── raw_input_type_kpis.py
#   │   └── raw_input_type_plots.py
#   ├── data/
#   │   └── video_list_data_obfuscated.csv
#   ├── text_to_sql/
#   │   ├── __init__.py
#   │   └── nlq_pipeline.py
#   └── ...

import logging
import os
import sys
from functools import lru_cache

import pandas as pd
import pyarrow as pa
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# ── Make sure the project root is on sys.path so text_to_sql is importable ──
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from text_to_sql.nlq_pipeline import nlq_query  # noqa: E402  (after sys.path fix)

# ── KPI / Plot imports (all live inside backend/) ────────────────────────────
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
    get_data_completeness_heatmap,
    get_published_pct_by_type,
    get_published_pct_by_uploader,
    get_published_vs_content_type,
    get_published_vs_team,
    get_top_uploaders,
)
from raw_monthly_duration_kpis import (
    get_kpi03_mom_duration_growth,
    get_kpi05_monthly_created_duration_trend,
    get_kpi05_total_content_hours_processed,
)
from raw_monthly_duration_plots import (
    get_grouped_duration_vs_month,
    get_top5_published_duration,
    get_top5_uploaded_duration,
)
from by_language_kpis import (
    get_kpi13_language_publish_rate,
    get_kpi15_language_upload_share,
)
from by_language_plots import (
    get_language_publish_rate,
    get_language_upload_share,
)
from output_type_kpis import (
    get_kpi11_output_format_publish_rate,
    get_kpi12_output_format_mix_distribution,
)
from output_type_plots import (
    get_output_format_mix_distribution_plot,
    get_output_format_publish_rate_plot,
)
from raw_input_type_kpis import (
    get_kpi09_input_type_publish_rate,
    get_kpi10_input_type_amplification_ratio,
    get_kpi19_unknown_input_type_rate,
)
from raw_input_type_plots import (
    get_input_type_amplification_ratio_plot,
    get_input_type_publish_rate_plot,
    get_unknown_input_type_contribution_plot,
)

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(title="Frammer AI Analytics API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "*",                         # open for development
        "http://localhost:3000",     # React / CRA
        "http://localhost:5173",     # Vite
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ── Pydantic models ───────────────────────────────────────────────────────────
class QueryRequest(BaseModel):
    question: str


# =============================================================================
# SHARED HELPERS
# =============================================================================

def _arrow_to_df(table) -> pd.DataFrame:
    """Convert a PyArrow Table or RecordBatchReader to a pandas DataFrame."""
    if isinstance(table, pa.lib.RecordBatchReader):
        table = table.read_all()
    return table.to_pandas()


def _kpi_response(table_func):
    """
    Utility for simple KPI endpoints whose function returns (table, chart_type).
    Fills NaN with 'Unknown' and returns a standard JSON dict.
    """
    try:
        table, chart_type = table_func()
        df = _arrow_to_df(table).fillna("Unknown")
        return {"data": df.to_dict(orient="records"), "chart_type": chart_type}
    except Exception as e:
        logger.exception("KPI error in %s", table_func.__name__)
        return {"error": str(e)}


def _kpi_response_from_fn(kpi_fn):
    """
    For funnel-style KPI functions that return: [(table, chart_type), ...]
    Takes the first tuple.
    """
    results = kpi_fn()
    if not results:
        return {"data": [], "chart_type": None}
    table, chart_type = results[0]
    df = _arrow_to_df(table).fillna(0)
    return {"data": df.to_dict(orient="records"), "chart_type": chart_type}


def _plot_response_from_fn(plot_fn, preferred_chart_types=("bar", "line", "heatmap")):
    """
    For plot functions that return: [[table, chart_type], ...]
    Selects the first match for the preferred chart type(s).
    """
    options = plot_fn()
    if not options:
        return {"data": [], "chart_type": None}

    selected = None
    for pref in preferred_chart_types:
        for table, chart_type in options:
            if chart_type == pref:
                selected = (table, chart_type)
                break
        if selected:
            break
    if selected is None:
        selected = options[0]

    table, chart_type = selected
    df = _arrow_to_df(table).fillna(0)
    return {"data": df.to_dict(orient="records"), "chart_type": chart_type}


def _select_first_supported_chart(chart_options: list, supported: set) -> tuple:
    """Return the first (table, chart_type) whose chart_type is in `supported`."""
    for table, chart_type in chart_options:
        if chart_type in supported:
            return table, chart_type
    return chart_options[0][0], chart_options[0][1]


def _normalize_to_month_value(df: pd.DataFrame, metric_col: str) -> list:
    """Collapse a DataFrame to [{Month?, value}] for time-series endpoints."""
    if "Month" in df.columns:
        out = df[["Month", metric_col]].rename(columns={metric_col: "value"})
    else:
        out = df[[metric_col]].rename(columns={metric_col: "value"})
    return out.to_dict(orient="records")


# =============================================================================
# HEALTH
# =============================================================================

@app.get("/health")
def health():
    return {"status": "ok"}


# =============================================================================
# TEXT-TO-SQL  (from main.py)
# =============================================================================

@app.post("/query")
def query(req: QueryRequest):
    """Natural-language → SQL query endpoint."""
    if not req.question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty.")
    logger.info("NLQ question: %s", req.question)
    result = nlq_query(req.question)
    if result.get("error"):
        logger.warning("Pipeline error: %s", result["error"])
    else:
        logger.info("NLQ OK — %d rows", result.get("explanation", {}).get("rows_returned", 0))
    return JSONResponse(content=result)


# =============================================================================
# RAW VIDEO LIST  —  KPI endpoints
# =============================================================================

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
    try:
        table, _ = get_kpi19_unknown_team_name_rate()
        df = _arrow_to_df(table).fillna("Unknown")
        return {"data": df.to_dict(orient="records"), "chart_type": None}
    except Exception as e:
        return {"error": str(e)}


@app.get("/kpi/published_url_completeness")
def kpi_published_url_completeness():
    try:
        table, _ = get_kpi20_published_url_completeness()
        df = _arrow_to_df(table).fillna("Unknown")
        return {"data": df.to_dict(orient="records"), "chart_type": None}
    except Exception as e:
        return {"error": str(e)}


@app.get("/kpi/overall_published_rate")
def kpi_overall_published_rate():
    try:
        table, _ = get_kpi21_overall_published_rate()
        df = _arrow_to_df(table).fillna("Unknown")
        return {"data": df.to_dict(orient="records"), "chart_type": None}
    except Exception as e:
        return {"error": str(e)}


@app.get("/kpi/duplicate_video_id_count")
def kpi_duplicate_video_id_count():
    try:
        table, _ = get_kpi22_duplicate_video_id_count()
        df = _arrow_to_df(table).fillna("Unknown")
        return {"data": df.to_dict(orient="records"), "chart_type": None}
    except Exception as e:
        return {"error": str(e)}


@app.get("/kpi/all")
def get_all_kpis():
    """Aggregate endpoint used by the input/output/funnel tabs."""
    try:
        kpi_functions = [
            get_kpi11_output_format_publish_rate,
            get_kpi12_output_format_mix_distribution,
            get_kpi09_input_type_publish_rate,
            get_kpi10_input_type_amplification_ratio,
            get_kpi19_unknown_input_type_rate,
        ]

        all_kpis = []
        for func in kpi_functions:
            try:
                result = func()
            except Exception as e:
                logger.warning("Skipping %s: %s", func.__name__, e)
                continue

            for table, chart_type in result:
                df = _arrow_to_df(table).fillna(0)
                if df.empty:
                    continue
                all_kpis.append({"data": df.to_dict(orient="records"), "chart_type": chart_type})

        return {"kpis": all_kpis}
    except Exception as e:
        return {"error": str(e)}


# =============================================================================
# RAW VIDEO LIST  —  Plot endpoint
# =============================================================================

@app.get("/plot/custom_plot")
def custom_plot(plot_name: str):
    """
    Select a chart by name.  plot_name options:
        published_vs_content_type | published_vs_team |
        data_completeness_heatmap | top_uploaders |
        published_pct_by_type     | published_pct_by_uploader
    """
    plot_funcs = {
        "published_vs_content_type": get_published_vs_content_type,
        "published_vs_team": get_published_vs_team,
        "data_completeness_heatmap": get_data_completeness_heatmap,
        "top_uploaders": get_top_uploaders,
        "published_pct_by_type": get_published_pct_by_type,
        "published_pct_by_uploader": get_published_pct_by_uploader,
    }

    if plot_name not in plot_funcs:
        return {"error": f"Invalid plot_name: {plot_name}"}

    try:
        table, chart_type = plot_funcs[plot_name]()
        df = _arrow_to_df(table).fillna(0)

        if chart_type == "bar":
            if "Type" in df.columns and "Published" in df.columns:
                data = [{"x": f"{r['Type']} - {r['Published']}", "y": r["count"]} for _, r in df.iterrows()]
            elif "Team Name" in df.columns and "Published" in df.columns:
                data = [{"x": f"{r['Team Name']} - {r['Published']}", "y": r["count"]} for _, r in df.iterrows()]
            else:
                data = [{"x": r[df.columns[0]], "y": r[df.columns[1]]} for _, r in df.iterrows()]
        elif chart_type == "line":
            data = [{"x": r[df.columns[0]], "y": r[df.columns[1]]} for _, r in df.iterrows()]
        elif chart_type == "heatmap":
            data = [{"x": r["Column Name"], "y": r["Column Name"], "value": r["completeness_pct"]} for _, r in df.iterrows()]
        else:
            data = df.to_dict(orient="records")

        return {"data": data, "chart_type": chart_type}
    except Exception as e:
        return {"error": str(e)}


# =============================================================================
# MONTHLY  —  Usage & Trends  KPI endpoints
# =============================================================================

@app.get("/monthly/kpi/{kpi_name}")
def monthly_kpi(kpi_name: str):
    """
    Usage & Trends KPI selector.
    Frontend expects: { data: [{Month?: str, value: number}], chart_type: str }
    """
    kpi_map = {
        "total_content_hours_processed": (
            get_kpi05_total_content_hours_processed, "total_content_hours", "kpi_card",
        ),
        "monthly_created_duration_trend": (
            get_kpi05_monthly_created_duration_trend, "total_created_hours", "line",
        ),
        "mom_duration_growth": (
            get_kpi03_mom_duration_growth, "mom_growth_pct", "line",
        ),
    }

    if kpi_name not in kpi_map:
        return {"error": f"Invalid kpi_name: {kpi_name}"}

    try:
        fn, metric_col, preferred_chart_type = kpi_map[kpi_name]
        chart_options = fn()
        supported = {"line", "bar", "kpi_card"}

        selected_table = selected_chart_type = None
        for table, chart_type in chart_options:
            if chart_type == preferred_chart_type:
                selected_table, selected_chart_type = table, chart_type
                break
        if selected_table is None:
            selected_table, selected_chart_type = _select_first_supported_chart(chart_options, supported)

        df = _arrow_to_df(selected_table).fillna(0)
        if metric_col not in df.columns:
            return {"error": f"Expected metric column '{metric_col}' not found"}

        return {"data": _normalize_to_month_value(df, metric_col), "chart_type": selected_chart_type}
    except Exception as e:
        return {"error": str(e)}


# =============================================================================
# MONTHLY  —  Usage & Trends  Plot endpoints
# =============================================================================

@app.get("/monthly/plot/custom_plot")
def monthly_plot(plot_name: str):
    """
    Usage & Trends plot selector.
    plot_name options:
        uploaded_duration_vs_month | created_duration_vs_month |
        published_duration_vs_month | top5_uploaded_duration |
        top5_published_duration
    """
    try:
        grouped_series = {
            "uploaded_duration_vs_month": "Uploaded_hours",
            "created_duration_vs_month": "Created_hours",
            "published_duration_vs_month": "Published_hours",
        }

        if plot_name in grouped_series:
            grouped_table, _ = _select_first_supported_chart(
                get_grouped_duration_vs_month(), {"line", "bar"}
            )
            df = _arrow_to_df(grouped_table).fillna(0)
            metric_col = grouped_series[plot_name]
            if metric_col not in df.columns:
                return {"error": f"Expected metric column '{metric_col}' not found"}
            data_df = df[["Month", metric_col]].rename(columns={metric_col: "value"})
            return {"data": data_df.to_dict(orient="records"), "chart_type": "line"}

        plot_map = {
            "top5_uploaded_duration": (get_top5_uploaded_duration, "Uploaded_hours", "bar"),
            "top5_published_duration": (get_top5_published_duration, "Published_hours", "bar"),
        }

        if plot_name not in plot_map:
            return {"error": f"Invalid plot_name: {plot_name}"}

        fn, metric_col, preferred_chart_type = plot_map[plot_name]
        chart_options = fn()

        selected_table = selected_chart_type = None
        for table, chart_type in chart_options:
            if chart_type == preferred_chart_type:
                selected_table, selected_chart_type = table, chart_type
                break
        if selected_table is None:
            selected_table, selected_chart_type = _select_first_supported_chart(chart_options, {"bar", "line"})

        df = _arrow_to_df(selected_table).fillna(0)
        if metric_col not in df.columns:
            return {"error": f"Expected metric column '{metric_col}' not found"}

        return {"data": _normalize_to_month_value(df, metric_col), "chart_type": selected_chart_type}
    except Exception as e:
        return {"error": str(e)}


# =============================================================================
# VIDEO EXPLORER  —  Infinite-scroll endpoints
# =============================================================================

VIDEO_FILE = os.path.join(os.path.dirname(__file__), "../data/video_list_data_obfuscated.csv")


@lru_cache(maxsize=1)
def _load_video_df() -> pd.DataFrame:
    """Load and normalise the video CSV once; cache in memory."""
    df = pd.read_csv(VIDEO_FILE)
    df["Headline"] = df["Headline"].fillna("")
    df["Type"] = df["Type"].fillna("")
    df["Uploaded By"] = df["Uploaded By"].fillna("")
    df["Published"] = df["Published"].fillna("")
    df["Video ID"] = pd.to_numeric(df["Video ID"], errors="coerce")
    return df


def _make_thumbnail_url(video_id, fallback_seed) -> str:
    seed = str(video_id) if video_id is not None and not pd.isna(video_id) else str(fallback_seed)
    return f"https://picsum.photos/seed/{seed}/400/200"


@app.get("/videos/meta")
def videos_meta():
    df = _load_video_df()
    types = sorted([t for t in df["Type"].dropna().unique() if str(t).strip() and str(t).lower() != "unknown"])
    uploaders = sorted([u for u in df["Uploaded By"].dropna().unique() if str(u).strip() and str(u).lower() != "unknown"])
    return {"types": types, "uploaded_by": uploaders}


@app.get("/videos")
def videos(
    name: str = "",
    type: str = "",
    uploaded_by: str = "",
    video_id: str = "",
    offset: int = 0,
    limit: int = 12,
):
    """Paginated video list for infinite scroll, sorted by Headline ASC then Video ID ASC."""
    df = _load_video_df()

    if name:
        df = df[df["Headline"].str.contains(name, case=False, na=False)]
    if type:
        df = df[df["Type"].str.lower() == str(type).lower()]
    if uploaded_by:
        df = df[df["Uploaded By"].str.lower() == str(uploaded_by).lower()]
    if video_id:
        vid_num = pd.to_numeric(video_id, errors="coerce")
        if not pd.isna(vid_num):
            df = df[df["Video ID"] == vid_num]
        else:
            df = df[df["Video ID"].astype("string").str.contains(video_id, case=False, na=False)]

    df = df.copy()
    df["_sort_vid"] = df["Video ID"].fillna(-1)
    df = df.sort_values(by=["Headline", "_sort_vid"], ascending=[True, True], kind="mergesort")
    df = df.drop(columns=["_sort_vid"])

    offset = max(0, int(offset))
    limit = max(1, min(int(limit), 50))
    page_df = df.iloc[offset: offset + limit]

    has_more = (offset + len(page_df)) < len(df)
    next_offset = offset + len(page_df)

    def _safe_str(x):
        return "" if pd.isna(x) else str(x)

    videos_list = []
    for idx, row in page_df.reset_index(drop=True).iterrows():
        vid = row.get("Video ID", None)
        videos_list.append({
            "video_id": None if pd.isna(vid) else int(vid),
            "title": _safe_str(row.get("Headline", "")),
            "type": _safe_str(row.get("Type", "")),
            "uploaded_by": _safe_str(row.get("Uploaded By", "")),
            "published": _safe_str(row.get("Published", "")),
            "published_platform": _safe_str(row.get("Published Platform", "")),
            "published_url": _safe_str(row.get("Published URL", "")),
            "source": _safe_str(row.get("Source", "")),
            "thumbnail": _make_thumbnail_url(vid, idx),
        })

    return {"data": videos_list, "has_more": has_more, "next_offset": next_offset}


# =============================================================================
# FUNNEL  —  Output / Language / Input Type  KPI endpoints
# =============================================================================

@app.get("/funnel/kpi/{kpi_name}")
def funnel_kpi(kpi_name: str):
    kpi_map = {
        "output_format_publish_rate":       get_kpi11_output_format_publish_rate,
        "output_format_mix_distribution":   get_kpi12_output_format_mix_distribution,
        "language_publish_rate":            get_kpi13_language_publish_rate,
        "language_upload_share":            get_kpi15_language_upload_share,
        "input_type_publish_rate":          get_kpi09_input_type_publish_rate,
        "input_type_amplification_ratio":   get_kpi10_input_type_amplification_ratio,
        "unknown_input_type_rate":          get_kpi19_unknown_input_type_rate,
    }

    if kpi_name not in kpi_map:
        return {"error": f"Invalid kpi_name: {kpi_name}"}

    try:
        return _kpi_response_from_fn(kpi_map[kpi_name])
    except Exception as e:
        return {"error": str(e)}


# =============================================================================
# FUNNEL  —  Output / Language / Input Type  Plot endpoints
# =============================================================================

@app.get("/funnel/plot/{plot_name}")
def funnel_plot(plot_name: str):
    plot_map = {
        "output_format_publish_rate":           get_output_format_publish_rate_plot,
        "output_format_mix_distribution":       get_output_format_mix_distribution_plot,
        "language_publish_rate":                get_language_publish_rate,
        "language_upload_share":                get_language_upload_share,
        "input_type_publish_rate":              get_input_type_publish_rate_plot,
        "input_type_amplification_ratio":       get_input_type_amplification_ratio_plot,
        "unknown_input_type_contribution":      get_unknown_input_type_contribution_plot,
    }

    if plot_name not in plot_map:
        return {"error": f"Invalid plot_name: {plot_name}"}

    try:
        return _plot_response_from_fn(plot_map[plot_name])
    except Exception as e:
        return {"error": str(e)}