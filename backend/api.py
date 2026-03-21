# """
# api.py  —  Frammer AI — Unified FastAPI Backend
# ================================================
# Place this file inside the backend/ folder.

# Folder structure:
#     Frammer-AI/
#     ├── backend/
#     │   ├── api.py               ← this file
#     │   ├── database.py          ← create this (see below)
#     │   ├── text_to_sql/
#     │   │   ├── __init__.py      ← empty file
#     │   │   ├── config.py
#     │   │   ├── nlq_pipeline.py
#     │   │   └── visualize.py
#     │   ├── raw_video_list_kpis.py
#     │   ├── raw_monthly_duration_kpis.py
#     │   └── ... (all other kpi/plot files)
#     └── src/

# Usage:
#     cd backend
#     uvicorn api:app --reload --port 8000
# """

# import logging
# import os
# from functools import lru_cache

# import pandas as pd
# import pyarrow as pa
# from fastapi import FastAPI, HTTPException
# from fastapi.middleware.cors import CORSMiddleware
# from fastapi.responses import JSONResponse
# from pydantic import BaseModel

# # ── Chatbot (text_to_sql) ─────────────────────────────────────────────────────
# from text_to_sql.nlq_pipeline import nlq_query

# # ── Video list KPIs ───────────────────────────────────────────────────────────
# from raw_video_list_kpis import (
#     get_kpi14_published_platform_distribution,
#     get_kpi16_published_rate_per_uploader,
#     get_kpi17_top_uploaders,
#     get_kpi19_unknown_team_name_rate,
#     get_kpi20_published_url_completeness,
#     get_kpi21_overall_published_rate,
#     get_kpi22_duplicate_video_id_count,
# )
# from raw_video_list_plots import (
#     get_published_vs_content_type,
#     get_published_vs_team,
#     get_data_completeness_heatmap,
#     get_top_uploaders,
#     get_published_pct_by_type,
#     get_published_pct_by_uploader,
# )

# # ── Monthly (Usage & Trends) ──────────────────────────────────────────────────
# from raw_monthly_duration_kpis import (
#     get_kpi05_total_content_hours_processed,
#     get_kpi05_monthly_created_duration_trend,
#     get_kpi03_mom_duration_growth,
# )
# from raw_monthly_duration_plots import (
#     get_grouped_duration_vs_month,
#     get_top5_uploaded_duration,
#     get_top5_published_duration,
# )

# # ── Language KPIs ─────────────────────────────────────────────────────────────
# from by_language_kpis import (
#     get_kpi13_language_publish_rate,
#     get_kpi15_language_upload_share,
# )

# # ── Output type KPIs ──────────────────────────────────────────────────────────
# from output_type_kpis import (
#     get_kpi11_output_format_publish_rate,
#     get_kpi12_output_format_mix_distribution,
# )

# # ── Input type KPIs ───────────────────────────────────────────────────────────
# from raw_input_type_kpis import (
#     get_kpi09_input_type_publish_rate,
#     get_kpi10_input_type_amplification_ratio,
#     get_kpi19_unknown_input_type_rate,
# )

# # ─────────────────────────────────────────────────────────────────────────────
# # Logging
# # ─────────────────────────────────────────────────────────────────────────────

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
# )
# logger = logging.getLogger(__name__)

# # ─────────────────────────────────────────────────────────────────────────────
# # App + CORS
# # ─────────────────────────────────────────────────────────────────────────────

# app = FastAPI(title="Frammer AI Analytics API", version="2.0.0")

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=[
#         "http://localhost:3000",
#         "http://localhost:5173",
#         "http://127.0.0.1:3000",
#     ],
#     allow_methods=["GET", "POST", "OPTIONS"],
#     allow_headers=["*"],
# )

# # ─────────────────────────────────────────────────────────────────────────────
# # Shared helpers
# # ─────────────────────────────────────────────────────────────────────────────

# def _to_df(table) -> pd.DataFrame:
#     """Convert pyarrow Table or RecordBatchReader → pandas DataFrame."""
#     if isinstance(table, pa.lib.RecordBatchReader):
#         table = table.read_all()
#     return table.to_pandas().fillna(0)


# def _kpi_response(kpi_fn) -> dict:
#     """
#     Standard wrapper for KPI functions.
#     Handles both return styles:
#       - (table, chart_type)          — direct tuple
#       - [[table, chart_type], ...]   — list of lists
#     """
#     try:
#         results = kpi_fn()
#         if not results:
#             return {"data": [], "chart_type": None}
#         # Direct tuple: (table, chart_type)
#         if isinstance(results, tuple):
#             table, chart_type = results
#         # List of lists: [[table, chart_type], ...]
#         else:
#             table, chart_type = results[0][0], results[0][1]
#         return {"data": _to_df(table).to_dict(orient="records"), "chart_type": chart_type}
#     except Exception as e:
#         logger.error("KPI error in %s: %s", kpi_fn.__name__, e)
#         return {"error": str(e)}


# def _plot_response(plot_fn, preferred=("bar", "line", "heatmap")) -> dict:
#     """
#     Standard wrapper for plot functions.
#     Handles both return styles:
#       - (table, chart_type)          — direct tuple
#       - [[table, chart_type], ...]   — list of lists
#     """
#     try:
#         result = plot_fn()
#         if not result:
#             return {"data": [], "chart_type": None}
#         # Direct tuple: (table, chart_type)
#         if isinstance(result, tuple):
#             return {"data": _to_df(result[0]).to_dict(orient="records"), "chart_type": result[1]}
#         # List of lists: [[table, chart_type], ...]
#         selected = None
#         for pref in preferred:
#             for item in result:
#                 if item[1] == pref:
#                     selected = item
#                     break
#             if selected:
#                 break
#         if selected is None:
#             selected = result[0]
#         return {"data": _to_df(selected[0]).to_dict(orient="records"), "chart_type": selected[1]}
#     except Exception as e:
#         logger.error("Plot error in %s: %s", plot_fn.__name__, e)
#         return {"error": str(e)}


# # ─────────────────────────────────────────────────────────────────────────────
# # Health check
# # ─────────────────────────────────────────────────────────────────────────────

# @app.get("/health")
# def health():
#     return {"status": "ok"}


# # ─────────────────────────────────────────────────────────────────────────────
# # Chatbot — NLQ → SQL
# # ─────────────────────────────────────────────────────────────────────────────

# class QueryRequest(BaseModel):
#     question: str


# @app.post("/query")
# def query(req: QueryRequest):
#     if not req.question.strip():
#         raise HTTPException(status_code=400, detail="Question cannot be empty.")
#     logger.info("Chatbot question: %s", req.question)
#     result = nlq_query(req.question)
#     if result["error"]:
#         logger.warning("Pipeline error: %s", result["error"])
#     else:
#         logger.info("OK — %d rows", result["explanation"].get("rows_returned", 0))
#     return JSONResponse(content=result)


# # ─────────────────────────────────────────────────────────────────────────────
# # Video list KPI endpoints
# # ─────────────────────────────────────────────────────────────────────────────

# @app.get("/kpi/platform_distribution")
# def kpi_platform_distribution():
#     return _kpi_response(get_kpi14_published_platform_distribution)

# @app.get("/kpi/published_rate_per_uploader")
# def kpi_published_rate_per_uploader():
#     return _kpi_response(get_kpi16_published_rate_per_uploader)

# @app.get("/kpi/top_uploaders")
# def kpi_top_uploaders():
#     return _kpi_response(get_kpi17_top_uploaders)

# @app.get("/kpi/unknown_team_rate")
# def kpi_unknown_team_rate():
#     return _kpi_response(get_kpi19_unknown_team_name_rate)

# @app.get("/kpi/published_url_completeness")
# def kpi_published_url_completeness():
#     return _kpi_response(get_kpi20_published_url_completeness)

# @app.get("/kpi/overall_published_rate")
# def kpi_overall_published_rate():
#     return _kpi_response(get_kpi21_overall_published_rate)

# @app.get("/kpi/duplicate_video_id_count")
# def kpi_duplicate_video_id_count():
#     return _kpi_response(get_kpi22_duplicate_video_id_count)

# @app.get("/kpi/all")
# def get_all_kpis():
#     try:
#         kpi_functions = [
#             get_kpi11_output_format_publish_rate,
#             get_kpi12_output_format_mix_distribution,
#             get_kpi09_input_type_publish_rate,
#             get_kpi10_input_type_amplification_ratio,
#             get_kpi19_unknown_input_type_rate,
#         ]
#         all_kpis = []
#         for fn in kpi_functions:
#             try:
#                 results = fn()
#             except Exception as e:
#                 logger.warning("Skipping %s: %s", fn.__name__, e)
#                 continue
#             for table, chart_type in results:
#                 df = _to_df(table)
#                 if df.empty:
#                     continue
#                 all_kpis.append({
#                     "data": df.to_dict(orient="records"),
#                     "chart_type": chart_type,
#                 })
#         return {"kpis": all_kpis}
#     except Exception as e:
#         return {"error": str(e)}


# # ─────────────────────────────────────────────────────────────────────────────
# # Video list plot endpoints
# # ─────────────────────────────────────────────────────────────────────────────

# @app.get("/plot/custom_plot")
# def custom_plot(plot_name: str):
#     plot_map = {
#         "published_vs_content_type": get_published_vs_content_type,
#         "published_vs_team":         get_published_vs_team,
#         "data_completeness_heatmap": get_data_completeness_heatmap,
#         "top_uploaders":             get_top_uploaders,
#         "published_pct_by_type":     get_published_pct_by_type,
#         "published_pct_by_uploader": get_published_pct_by_uploader,
#     }
#     if plot_name not in plot_map:
#         return {"error": f"Invalid plot_name: {plot_name}"}
#     return _plot_response(plot_map[plot_name])


# # ─────────────────────────────────────────────────────────────────────────────
# # Monthly KPI endpoints (Usage & Trends)
# # ─────────────────────────────────────────────────────────────────────────────

# def _normalize_month_value(df: pd.DataFrame, metric_col: str) -> list[dict]:
#     if "Month" in df.columns:
#         return df[["Month", metric_col]].rename(columns={metric_col: "value"}).to_dict(orient="records")
#     return df[[metric_col]].rename(columns={metric_col: "value"}).to_dict(orient="records")


# @app.get("/monthly/kpi/{kpi_name}")
# def monthly_kpi(kpi_name: str):
#     kpi_map = {
#         "total_content_hours_processed":    (get_kpi05_total_content_hours_processed,    "total_content_hours",   "kpi_card"),
#         "monthly_created_duration_trend":   (get_kpi05_monthly_created_duration_trend,   "total_created_hours",   "line"),
#         "mom_duration_growth":              (get_kpi03_mom_duration_growth,              "mom_growth_pct",        "line"),
#     }
#     if kpi_name not in kpi_map:
#         return {"error": f"Invalid kpi_name: {kpi_name}"}
#     try:
#         fn, metric_col, preferred_chart = kpi_map[kpi_name]
#         options = fn()
#         selected = next(((t, c) for t, c in options if c == preferred_chart), options[0])
#         df = _to_df(selected[0])
#         if metric_col not in df.columns:
#             return {"error": f"Column '{metric_col}' not found"}
#         return {"data": _normalize_month_value(df, metric_col), "chart_type": selected[1]}
#     except Exception as e:
#         return {"error": str(e)}


# @app.get("/monthly/plot/custom_plot")
# def monthly_plot(plot_name: str):
#     try:
#         duration_plots = {
#             "uploaded_duration_vs_month":  "Uploaded_hours",
#             "created_duration_vs_month":   "Created_hours",
#             "published_duration_vs_month": "Published_hours",
#         }
#         if plot_name in duration_plots:
#             metric_col = duration_plots[plot_name]
#             options = get_grouped_duration_vs_month()
#             table, _ = next(((t, c) for t, c in options if c == "line"), options[0])
#             df = _to_df(table)
#             if metric_col not in df.columns:
#                 return {"error": f"Column '{metric_col}' not found"}
#             return {
#                 "data": df[["Month", metric_col]].rename(columns={metric_col: "value"}).to_dict(orient="records"),
#                 "chart_type": "line",
#             }

#         plot_map = {
#             "top5_uploaded_duration":  (get_top5_uploaded_duration,  "Uploaded_hours",  "bar"),
#             "top5_published_duration": (get_top5_published_duration, "Published_hours", "bar"),
#         }
#         if plot_name not in plot_map:
#             return {"error": f"Invalid plot_name: {plot_name}"}

#         fn, metric_col, preferred = plot_map[plot_name]
#         options = fn()
#         selected = next(((t, c) for t, c in options if c == preferred), options[0])
#         df = _to_df(selected[0])
#         if metric_col not in df.columns:
#             return {"error": f"Column '{metric_col}' not found"}
#         return {"data": _normalize_month_value(df, metric_col), "chart_type": selected[1]}

#     except Exception as e:
#         return {"error": str(e)}


# # ─────────────────────────────────────────────────────────────────────────────
# # Funnel endpoints (Output / Language / Input type)
# # ─────────────────────────────────────────────────────────────────────────────

# @app.get("/funnel/kpi/{kpi_name}")
# def funnel_kpi(kpi_name: str):
#     kpi_map = {
#         "output_format_publish_rate":      get_kpi11_output_format_publish_rate,
#         "output_format_mix_distribution":  get_kpi12_output_format_mix_distribution,
#         "language_publish_rate":           get_kpi13_language_publish_rate,
#         "language_upload_share":           get_kpi15_language_upload_share,
#         "input_type_publish_rate":         get_kpi09_input_type_publish_rate,
#         "input_type_amplification_ratio":  get_kpi10_input_type_amplification_ratio,
#         "unknown_input_type_rate":         get_kpi19_unknown_input_type_rate,
#     }
#     if kpi_name not in kpi_map:
#         return {"error": f"Invalid kpi_name: {kpi_name}"}
#     return _plot_response(kpi_map[kpi_name])


# @app.get("/funnel/plot/{plot_name}")
# def funnel_plot(plot_name: str):
#     from output_type_plots import (
#         get_output_format_publish_rate_plot,
#         get_output_format_mix_distribution_plot,
#     )
#     from by_language_plots import (
#         get_language_publish_rate,
#         get_language_upload_share,
#     )
#     from raw_input_type_plots import (
#         get_input_type_publish_rate_plot,
#         get_input_type_amplification_ratio_plot,
#         get_unknown_input_type_contribution_plot,
#     )

#     plot_map = {
#         "output_format_publish_rate":          get_output_format_publish_rate_plot,
#         "output_format_mix_distribution":      get_output_format_mix_distribution_plot,
#         "language_publish_rate":               get_language_publish_rate,
#         "language_upload_share":               get_language_upload_share,
#         "input_type_publish_rate":             get_input_type_publish_rate_plot,
#         "input_type_amplification_ratio":      get_input_type_amplification_ratio_plot,
#         "unknown_input_type_contribution":     get_unknown_input_type_contribution_plot,
#     }
#     if plot_name not in plot_map:
#         return {"error": f"Invalid plot_name: {plot_name}"}
#     return _plot_response(plot_map[plot_name])


# # ─────────────────────────────────────────────────────────────────────────────
# # Video Explorer endpoints
# # ─────────────────────────────────────────────────────────────────────────────

# VIDEO_FILE = os.path.join(os.path.dirname(__file__), "../data/video_list_data_obfuscated.csv")


# @lru_cache(maxsize=1)
# def _load_video_df() -> pd.DataFrame:
#     df = pd.read_csv(VIDEO_FILE)
#     df["Headline"]   = df["Headline"].fillna("")
#     df["Type"]       = df["Type"].fillna("")
#     df["Uploaded By"] = df["Uploaded By"].fillna("")
#     df["Published"]  = df["Published"].fillna("")
#     df["Video ID"]   = pd.to_numeric(df["Video ID"], errors="coerce")
#     return df


# def _thumbnail_url(video_id, fallback_seed) -> str:
#     seed = str(int(video_id)) if video_id is not None and not pd.isna(video_id) else str(fallback_seed)
#     return f"https://picsum.photos/seed/{seed}/400/200"


# @app.get("/videos/meta")
# def videos_meta():
#     df = _load_video_df()
#     types     = sorted([t for t in df["Type"].dropna().unique() if str(t).strip() and str(t).lower() != "unknown"])
#     uploaders = sorted([u for u in df["Uploaded By"].dropna().unique() if str(u).strip() and str(u).lower() != "unknown"])
#     return {"types": types, "uploaded_by": uploaders}


# @app.get("/videos")
# def videos(
#     name:        str = "",
#     type:        str = "",
#     uploaded_by: str = "",
#     video_id:    str = "",
#     offset:      int = 0,
#     limit:       int = 12,
# ):
#     df = _load_video_df()

#     if name:
#         df = df[df["Headline"].str.contains(name, case=False, na=False)]
#     if type:
#         df = df[df["Type"].str.lower() == type.lower()]
#     if uploaded_by:
#         df = df[df["Uploaded By"].str.lower() == uploaded_by.lower()]
#     if video_id:
#         vid_num = pd.to_numeric(video_id, errors="coerce")
#         if not pd.isna(vid_num):
#             df = df[df["Video ID"] == vid_num]
#         else:
#             df = df[df["Video ID"].astype("string").str.contains(video_id, case=False, na=False)]

#     df = df.copy()
#     df["_sort_vid"] = df["Video ID"].fillna(-1)
#     df = df.sort_values(by=["Headline", "_sort_vid"], ascending=True, kind="mergesort").drop(columns=["_sort_vid"])

#     offset   = max(0, offset)
#     limit    = max(1, min(limit, 50))
#     page_df  = df.iloc[offset: offset + limit]
#     has_more = (offset + len(page_df)) < len(df)

#     def _s(x):
#         return "" if pd.isna(x) else str(x)

#     result = []
#     for idx, row in page_df.reset_index(drop=True).iterrows():
#         vid = row.get("Video ID")
#         result.append({
#             "video_id":          None if pd.isna(vid) else int(vid),
#             "title":             _s(row.get("Headline", "")),
#             "type":              _s(row.get("Type", "")),
#             "uploaded_by":       _s(row.get("Uploaded By", "")),
#             "published":         _s(row.get("Published", "")),
#             "published_platform": _s(row.get("Published Platform", "")),
#             "published_url":     _s(row.get("Published URL", "")),
#             "source":            _s(row.get("Source", "")),
#             "thumbnail":         _thumbnail_url(vid, idx),
#         })

#     return {"data": result, "has_more": has_more, "next_offset": offset + len(page_df)}

"""
api.py  —  Frammer AI — Unified FastAPI Backend
================================================
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

# ── Chatbot ───────────────────────────────────────────────────────────────────
from text_to_sql.nlq_pipeline import nlq_query

# ── Language Plots ────────────────────────────────────────────────────────────
from by_language_plots import (
    get_language_publish_rate,
    get_language_upload_share,
)

# ── Channel Wise Duration Plots ───────────────────────────────────────────────
from channel_wise_publishing_duration_plots import (
    get_platform_duration_distribution,
    get_absolute_duration_by_platform,
    get_channel_total_duration,
    get_channel_platform_duration_heatmap,
    get_top_channels_by_duration,
    get_platform_share_per_channel,
    get_platform_coverage_heatmap,
)

# ── Channel Wise Publishing Plots ─────────────────────────────────────────────
from channelwise_publishing_plots import (
    get_published_count_per_platform,
    get_platform_distribution_pct,
    get_zero_vs_active_channels,
    get_top_channels_by_publishes,
    get_channel_platform_heatmap,
    get_platform_diversity_per_channel,
    get_channel_publish_share_per_platform,
)

# ── User Channel Combined Plots ───────────────────────────────────────────────
from combined_by_user_channel_plots import (
    get_publish_rate_by_channel          as get_user_channel_publish_rate,
    get_created_vs_published_by_channel,
    get_upload_volume_by_channel,
    get_top3_uploaders_per_channel,
    get_top3_user_publish_rate_per_channel,
    get_user_channel_publish_rate_heatmap,
    get_top_users_publish_rate_across_channels,
    get_user_contribution_per_channel,
    get_top10_users_by_publish_rate,
)

# ── User KPI Plots ────────────────────────────────────────────────────────────
from combined_by_user_kpis_plots import (
    get_plot_publish_rate,
    get_plot_top_uploaders,
)

# ── Output Type Plots ─────────────────────────────────────────────────────────
from output_type_plots import (
    get_output_format_publish_rate_plot,
    get_output_format_mix_distribution_plot,
)

# ── Client Combined Plots ─────────────────────────────────────────────────────
from raw_client1_combined_plots import (
    get_publish_rate_by_channel          as get_client_publish_rate_by_channel,
    get_content_flow_counts,
    get_total_duration_by_channel,
    get_created_vs_published_efficiency,
    get_zero_publish_channels_detail,
)

# ── Input Type Plots ──────────────────────────────────────────────────────────
from raw_input_type_plots import (
    get_input_type_publish_rate_plot,
    get_input_type_amplification_ratio_plot,
    get_unknown_input_type_contribution_plot,
    get_publish_vs_amplification_scatter,
)

# ── Monthly Count Plots ───────────────────────────────────────────────────────
from raw_monthly_count_plots import (
    get_total_created_vs_month,
    get_total_uploaded_vs_month,
    get_total_published_vs_month,
    get_mom_increase_published,
    get_mom_increase_uploaded,
)

# ── Monthly Duration Plots ────────────────────────────────────────────────────
from raw_monthly_duration_plots import (
    get_grouped_duration_vs_month,
    get_top5_uploaded_duration,
    get_top5_published_duration,
)

# ── Video List Plots ──────────────────────────────────────────────────────────
from raw_video_list_plots import (
    get_published_vs_content_type,
    get_published_vs_team,
    get_data_completeness_heatmap,
    get_top_uploaders,
    get_published_pct_by_type,
    get_published_pct_by_uploader,
)

# ── User Channel KPIs ─────────────────────────────────────────────────────────
from combined_by_user_channel_kpi import (
    get_kpi06_publish_rate_by_channel    as get_kpi06_user_channel_publish_rate,
    get_kpi16_user_publish_rate_within_channel,
    get_kpi17_user_upload_volume_by_channel,
    get_kpi18_channel_user_cross_dimension,
)

# ── User KPIs ─────────────────────────────────────────────────────────────────
from combined_by_user_kpis import (
    get_user_publish_rate,
    get_top_uploaders                    as get_user_top_uploaders,
    get_qa_activity_percent,
)

# ── Output Type KPIs ──────────────────────────────────────────────────────────
from output_type_kpis import (
    get_kpi11_output_format_publish_rate,
    get_kpi12_output_format_mix_distribution,
)

# ── Raw Client KPIs ───────────────────────────────────────────────────────────
from raw_client1_combined_kpis import (
    get_kpi01_overall_publish_rate       as get_client_kpi01_overall_publish_rate,
    get_kpi02_amplification_ratio,
    get_kpi04_dropoff_volume,
    get_kpi05_total_content_hours,
    get_kpi06_publish_rate_by_channel    as get_client_kpi06_publish_rate,
    get_kpi08_zero_publish_channels,
)

# ── Input Type KPIs ───────────────────────────────────────────────────────────
from raw_input_type_kpis import (
    get_kpi09_input_type_publish_rate,
    get_kpi10_input_type_amplification_ratio,
    get_kpi19_unknown_input_type_rate,
)

# ── Monthly Count KPIs ────────────────────────────────────────────────────────
from raw_monthly_count_kpis import (
    get_kpi03_mom_upload_growth,
    get_kpi07_monthly_publish_rate,
    get_kpi01_overall_publish_rate       as get_monthly_kpi01_overall_publish_rate,
    get_kpi02_monthly_amplification_ratio,
)

# ── Monthly Duration KPIs ─────────────────────────────────────────────────────
from raw_monthly_duration_kpis import (
    get_kpi05_total_content_hours_processed,
    get_kpi05_monthly_created_duration_trend,
    get_kpi03_mom_duration_growth,
)

# ── Video List KPIs ───────────────────────────────────────────────────────────
from raw_video_list_kpis import (
    get_kpi14_published_platform_distribution,
    get_kpi16_published_rate_per_uploader,
    get_kpi17_top_uploaders,
    get_kpi19_unknown_team_name_rate,
    get_kpi20_published_url_completeness,
    get_kpi21_overall_published_rate,
    get_kpi22_duplicate_video_id_count,
)

# ── Channelwise KPIs ──────────────────────────────────────────────────────────
from channelwise_publishing_kpi import (
    get_kpi14_platform_publish_distribution,
    get_kpi08_zero_publish_channel_count,
    get_kpi08_zero_publish_channel_rate,
)

# ── Channel Duration KPIs ─────────────────────────────────────────────────────
from channel_wise_publishing_duration_kpi import (
    get_kpi14_platform_duration_distribution,
    get_kpi05_published_duration_by_platform,
    get_channel_total_duration           as get_kpi_channel_total_duration,
    get_channel_platform_duration_matrix,
)

# ── Language KPIs ─────────────────────────────────────────────────────────────
from by_language_kpis import (
    get_kpi13_language_publish_rate,
    get_kpi15_language_upload_share,
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
    if isinstance(table, pa.lib.RecordBatchReader):
        table = table.read_all()
    return table.to_pandas().fillna(0)


def _kpi_response(kpi_fn) -> dict:
    try:
        results = kpi_fn()
        if not results:
            return {"data": [], "chart_type": None}
        if isinstance(results, tuple):
            table, chart_type = results
        else:
            table, chart_type = results[0][0], results[0][1]
        return {"data": _to_df(table).to_dict(orient="records"), "chart_type": chart_type}
    except Exception as e:
        logger.error("KPI error in %s: %s", kpi_fn.__name__, e)
        return {"error": str(e)}


def _plot_response(plot_fn, preferred=("bar", "line", "heatmap")) -> dict:
    try:
        result = plot_fn()
        if not result:
            return {"data": [], "chart_type": None}
        if isinstance(result, tuple):
            return {"data": _to_df(result[0]).to_dict(orient="records"), "chart_type": result[1]}
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


def _normalize_month_value(df: pd.DataFrame, metric_col: str) -> list[dict]:
    if "Month" in df.columns:
        return df[["Month", metric_col]].rename(columns={metric_col: "value"}).to_dict(orient="records")
    return df[[metric_col]].rename(columns={metric_col: "value"}).to_dict(orient="records")


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
# Tab 1 — Executive Summary
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/executive/kpi/{kpi_name}")
def executive_kpi(kpi_name: str):
    kpi_map = {
        "overall_publish_rate"          : get_client_kpi01_overall_publish_rate,
        "amplification_ratio"           : get_kpi02_amplification_ratio,
        "total_content_hours"           : get_kpi05_total_content_hours,
        "dropoff_volume"                : get_kpi04_dropoff_volume,
        "zero_publish_channels"         : get_kpi08_zero_publish_channels,
        "total_content_hours_processed" : get_kpi05_total_content_hours_processed,
        "overall_published_rate"        : get_kpi21_overall_published_rate,
        "duplicate_video_id_count"      : get_kpi22_duplicate_video_id_count,
        "unknown_team_rate"             : get_kpi19_unknown_team_name_rate,
        "published_url_completeness"    : get_kpi20_published_url_completeness,
    }
    if kpi_name not in kpi_map:
        return {"error": f"Invalid kpi_name: {kpi_name}"}
    return _kpi_response(kpi_map[kpi_name])


@app.get("/executive/plot/{plot_name}")
def executive_plot(plot_name: str):
    plot_map = {
        "publish_rate_by_channel"       : get_client_publish_rate_by_channel,
        "zero_publish_channels_detail"  : get_zero_publish_channels_detail,
        "data_completeness_heatmap"     : get_data_completeness_heatmap,
        "published_vs_team"             : get_published_vs_team,
    }
    if plot_name not in plot_map:
        return {"error": f"Invalid plot_name: {plot_name}"}
    return _plot_response(plot_map[plot_name])


# ─────────────────────────────────────────────────────────────────────────────
# Tab 2 — Usage & Trends
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/trends/kpi/{kpi_name}")
def trends_kpi(kpi_name: str):
    kpi_map = {
        "mom_upload_growth"              : get_kpi03_mom_upload_growth,
        "monthly_publish_rate"           : get_kpi07_monthly_publish_rate,
        "monthly_created_duration_trend" : get_kpi05_monthly_created_duration_trend,
        "mom_duration_growth"            : get_kpi03_mom_duration_growth,
        "overall_publish_rate"           : get_monthly_kpi01_overall_publish_rate,
        "monthly_amplification_ratio"    : get_kpi02_monthly_amplification_ratio,
        "total_content_hours_processed"  : get_kpi05_total_content_hours_processed,
    }
    if kpi_name not in kpi_map:
        return {"error": f"Invalid kpi_name: {kpi_name}"}
    return _kpi_response(kpi_map[kpi_name])


@app.get("/trends/plot/{plot_name}")
def trends_plot(plot_name: str):
    plot_map = {
        # Monthly Count Plots
        "total_created_vs_month"         : get_total_created_vs_month,
        "total_uploaded_vs_month"        : get_total_uploaded_vs_month,
        "total_published_vs_month"       : get_total_published_vs_month,
        "mom_increase_published"         : get_mom_increase_published,
        "mom_increase_uploaded"          : get_mom_increase_uploaded,
        # Client Combined Plots
        "total_duration_by_channel"      : get_total_duration_by_channel,
        # Channel Duration Plots
        "channel_total_duration"         : get_channel_total_duration,
        # Monthly Duration Plots
        "top5_uploaded_duration"         : get_top5_uploaded_duration,
        "top5_published_duration"        : get_top5_published_duration,
    }
    if plot_name not in plot_map:
        return {"error": f"Invalid plot_name: {plot_name}"}
    return _plot_response(plot_map[plot_name])


@app.get("/trends/plot/duration/{metric}")
def trends_duration_plot(metric: str):
    """
    Serves uploaded / created / published duration vs month
    metric: uploaded_duration | created_duration | published_duration
    """
    duration_map = {
        "uploaded_duration"  : "Uploaded_hours",
        "created_duration"   : "Created_hours",
        "published_duration" : "Published_hours",
    }
    if metric not in duration_map:
        return {"error": f"Invalid metric: {metric}"}
    try:
        metric_col = duration_map[metric]
        options    = get_grouped_duration_vs_month()
        table, _   = next(((t, c) for t, c in options if c == "line"), options[0])
        df         = _to_df(table)
        if metric_col not in df.columns:
            return {"error": f"Column '{metric_col}' not found"}
        return {
            "data"       : df[["Month", metric_col]].rename(columns={metric_col: "value"}).to_dict(orient="records"),
            "chart_type" : "line",
        }
    except Exception as e:
        return {"error": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# Tab 3 — Client / Channel / User Analysis
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/channel/kpi/{kpi_name}")
def channel_kpi(kpi_name: str):
    kpi_map = {
        # User Channel KPIs
        "publish_rate_by_channel"              : get_kpi06_user_channel_publish_rate,
        "user_publish_rate_within_channel"     : get_kpi16_user_publish_rate_within_channel,
        "user_upload_volume_by_channel"        : get_kpi17_user_upload_volume_by_channel,
        "channel_user_cross_dimension"         : get_kpi18_channel_user_cross_dimension,
        # User KPIs
        "user_publish_rate"                    : get_user_publish_rate,
        "qa_activity_percent"                  : get_qa_activity_percent,
        # Language KPIs
        "language_publish_rate"                : get_kpi13_language_publish_rate,
        "language_upload_share"                : get_kpi15_language_upload_share,
        # Platform KPIs
        "platform_publish_distribution"        : get_kpi14_platform_publish_distribution,
        "platform_duration_distribution"       : get_kpi14_platform_duration_distribution,
        "published_duration_by_platform"       : get_kpi05_published_duration_by_platform,
        "channel_platform_duration_matrix"     : get_channel_platform_duration_matrix,
        # Video List KPIs
        "published_rate_per_uploader"          : get_kpi16_published_rate_per_uploader,
        "top_uploaders_kpi"                    : get_kpi17_top_uploaders,
        # Channel KPIs
        "zero_publish_channel_count"           : get_kpi08_zero_publish_channel_count,
        "zero_publish_channel_rate"            : get_kpi08_zero_publish_channel_rate,
    }
    if kpi_name not in kpi_map:
        return {"error": f"Invalid kpi_name: {kpi_name}"}
    return _kpi_response(kpi_map[kpi_name])


@app.get("/channel/plot/{plot_name}")
def channel_plot(plot_name: str):
    plot_map = {
        # User Channel Plots
        "created_vs_published_by_channel"      : get_created_vs_published_by_channel,
        "upload_volume_by_channel"             : get_upload_volume_by_channel,
        "top3_uploaders_per_channel"           : get_top3_uploaders_per_channel,
        "top3_user_publish_rate_per_channel"   : get_top3_user_publish_rate_per_channel,
        "user_channel_publish_rate_heatmap"    : get_user_channel_publish_rate_heatmap,
        "top_users_publish_rate_across_channels": get_top_users_publish_rate_across_channels,
        "user_contribution_per_channel"        : get_user_contribution_per_channel,
        "top10_users_by_publish_rate"          : get_top10_users_by_publish_rate,
        # User KPI Plots
        "plot_publish_rate"                    : get_plot_publish_rate,
        "plot_top_uploaders"                   : get_plot_top_uploaders,
        # Channelwise Publishing Plots
        "top_channels_by_publishes"            : get_top_channels_by_publishes,
        "channel_platform_heatmap"             : get_channel_platform_heatmap,
        "platform_diversity_per_channel"       : get_platform_diversity_per_channel,
        "channel_publish_share_per_platform"   : get_channel_publish_share_per_platform,
        # Language Plots
        "language_publish_rate"                : get_language_publish_rate,
        "language_upload_share"                : get_language_upload_share,
        # Video List Plots
        "published_vs_team"                    : get_published_vs_team,
        "top_uploaders"                        : get_top_uploaders,
        "published_pct_by_uploader"            : get_published_pct_by_uploader,
        # Channel Duration Plots
        "platform_duration_distribution"       : get_platform_duration_distribution,
        "absolute_duration_by_platform"        : get_absolute_duration_by_platform,
        "channel_platform_duration_heatmap"    : get_channel_platform_duration_heatmap,
        "top_channels_by_duration"             : get_top_channels_by_duration,
        "platform_share_per_channel"           : get_platform_share_per_channel,
        "platform_coverage_heatmap"            : get_platform_coverage_heatmap,
    }
    if plot_name not in plot_map:
        return {"error": f"Invalid plot_name: {plot_name}"}
    return _plot_response(plot_map[plot_name])


# ─────────────────────────────────────────────────────────────────────────────
# Tab 4 — Type Mix & Publishing Funnel
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/funnel/kpi/{kpi_name}")
def funnel_kpi(kpi_name: str):
    kpi_map = {
        # Input Type KPIs
        "input_type_publish_rate"              : get_kpi09_input_type_publish_rate,
        "input_type_amplification_ratio"       : get_kpi10_input_type_amplification_ratio,
        "unknown_input_type_rate"              : get_kpi19_unknown_input_type_rate,
        # Output Type KPIs
        "output_format_publish_rate"           : get_kpi11_output_format_publish_rate,
        "output_format_mix_distribution"       : get_kpi12_output_format_mix_distribution,
        # Platform KPIs
        "published_platform_distribution"      : get_kpi14_published_platform_distribution,
    }
    if kpi_name not in kpi_map:
        return {"error": f"Invalid kpi_name: {kpi_name}"}
    return _kpi_response(kpi_map[kpi_name])


@app.get("/funnel/plot/{plot_name}")
def funnel_plot(plot_name: str):
    plot_map = {
        # Input Type Plots
        "input_type_publish_rate"              : get_input_type_publish_rate_plot,
        "input_type_amplification_ratio"       : get_input_type_amplification_ratio_plot,
        "unknown_input_type_contribution"      : get_unknown_input_type_contribution_plot,
        "publish_vs_amplification_scatter"     : get_publish_vs_amplification_scatter,
        # Output Type Plots
        "output_format_publish_rate"           : get_output_format_publish_rate_plot,
        "output_format_mix_distribution"       : get_output_format_mix_distribution_plot,
        # Video List Plots
        "published_vs_content_type"            : get_published_vs_content_type,
        "published_pct_by_type"                : get_published_pct_by_type,
        # Client Combined Plots
        "content_flow_counts"                  : get_content_flow_counts,
        "created_vs_published_efficiency"      : get_created_vs_published_efficiency,
        # Channelwise Publishing Plots
        "published_count_per_platform"         : get_published_count_per_platform,
        "platform_distribution_pct"            : get_platform_distribution_pct,
    }
    if plot_name not in plot_map:
        return {"error": f"Invalid plot_name: {plot_name}"}
    return _plot_response(plot_map[plot_name])


# ─────────────────────────────────────────────────────────────────────────────
# Tab 5 — Video Explorer
# ─────────────────────────────────────────────────────────────────────────────

VIDEO_FILE = os.path.join(os.path.dirname(__file__), "../data/video_list_data_obfuscated.csv")


@lru_cache(maxsize=1)
def _load_video_df() -> pd.DataFrame:
    df = pd.read_csv(VIDEO_FILE)
    df["Headline"]            = df["Headline"].fillna("")
    df["Type"]                = df["Type"].fillna("")
    df["Uploaded By"]         = df["Uploaded By"].fillna("")
    df["Published"]           = df["Published"].fillna("")
    df["Video ID"]            = pd.to_numeric(df["Video ID"], errors="coerce")
    return df


def _thumbnail_url(video_id, fallback_seed) -> str:
    seed = str(int(video_id)) if video_id is not None and not pd.isna(video_id) else str(fallback_seed)
    return f"https://picsum.photos/seed/{seed}/400/200"


@app.get("/videos/meta")
def videos_meta():
    df        = _load_video_df()
    types     = sorted([t for t in df["Type"].dropna().unique()        if str(t).strip() and str(t).lower() != "unknown"])
    uploaders = sorted([u for u in df["Uploaded By"].dropna().unique() if str(u).strip() and str(u).lower() != "unknown"])
    return {"types": types, "uploaded_by": uploaders}


@app.get("/videos")
def videos(
    name        : str = "",
    type        : str = "",
    uploaded_by : str = "",
    video_id    : str = "",
    offset      : int = 0,
    limit       : int = 12,
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

    df          = df.copy()
    df["_sort"] = df["Video ID"].fillna(-1)
    df          = df.sort_values(by=["Headline", "_sort"], ascending=True, kind="mergesort").drop(columns=["_sort"])

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
            "video_id"           : None if pd.isna(vid) else int(vid),
            "title"              : _s(row.get("Headline", "")),
            "type"               : _s(row.get("Type", "")),
            "uploaded_by"        : _s(row.get("Uploaded By", "")),
            "published"          : _s(row.get("Published", "")),
            "published_platform" : _s(row.get("Published Platform", "")),
            "published_url"      : _s(row.get("Published URL", "")),
            "source"             : _s(row.get("Source", "")),
            "thumbnail"          : _thumbnail_url(vid, idx),
        })

    return {"data": result, "has_more": has_more, "next_offset": offset + len(page_df)}

    