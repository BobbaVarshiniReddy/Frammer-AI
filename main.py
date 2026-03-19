"""
main.py  —  FastAPI server for Frammer AI Analytics
=====================================================
Place this file at the project ROOT (same level as package.json).

Folder structure:
    Frammer-AI/
    ├── main.py              ← this file
    ├── package.json
    ├── text_to_sql/
    │   ├── __init__.py      ← create this (empty file)
    │   ├── config.py
    │   ├── nlq_pipeline.py
    │   ├── visualize.py
    │   └── ...
    └── src/

Usage:
    pip install fastapi uvicorn
    uvicorn main:app --reload --port 8000
"""

import logging
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from text_to_sql.nlq_pipeline import nlq_query

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(title="Frammer AI Analytics API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",   # React (CRA default)
        "http://localhost:5173",   # Vite default
        "http://127.0.0.1:3000",
    ],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


# ── Models ────────────────────────────────────────────────────────────────────
class QueryRequest(BaseModel):
    question: str


# ── Routes ───────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/query")
def query(req: QueryRequest):
    if not req.question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty.")

    logger.info("Question: %s", req.question)
    result = nlq_query(req.question)

    if result["error"]:
        logger.warning("Pipeline error: %s", result["error"])
    else:
        logger.info("OK — %d rows", result["explanation"].get("rows_returned", 0))

    return JSONResponse(content=result)