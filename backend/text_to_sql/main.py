"""
main.py  —  FastAPI server for Frammer AI Analytics
=====================================================
Exposes nlq_pipeline.nlq_query() over HTTP so a React
frontend (or any HTTP client) can call it.

Usage:
    pip install fastapi uvicorn
    uvicorn main:app --reload --port 8000

Endpoints:
    POST /query   { "question": "..." }
    GET  /health
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from nlq_pipeline import nlq_query

app = FastAPI(title="Frammer AI Analytics API", version="1.0.0")

# ── CORS: allow the Vite dev server and any same-origin prod deployment ────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",   # Vite default
        "http://localhost:3000",   # CRA default
        "http://127.0.0.1:5173",
    ],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


# ─────────────────────────────────────────────────────────────────────────────
# REQUEST / RESPONSE MODELS
# ─────────────────────────────────────────────────────────────────────────────

class QueryRequest(BaseModel):
    question: str


# ─────────────────────────────────────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/query")
def query(req: QueryRequest):
    if not req.question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty.")

    # nlq_query() now returns a fully JSON-serializable dict
    result = nlq_query(req.question)

    # Surface pipeline errors as HTTP 200 with error field intact —
    # the frontend decides how to display them, not the server.
    return JSONResponse(content=result)