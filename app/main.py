from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import json
import os
import shutil
import pandas as pd
from typing import Optional
from pathlib import Path

from src.orchestrator import OrchestrationAgent, OrchestrationState, PipelineState
from src.utils.utils import load_csv

app = FastAPI()

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global store for pipeline state (In-memory for now, could be persistent)
sessions = {}

@app.post("/pipeline/start")
async def start_pipeline(
    file: UploadFile = File(...),
    user_metadata: str = Form(...)
):
    session_id = os.urandom(8).hex()
    
    # Save file temporarily
    upload_dir = Path("data/raw")
    upload_dir.mkdir(parents=True, exist_ok=True)
    file_path = upload_dir / file.filename
    with file_path.open("wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    state = OrchestrationState()
    state.user_metadata = json.loads(user_metadata)
    
    agent = OrchestrationAgent()
    reply, state = agent.process("", str(file_path), state)
    
    sessions[session_id] = {"agent": agent, "state": state}
    
    return {
        "session_id": session_id,
        "reply": reply,
        "current_step": state.current_step,
        "tasks": state.pending_tasks
    }

@app.post("/pipeline/next")
async def next_step(
    session_id: str = Form(...),
    user_input: str = Form(...)
):
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")
    
    session = sessions[session_id]
    agent = session["agent"]
    state = session["state"]
    
    reply, state = agent.process(user_input, None, state)
    
    return {
        "session_id": session_id,
        "reply": reply,
        "current_step": state.current_step,
        "tasks": state.pending_tasks,
        "kpis": getattr(state, "kpis", [])
    }

@app.get("/pipeline/status/{session_id}")
async def get_status(session_id: str):
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")
    
    state = sessions[session_id]["state"]
    return {
        "current_step": state.current_step,
        "health_report": state.health_report,
        "kpis": getattr(state, "kpis", [])
    }

# Original endpoints for backward compatibility
@app.post("/analyze")
async def analyze(
    file: UploadFile = File(...),
    descriptions: str = Form(...)
):
    # This now uses the orchestrator logic internally but in a single shot if possible
    # (Only works if no HITL is required)
    upload_dir = Path("data/raw")
    upload_dir.mkdir(parents=True, exist_ok=True)
    file_path = upload_dir / file.filename
    with file_path.open("wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    state = OrchestrationState()
    state.user_metadata = json.loads(descriptions)
    agent = OrchestrationAgent()
    
    reply, state = agent.process("", str(file_path), state)
    
    # Auto-resolve low severity and loop until HITL or COMPLETED
    while state.current_step not in [PipelineState.COMPLETED, PipelineState.MAPPING_PENDING, PipelineState.QUALITY_ISSUE_FOUND]:
        reply, state = agent.process("yes", None, state)

    return {
        "reply": reply,
        "current_step": state.current_step,
        "kpis": getattr(state, "kpis", []),
        "health_report": state.health_report
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
