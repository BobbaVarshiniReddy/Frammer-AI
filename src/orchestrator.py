import os
import logging
import pandas as pd
import json
import sys
import select
from typing import Dict, Any, List, Optional, Union, Tuple
from pathlib import Path
from src.agents.preprocessing_agent import schema_mapper, quality_analyzer, column_classifier, data_cleaner
from src.agents.kpi_agent import kpi_planner, kpi_engine, insight_engine
from src.data_engine.extensibility_engine import ExtensibilityEngine
from src.data_engine.loader import DataLoader

# Configure professional logging
logger = logging.getLogger("Orchestrator")

class PipelineState:
    """Explicitly defined states for the Data Engineering Workflow."""
    IDLE = "IDLE"
    
    # Schema Analysis Phase
    ANALYZING_SCHEMA = "ANALYZING_SCHEMA"
    MAPPING_PENDING = "MAPPING_PENDING"  # Waiting for HITL column confirmation
    
    # Data Quality Phase
    ANALYZING_QUALITY = "ANALYZING_QUALITY"
    QUALITY_ISSUE_FOUND = "QUALITY_ISSUE_FOUND"  # Waiting for HITL fix decision
    
    # Classification Phase
    CLASSIFYING = "CLASSIFYING"
    
    # KPI Phase
    PLANNING_KPIS = "PLANNING_KPIS"
    
    # Completion Phase
    FINALIZING = "FINALIZING"
    SYNCING = "SYNCING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class OrchestrationState:
    """The 'Brain' of the pipeline session. Maintains history and current step."""
    def __init__(self):
        self.current_step = PipelineState.IDLE
        self.csv_path = None
        self.df = None
        self.user_metadata = {}
        self.pending_tasks = []
        self.classification = {}
        self.kpis = []
        self.history = []  # Internal chat history for the pipeline
        self.error = None
        self.health_report = {
            "auto_resolved": [], 
            "semantic_mappings": [],
            "data_cleaning": [],
            "dropped_columns": [],
            "discrepancies": []
        }

class OrchestrationAgent:
    """
    State-Based Router for the Data Pipeline.
    Acts as the Air Traffic Controller between the Preprocessor and the Human.
    """

    def __init__(self):
        self.loader = DataLoader()
        self.ext_engine = ExtensibilityEngine()
        self.output_dir = Path("data/processed")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # State persistence for Frontend (The 'Hard Drive')
        self.state_file = self.output_dir / "pipeline_state.json"

    def process(self, text: str, file_path: Optional[str], state: OrchestrationState) -> Tuple[str, OrchestrationState]:
        """Routes message to agents based on state.current_step."""
        
        logger.info(f"Routing | State={state.current_step} | Input={text[:30]}")
        
        # 1. Identity Check: New File Uploaded
        if file_path:
            logger.info(f"New file detected: {file_path}")
            state.csv_path = file_path
            state.df = pd.read_csv(file_path)
            state.current_step = PipelineState.ANALYZING_SCHEMA
            # Transition immediately to start analyzing
            return self.process("", None, state)

        # 2. Workflow Routing (The 'If-Else' Engine)
        
        # --- PHASE: SCHEMA MAPPING ---
        if state.current_step == PipelineState.ANALYZING_SCHEMA:
            logger.info("Routing to schema_mapper: analyze_schema")
            state.pending_tasks = schema_mapper.analyze_schema(state.df, state.user_metadata)
            return self._handle_task_queue(state, next_step=PipelineState.ANALYZING_QUALITY)

        elif state.current_step == PipelineState.MAPPING_PENDING:
            logger.info("Routing HITL response to data_cleaner: apply_cleaning_task")
            return self._resolve_pending_task(text, state, return_to=PipelineState.ANALYZING_SCHEMA)

        # --- PHASE: DATA QUALITY ---
        elif state.current_step == PipelineState.ANALYZING_QUALITY:
            logger.info("Routing to quality_analyzer: analyze_quality")
            state.pending_tasks = quality_analyzer.analyze_quality(state.df)
            return self._handle_task_queue(state, next_step=PipelineState.CLASSIFYING)

        elif state.current_step == PipelineState.QUALITY_ISSUE_FOUND:
            logger.info("Routing HITL quality fix to data_cleaner")
            return self._resolve_pending_task(text, state, return_to=PipelineState.ANALYZING_QUALITY)

        # --- PHASE: CLASSIFICATION ---
        elif state.current_step == PipelineState.CLASSIFYING:
            logger.info("Routing to column_classifier: classify_columns")
            state.classification = column_classifier.classify_columns(state.df, state.user_metadata)
            state.current_step = PipelineState.PLANNING_KPIS
            return self.process("", None, state)

        # --- PHASE: KPI PLANNING ---
        elif state.current_step == PipelineState.PLANNING_KPIS:
            logger.info("Routing to kpi_planner: plan_kpis")
            state.kpis = kpi_planner.plan_kpis(state.user_metadata)
            state.current_step = PipelineState.FINALIZING
            return self.process("", None, state)

        # --- PHASE: FINALIZATION ---
        elif state.current_step == PipelineState.FINALIZING:
            logger.info("Routing to data_cleaner: finalize_cleaning")
            state.df = data_cleaner.finalize_cleaning(state.df, state.classification, state.health_report, self.ext_engine)
            state.current_step = PipelineState.SYNCING
            return self.process("", None, state)

        elif state.current_step == PipelineState.SYNCING:
            logger.info("Routing to DataLoader: sync_dataframe_to_duckdb")
            table_name = f"processed_{Path(state.csv_path).stem}"
            self.loader.sync_dataframe_to_duckdb(state.df, table_name=table_name)
            state.current_step = PipelineState.COMPLETED
            
            # Compute KPIs for the report
            kpi_results = kpi_engine.compute_kpis(state.df, state.kpis)
            for r in kpi_results:
                r["insight"] = insight_engine.generate_insight(r["name"], r["value"])
            
            state.kpis = kpi_results
            return f"✅ Pipeline Complete! Data synced to '{table_name}'.", state

        elif state.current_step == PipelineState.COMPLETED:
            return "The data processing is already finished. Ready for new upload.", state

        # Fallback
        return "I'm in an unknown state. Please upload a file to reset.", state

    def _handle_task_queue(self, state: OrchestrationState, next_step: str) -> Tuple[str, OrchestrationState]:
        """Checks the agent's task queue. If empty, moves forward. If not, asks Human."""
        if not state.pending_tasks:
            state.current_step = next_step
            return self.process("", None, state)

        # Get the next task needing human input
        current_task = state.pending_tasks[0]
        
        # If low severity, auto-resolve and recurse
        if current_task.get("severity") == "low":
            state.df = data_cleaner.apply_cleaning_task(state.df, current_task, state.health_report)
            state.pending_tasks.pop(0)
            return self._handle_task_queue(state, next_step)

        # High/Medium Severity -> Ask Human
        if state.current_step == PipelineState.ANALYZING_SCHEMA:
            state.current_step = PipelineState.MAPPING_PENDING
        elif state.current_step == PipelineState.ANALYZING_QUALITY:
            state.current_step = PipelineState.QUALITY_ISSUE_FOUND

        self._persist_to_disk(state, current_task)
        return f"💡 **Attention Needed**: {current_task['message']}. Should I proceed? (yes/no/drop)", state

    def _normalize_input(self, user_input: str) -> str:
        """Standardizes user responses into internal action tokens."""
        user_input = user_input.strip().lower()

        if user_input in ["yes", "y", "yeah", "yep", "ok", "correct", "true", ""]:
            return "accepted"
        elif user_input in ["no", "n", "nope", "wrong", "false"]:
            return "rejected"
        elif user_input in ["drop", "remove", "delete"]:
            return "drop"
        else:
            # Handle custom feedback (e.g., "use column X instead")
            return f"custom:{user_input}"

    def _resolve_pending_task(self, user_input: str, state: OrchestrationState, return_to: str) -> Tuple[str, OrchestrationState]:
        """Applies human decision to the current task and resumes analysis."""
        decision = self._normalize_input(user_input)
        
        if state.pending_tasks:
            task = state.pending_tasks.pop(0)
            logger.info(f"Resolving Task: {task['type']} with Decision: {decision}")
            state.df = data_cleaner.apply_cleaning_task(state.df, task, state.health_report, decision)
        
        # Return to analysis to see if more issues exist
        state.current_step = return_to
        return self.process("", None, state)

    def _persist_to_disk(self, state: OrchestrationState, task: Optional[Dict] = None):
        """Updates pipeline_state.json for the Frontend."""
        payload = {
            "current_step": state.current_step,
            "csv_path": state.csv_path,
            "pending_task": task,
            "timestamp": pd.Timestamp.now().isoformat()
        }
        with open(self.state_file, "w") as f:
            json.dump(payload, f, indent=4)

    def run_pipeline(self, csv_path: str, user_metadata: Dict[str, str]):
        """CLI Loop for testing the state machine."""
        state = OrchestrationState()
        state.user_metadata = user_metadata
        
        reply, state = self.process("", csv_path, state)
        while state.current_step in [PipelineState.MAPPING_PENDING, PipelineState.QUALITY_ISSUE_FOUND]:
            print(f"\n{reply}")
            user_input = input("Decision: ")
            reply, state = self.process(user_input, None, state)
        
        print(f"\nResult: {reply}")

if __name__ == "__main__":
    orchestrator = OrchestrationAgent()
