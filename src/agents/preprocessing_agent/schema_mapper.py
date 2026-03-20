import json
import logging
import pandas as pd
from typing import Dict, Any, List, Optional
from langchain_core.prompts import ChatPromptTemplate
from .utils import get_llm, parse_json_safely

logger = logging.getLogger(__name__)

# Decision Thresholds
CONFIDENCE_AUTO_RESOLVE = 0.85
CONFIDENCE_HITL_REQUIRED = 0.45

def analyze_schema(df: pd.DataFrame, user_columns: Dict[str, str], model_name: str = "gemini-2.0-flash") -> List[Dict[str, Any]]:
    """Identifies mapping discrepancies between user intent and CSV structure."""
    csv_cols = list(df.columns)
    mapping_results = _get_semantic_mapping(user_columns, csv_cols, model_name)
    
    tasks = []
    for user_col, info in mapping_results.items():
        csv_col = info.get("matched_csv_col")
        conf = info.get("confidence", 0)
        
        if not csv_col or csv_col not in csv_cols:
            tasks.append({
                "type": "COLUMN_NOT_FOUND",
                "severity": "high",
                "message": f"Required column '{user_col}' not found in file.",
                "data": {"user_col": user_col, "confidence": conf}
            })
            continue

        if conf >= CONFIDENCE_AUTO_RESOLVE:
            tasks.append({
                "type": "AUTO_MAP",
                "severity": "low",
                "message": f"Auto-mapping '{csv_col}' to '{user_col}'",
                "data": {"csv_col": csv_col, "user_col": user_col, "confidence": conf}
            })
        elif conf >= CONFIDENCE_HITL_REQUIRED:
            tasks.append({
                "type": "SEMANTIC_MAP_SUGGESTION",
                "severity": "medium",
                "message": f"Is '{csv_col}' the same as '{user_col}'? (Confidence: {conf}%)",
                "data": {"csv_col": csv_col, "user_col": user_col, "confidence": conf, "reason": info.get("reason")}
            })
        else:
            tasks.append({
                "type": "LOW_CONFIDENCE_MAPPING",
                "severity": "high",
                "message": f"Low confidence mapping for '{user_col}'. Manual verify needed.",
                "data": {"csv_col": csv_col, "user_col": user_col, "confidence": conf}
            })
    return tasks

def _get_semantic_mapping(user_cols: Dict[str, str], csv_cols: List[str], model_name: str) -> Dict[str, Any]:
    """Maps user intent to actual CSV columns using a specialized LLM prompt."""
    llm = get_llm(model_name)
    prompt = ChatPromptTemplate.from_template("""
    You are a Data Mapping Expert. Match the user's intended columns to the actual columns found in a CSV file.
    
    User Intent (Columns they want): {user_cols}
    Actual CSV Columns: {csv_cols}

    Instructions:
    1. For each user-intended column, find the most likely match in the CSV columns based on semantic similarity.
    2. Assign a confidence score from 0 to 100.
    3. Provide a brief reason for the match.
    4. If no reasonable match is found, set 'matched_csv_col' to null.
    
    Return ONLY a JSON object in this format:
    {{
        "user_intended_col_name": {{
            "matched_csv_col": "actual_csv_col_name",
            "confidence": score,
            "reason": "explanation"
        }}
    }}
    """)
    try:
        chain = prompt | llm
        res = chain.invoke({"user_cols": json.dumps(user_cols), "csv_cols": str(csv_cols)})
        return parse_json_safely(res.content)
    except Exception as e:
        logger.error(f"Semantic mapping failed: {e}")
        return {}
