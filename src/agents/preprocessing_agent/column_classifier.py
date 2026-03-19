import json
import logging
import pandas as pd
from typing import Dict
from langchain_core.prompts import ChatPromptTemplate
from .utils import get_llm, parse_json_safely

logger = logging.getLogger(__name__)

def classify_columns(df: pd.DataFrame, user_metadata: Dict[str, str], model_name: str = "gemini-2.0-flash") -> Dict[str, str]:
    """Classifies columns into functional categories using a professional LLM prompt."""
    llm = get_llm(model_name)
    sample = {col: df[col].dropna().head(3).tolist() for col in df.columns}
    prompt = ChatPromptTemplate.from_template("""
    You are a Data Engineering Expert. Classify the following columns into functional categories for a Star Schema.
    
    Categories:
    - ID: Unique identifiers (e.g., user_id, uuid, video_id).
    - Dimension: Categorical data used for grouping/filtering (e.g., country, channel_name, category, language).
    - Metric: Quantitative data that can be aggregated (e.g., views, price, errors, score).
    - Time: Temporal data including timestamps or durations (e.g., created_at, duration, timestamp).
    - Other: Metadata or text that doesn't fit the above.

    User Metadata (Intent): {user_metadata}
    Sample Data from File: {sample}

    Return ONLY a JSON object mapping each column name to its category.
    """)
    try:
        chain = prompt | llm
        response = chain.invoke({"user_metadata": json.dumps(user_metadata), "sample": json.dumps(sample)})
        return parse_json_safely(response.content)
    except Exception as e:
        logger.error(f"Classification failed: {e}")
        
        fallback = {}
        for col in df.columns:
            col_lower = col.lower()
            if pd.api.types.is_numeric_dtype(df[col]) or any(k in col_lower for k in ["view", "revenue", "price", "count", "score", "amount"]):
                fallback[col] = "Metric"
            elif any(k in col_lower for k in ["date", "time", "duration", "timestamp"]):
                fallback[col] = "Time"
            elif "id" in col_lower:
                fallback[col] = "ID"
            else:
                fallback[col] = "Dimension"
        return fallback
