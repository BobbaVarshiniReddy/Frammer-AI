import pandas as pd
import re
from typing import Dict, Any, Union, Optional, List
from src.data_engine.extensibility_engine import ExtensibilityEngine

def apply_cleaning_task(df: pd.DataFrame, task: Dict[str, Any], health_report: Dict[str, List[str]], user_decision: Optional[str] = None) -> pd.DataFrame:
    """Executes a specific cleaning operation based on a decision."""
    t_type = task["type"]
    data = task["data"]

    if t_type == "AUTO_MAP" or (t_type in ["SEMANTIC_MAP_SUGGESTION", "LOW_CONFIDENCE_MAPPING"] and user_decision == "accepted"):
        df.rename(columns={data["csv_col"]: data["user_col"]}, inplace=True)
        health_report["semantic_mappings"].append(f"{data['csv_col']} -> {data['user_col']}")

    elif t_type == "IMPUTE_MISSING":
        col = data["col"]
        fill_val = df[col].mean() if pd.api.types.is_numeric_dtype(df[col]) else "Unknown"
        df[col] = df[col].fillna(fill_val)
        health_report["auto_resolved"].append(f"Imputed {col}")

    elif t_type == "HIGH_MISSING_VALUES":
        if user_decision in ["drop", "accepted", None]:
            df.drop(columns=[data["col"]], inplace=True)
            health_report["dropped_columns"].append(data["col"])
    return df

def finalize_cleaning(df: pd.DataFrame, classification: Dict[str, str], health_report: Dict[str, List[str]], ext_engine: ExtensibilityEngine) -> pd.DataFrame:
    """Performs numeric normalization and time standardization."""
    for col in df.columns:
        cat = classification.get(col, "Other")
        if cat == "Metric" and df[col].dtype == 'object':
            df[col] = df[col].apply(_extract_numeric)
            health_report["data_cleaning"].append(f"Cleaned metric: {col}")
        elif cat == "Time":
            df[f"{col}_sec"] = df[col].apply(_convert_to_seconds)
            health_report["data_cleaning"].append(f"Standardized time: {col}")
    
    # Update Logic Registry (Dimensions only)
    for col, cat in classification.items():
        if cat == "Dimension":
            dim_name = f"dim_{col.lower().replace(' ', '_')}"
            if dim_name not in ext_engine.logic['existing_dimensions']:
                ext_engine.logic['existing_dimensions'][dim_name] = [col]
    ext_engine._save_logic()
    
    return df

def _extract_numeric(val: Any) -> Union[float, int, Any]:
    if pd.isna(val) or val == "": return val
    clean = str(val).replace(',', '').replace('$', '')
    match = re.search(r"([-+]?\d*\.\d+|\d+)", clean)
    if match:
        n = match.group(1)
        return float(n) if "." in n else int(n)
    return val

def _convert_to_seconds(val: Any) -> int:
    if pd.isna(val) or str(val).strip() == "": return 0
    s = str(val).lower().strip()
    if ":" in s:
        p = s.split(":")
        if len(p) == 3: return int(p[0])*3600 + int(p[1])*60 + int(p[2])
        if len(p) == 2: return int(p[0])*60 + int(p[1])
    m = re.search(r"(\d+)\s*(hour|hr|min|sec|s)", s)
    if m:
        n, u = int(m.group(1)), m.group(2)
        if u.startswith("h"): return n * 3600
        if u.startswith("m"): return n * 60
        return n
    return 0
