import pandas as pd
from typing import List, Dict, Any

MISSING_VALUE_DROP_THRESHOLD = 0.50

def analyze_quality(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Detects missing values and quality gaps."""
    tasks = []
    for col in df.columns:
        missing_ratio = df[col].isna().sum() / len(df)
        if missing_ratio == 0: continue
            
        if missing_ratio > MISSING_VALUE_DROP_THRESHOLD:
            tasks.append({
                "type": "HIGH_MISSING_VALUES",
                "severity": "medium",
                "message": f"Column '{col}' is {missing_ratio:.1%} empty. Drop it?",
                "data": {"col": col, "missing_ratio": missing_ratio}
            })
        else:
            tasks.append({
                "type": "IMPUTE_MISSING",
                "severity": "low",
                "message": f"Imputing {missing_ratio:.1%} missing values in '{col}'",
                "data": {"col": col, "missing_ratio": missing_ratio}
            })
    return tasks
