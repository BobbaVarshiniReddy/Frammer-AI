import pandas as pd

def analyze_schema(df: pd.DataFrame):
    schema = {
        "dimensions": [],
        "metrics": []
    }

    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            schema["metrics"].append(col)
        else:
            schema["dimensions"].append(col)

    return schema