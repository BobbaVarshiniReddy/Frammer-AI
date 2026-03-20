def profile_data(df):
    return {
        "columns": df.columns.tolist(),
        "numerical": df.select_dtypes(include='number').columns.tolist(),
        "categorical": df.select_dtypes(include='object').columns.tolist(),
        "datetime": [col for col in df.columns if "date" in col.lower()],
        "row_count": len(df)
    }