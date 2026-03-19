def validate_plan(plan, df):
    valid_columns = set(df.columns)

    clean_plan = []

    for p in plan:
        if p.get("x") not in valid_columns:
            continue
        if p.get("y") not in valid_columns:
            continue
        if p.get("plot") not in ["line", "bar", "scatter", "histogram", "box", "pie"]:
            continue

        if "aggregation" not in p:
            p["aggregation"] = "sum"

        clean_plan.append(p)

    return clean_plan