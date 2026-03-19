def compute_kpis(df, kpis):
    results = []

    # 🔥 Create safe column names
    col_map = {col: col.replace(" ", "_") for col in df.columns}

    safe_df = df.rename(columns=col_map)

    # Only numeric columns
    col_sums = {
        col_map[col]: safe_df[col_map[col]].sum()
        for col in df.columns
        if df[col].dtype != 'object'
    }

    for kpi in kpis:
        try:
            formula = kpi["formula"]

            # 🔥 Replace column names in formula
            for original, safe in col_map.items():
                formula = formula.replace(original, safe)

            print("Evaluating:", formula)  # DEBUG

            value = eval(formula, {}, col_sums)

            results.append({
            "name": kpi["name"],
            "formula": kpi["formula"].replace("_", " "),  # 🔥 clean
            "value": value,
            "why": kpi.get("reason", "")
            })

        except Exception as e:
            print("❌ KPI ERROR:", kpi["name"], "|", e)
            continue

    return results