import json

def get_plot_plan_llm(kpis, schema, user_query, model):
    prompt = f"""
You are a data visualization expert.

KPIs:
{kpis}

Schema:
{schema}

User Query:
{user_query}

Return ONLY JSON:
[
  {{
    "kpi": "...",
    "plot": "line/bar/scatter/histogram/box/pie",
    "x": "...",
    "y": "...",
    "aggregation": "sum/mean/count"
  }}
]
"""

    response = model.generate_content(prompt)
    
    try:
        return json.loads(response.text)
    except:
        return []