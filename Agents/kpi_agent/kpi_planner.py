import google.generativeai as genai
import json
import os

genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))

model = genai.GenerativeModel("gemini-3-flash-preview")


def plan_kpis(column_descriptions: dict):
    prompt = f"""
Act as a Senior Data Engineer. 
Analyze the following dataset schema and return a list of business KPIs.

### SCHEMA:
{json.dumps(column_descriptions, indent=2)}

### TASK:
1. Identify columns that can be aggregated (Numeric).
2. Identify columns that are dimensions (Categorical/Dates).
3. Create KPIs following this JSON structure.

### OUTPUT RULES:
- If no KPIs can be formed, return an empty list [].
- Use NULLIF(column, 0) in SQL to prevent division by zero.
- Return valid JSON only.

### JSON TEMPLATE:
[
  {{
"name": "Publish Rate",
"formula": "Published Count / Created Count",
"reason": "Measures how many created outputs are published"
}}
]

Response:
"""

    response = model.generate_content(prompt)
    raw = response.text.strip()

    print("\n🔥 GEMINI RAW OUTPUT:\n", raw)  # DEBUG

    try:
        # Remove ``` blocks
        if "```" in raw:
            raw = raw.split("```")[1]

        raw = raw.replace("json", "").strip()

        return json.loads(raw)

    except Exception:
        print("⚠️ JSON parsing failed. Returning empty KPIs.")
        return []