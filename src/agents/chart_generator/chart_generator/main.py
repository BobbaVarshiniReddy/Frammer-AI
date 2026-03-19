import pandas as pd
import google.generativeai as genai

from visualization_agent import visualization_agent

# Configure Gemini
genai.configure(api_key="GOOGLE_API_KEY")
model = genai.GenerativeModel("gemini-pro")

#csv and kpis will be received from kpi agent 
# Load CSV
df = pd.read_csv("your_file.csv")

# Example KPIs 
kpis = [
    {"name": "Total Sales", "metric": "sales"},
    {"name": "Profit", "metric": "profit"}
]

# User Query
user_query = "Show sales trend and compare across regions"

# Run agent
result = visualization_agent(df, kpis, user_query, model)

print(result["plan"])
print(result["code"])