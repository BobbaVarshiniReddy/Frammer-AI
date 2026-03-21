import os

_HERE    = os.path.dirname(os.path.abspath(__file__))  # backend/text_to_sql/
_BACKEND = os.path.dirname(_HERE)                       # backend/
_ROOT    = os.path.dirname(_BACKEND)                    # Frammer-AI/

DB_PATH     = os.path.join(_ROOT, "database", "frammer.db")
DATA_PATH   = os.path.join(_ROOT, "data") + os.sep
SCHEMA_PATH = os.path.join(_ROOT, "llm_schema.txt")

GEMINI_API_KEY = "AIzaSyAjiyr_XDiRbPsKX15h6XxKPSTgF-pUZKU"
GEMINI_MODEL   = "gemini-2.5-flash"