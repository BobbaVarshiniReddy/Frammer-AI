"""
config.py  —  Centralised paths and API keys
=============================================
Uses __file__ to build absolute paths so this module
works correctly whether called from the project root
(via main.py) or from inside text_to_sql/ directly.
"""

import os

# Absolute path to the text_to_sql/ directory
_HERE = os.path.dirname(os.path.abspath(__file__))

# Absolute path to the project root (one level up from text_to_sql/)
_ROOT = os.path.dirname(_HERE)

# ── Paths ─────────────────────────────────────────────────────────────────────
DB_PATH     = os.path.join(_ROOT, "database", "frammer.db")
DATA_PATH   = os.path.join(_ROOT, "data") + os.sep
SCHEMA_PATH = os.path.join(_ROOT, "llm_schema.txt")

# ── Gemini ────────────────────────────────────────────────────────────────────
GEMINI_API_KEY = ""          # ← paste your key here
GEMINI_MODEL   = "gemini-2.5-flash"