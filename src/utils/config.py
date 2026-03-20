import os
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("DB_PATH", "./database/frammer.db")
DATA_PATH = os.getenv("DATA_PATH", "./data/raw/")
SCHEMA_PATH = os.getenv("SCHEMA_PATH", "./llm_schema.txt")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")