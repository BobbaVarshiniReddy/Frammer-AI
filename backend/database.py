import duckdb
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "../database/frammer.db")

def get_connection():
    return duckdb.connect(DB_PATH)