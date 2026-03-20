import duckdb
import os
import pandas as pd
from typing import Optional
from src.utils.config import DB_PATH, DATA_PATH

class DataLoader:
    """
    Handles data ingestion into DuckDB.
    Supports both bulk CSV loading and dynamic DataFrame syncing.
    """
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        # Ensure database directory exists
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

    def _get_connection(self):
        return duckdb.connect(self.db_path)

    def sync_dataframe_to_duckdb(self, df: pd.DataFrame, table_name: str):
        """Creates or replaces a table in DuckDB with the given DataFrame."""
        conn = self._get_connection()
        try:
            conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
            count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"✅ Synced {table_name}: {count} rows")
        finally:
            conn.close()

    def load_initial_data(self):
        """Initial load of raw CSV files from data directory."""
        conn = self._get_connection()
        try:
            files = {
                'raw_video_list': f'{DATA_PATH}video_list_data_obfuscated.csv',
                'raw_channel_summary': f'{DATA_PATH}CLIENT 1 combined_data(2025-3-1-2026-2-28).csv',
                'raw_channel_user': f'{DATA_PATH}combined_data(2025-3-1-2026-2-28) by channel and user.csv',
                'raw_input_type': f'{DATA_PATH}combined_data(2025-3-1-2026-2-28) by input type.csv',
                'raw_output_type': f'{DATA_PATH}combined_data(2025-3-1-2026-2-28) by output type.csv',
                'raw_language': f'{DATA_PATH}combined_data(2025-3-1-2026-2-28) by language.csv',
                'raw_channel_platform': f'{DATA_PATH}channel-wise-publishing.csv',
                'raw_monthly_count': f'{DATA_PATH}monthly-chart.csv',
                'raw_monthly_duration': f'{DATA_PATH}month-wise-duration.csv',
            }

            print("Initial Data Loading...")
            for table_name, file_path in files.items():
                if not os.path.exists(file_path):
                    print(f"MISSING: {file_path}")
                    continue

                conn.execute(f"""
                    CREATE OR REPLACE TABLE {table_name} AS
                    SELECT * FROM read_csv_auto(
                        '{file_path}',
                        header=true,
                        ignore_errors=true
                    )
                """)
                count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                print(f"OK  {table_name:30s} {count:>6} rows")
        finally:
            conn.close()

if __name__ == "__main__":
    loader = DataLoader()
    loader.load_initial_data()
