import duckdb
import os
from config import DB_PATH,DATA_PATH

conn = duckdb.connect(DB_PATH)
print("Connected to database successfully.")    

files = {

    'raw_video_list': 
        f'{DATA_PATH}video_list_data_obfuscated.csv',

    'raw_channel_summary': 
        f'{DATA_PATH}CLIENT 1 combined_data(2025-3-1-2026-2-28).csv',

    'raw_channel_user': 
        f'{DATA_PATH}combined_data(2025-3-1-2026-2-28) by channel and user.csv',

    'raw_input_type': 
        f'{DATA_PATH}combined_data(2025-3-1-2026-2-28) by input type.csv',

    'raw_output_type': 
        f'{DATA_PATH}combined_data(2025-3-1-2026-2-28) by output type.csv',

    'raw_language': 
        f'{DATA_PATH}combined_data(2025-3-1-2026-2-28) by language.csv',

    'raw_channel_platform': 
        f'{DATA_PATH}channel-wise-publishing.csv',

    'raw_monthly_count': 
        f'{DATA_PATH}monthly-chart.csv',

    'raw_monthly_duration': 
        f'{DATA_PATH}month-wise-duration.csv'
}

print("File paths defined successfully.")

for table_name,file_path in files.items():
    if not os.path.exists(file_path):
        print(f"missing :{file_path}")
        continue
    conn.execute(f"""
                 CREATE OR REPLACE TABLE {table_name} AS
                 SELECT * FROM read_csv_auto(
                 "{file_path}",
                 header=true,
                 ignore_errors=true)
                 """)

    count = conn.execute(
            f"SELECT COUNT(*) FROM {table_name}"
        ).fetchone()[0]

    print(f"OK  {table_name:30s} {count:>6} rows")  
print("Data loaded into DuckDB successfully.")
tables=conn.execute("show tables").df()
print(tables.to_string(index=False))
conn.close()
print("Database connection closed.")