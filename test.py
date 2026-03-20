import duckdb
from config import DB_PATH
conn = duckdb.connect(DB_PATH)
print(conn.execute("SELECT * FROM raw_user LIMIT 2").df().to_string())
print(conn.execute("SELECT * FROM raw_channel_duration LIMIT 2").df().to_string())
conn.close()