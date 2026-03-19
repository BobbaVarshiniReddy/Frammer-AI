import duckdb

def run_sql(df, query):
    con = duckdb.connect()
    con.register("df", df)

    result = con.execute(query).fetchdf()

    return result.to_dict(orient="records")