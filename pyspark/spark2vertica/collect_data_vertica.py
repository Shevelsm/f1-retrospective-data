import vertica_python


conn_info = {
    "host": "vertica",
    "user": "dbadmin",
    "password": "",
    "database": "docker",
    "use_prepared_statements": True,
}

with vertica_python.connect(**conn_info) as connection:
    cur = connection.cursor()
    cur.execute("TRUNCATE TABLE circuits_laps;")
    cur.execute("TRUNCATE TABLE gp_results;")
    cur.execute("COPY circuits_laps FROM '/tmp/input/circuits_laps.parquet/*' PARQUET;")
    cur.execute("COPY gp_results FROM '/tmp/input/gp_results.parquet/*' PARQUET;")

