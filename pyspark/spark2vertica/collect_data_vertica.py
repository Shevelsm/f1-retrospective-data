import logging
import sys

import vertica_python


logging.basicConfig(level=logging.INFO)


logging.info(" Collect pyspark output data for analysing it in Vertica")

conn_info = {
    "host": "vertica",
    "user": "dbadmin",
    "password": "",
    "database": "docker",
    "use_prepared_statements": True,
}

logging.info(" Connecting to Vertica...")
logging.info(" user={} database={}".format(conn_info["user"], conn_info["database"]))

try:
    with vertica_python.connect(**conn_info) as connection:
        cur = connection.cursor()
        cur.execute("TRUNCATE TABLE circuits_laps;")
        cur.execute("TRUNCATE TABLE gp_results;")
        cur.execute(
            "COPY circuits_laps FROM '/tmp/input/circuits_laps.parquet/*' PARQUET;"
        )
        cur.execute("COPY gp_results FROM '/tmp/input/gp_results.parquet/*' PARQUET;")
except vertica_python.errors.ConnectionError:
    logging.warning(" Failed to establish connection to Vertica...")
    sys.exit(1)

logging.info(" The job has been finished!")
