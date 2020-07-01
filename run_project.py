import logging
import os
import time
import shutil
from pathlib import Path


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(message)s", datefmt="%d-%m-%Y %H:%M:%S",
)

output_folder = Path("pyspark/spark2vertica/output").absolute()
shutil.rmtree(output_folder)
os.makedirs(output_folder)

logging.info("=============F1 Retrospective Data=============")

logging.info("Stage #1. Run pyspark job for transforming raw data")
os.system(
    "docker exec f1-retrospective-data_pyspark-notebook_1 python transfrom_data_pyspark.py"
)

logging.info("Stage #2. Run SQL DDL code for creating tables in Vertica")
os.system(
    "docker exec f1-retrospective-data_vertica_1 /opt/vertica/bin/vsql -U dbadmin -f /tmp/ddl/DDL_part.sql"
)

logging.info(" Waiting for transfering data to vertica container...")
os.system("docker restart f1-retrospective-data_vertica_1")
time.sleep(30)

logging.info("Stage #3. Read parquet files with tables")
os.system(
    "docker exec f1-retrospective-data_pyspark-notebook_1 python collect_data_vertica.py"
)

logging.info("Stage #4. Run Jupyter Notebook with analytic data visualization")
os.system("jupyter notebook F1-retrospective.ipynb")
