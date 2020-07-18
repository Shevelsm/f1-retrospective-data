import logging
import os
import shutil
import sys

from pyspark import SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col
from py4j.protocol import Py4JJavaError


logging.basicConfig(level=logging.INFO)

logging.info(" Creating Spark configuration...")

conf = SparkConf()
conf.set("spark.jars", "mysql-connector-java-8.0.20.jar")

spark = (
    SparkSession.builder.config(conf=conf)
    .master("local")
    .appName("ETL_F1_data")
    .getOrCreate()
)

sc = spark.sparkContext
sqlContext = SQLContext(sc)


logging.info(" Collecting raw data from MySQL to the DataFrames...")
try:
    # circuits
    logging.debug("creating circuits dataframe")
    circuits_df = (
        sqlContext.read.format("jdbc")
        .options(
            url="jdbc:mysql://mysql:3306/raw_data",
            driver="com.mysql.jdbc.Driver",
            dbtable="circuits",
            user="root",
            password="mysecretpassword",
        )
        .load()
    )
    ci = circuits_df.alias("cir")

    # constructors
    logging.debug("creating constructors dataframe")
    constructors_df = (
        sqlContext.read.format("jdbc")
        .options(
            url="jdbc:mysql://mysql:3306/raw_data",
            driver="com.mysql.jdbc.Driver",
            dbtable="constructors",
            user="root",
            password="mysecretpassword",
        )
        .load()
    )
    co = constructors_df.alias("co")

    # drivers
    logging.debug("creating drivers dataframe")
    drivers_df = (
        sqlContext.read.format("jdbc")
        .options(
            url="jdbc:mysql://mysql:3306/raw_data",
            driver="com.mysql.jdbc.Driver",
            dbtable="drivers",
            user="root",
            password="mysecretpassword",
        )
        .load()
    )
    dr = drivers_df.alias("dr")

    # lapTimes
    logging.debug("creating lapTimes dataframe")
    lapTimes_df = (
        sqlContext.read.format("jdbc")
        .options(
            url="jdbc:mysql://mysql:3306/raw_data",
            driver="com.mysql.jdbc.Driver",
            dbtable="lapTimes",
            user="root",
            password="mysecretpassword",
        )
        .load()
    )
    lT = lapTimes_df.alias("lT")

    # races
    logging.debug("creating races dataframe")
    races_df = (
        sqlContext.read.format("jdbc")
        .options(
            url="jdbc:mysql://mysql:3306/raw_data",
            driver="com.mysql.jdbc.Driver",
            dbtable="races",
            user="root",
            password="mysecretpassword",
        )
        .load()
    )
    ra = races_df.alias("ra")

    # results
    logging.debug("creating results dataframe")
    results_df = (
        sqlContext.read.format("jdbc")
        .options(
            url="jdbc:mysql://mysql:3306/raw_data",
            driver="com.mysql.jdbc.Driver",
            dbtable="results",
            user="root",
            password="mysecretpassword",
        )
        .load()
    )
    re = results_df.alias("re")

    # status
    logging.debug("creating status dataframe")
    status_df = (
        sqlContext.read.format("jdbc")
        .options(
            url="jdbc:mysql://mysql:3306/raw_data",
            driver="com.mysql.jdbc.Driver",
            dbtable="status",
            user="root",
            password="mysecretpassword",
        )
        .load()
    )
    st = status_df.alias("st")
except Py4JJavaError:
    logging.warning(" FAILED! Check mysql container and tables in raw_data!")
    sys.exit(1)


# Transform data and save data
logging.info(" Transforming raw data to a new dataframes...")

shutil.rmtree("output/*", ignore_errors=True)

gp_results_df = (
    re.join(dr, re.driverId == dr.driverId, how="inner")
    .join(ra, re.raceId == ra.raceId, how="inner")
    .join(co, re.constructorId == co.constructorId, how="inner")
    .join(st, re.statusId == st.statusId, how="inner")
    .where(col("position").isNotNull())
    .select(
        ra.year.alias("season"),
        ra.name.alias("grand_prix"),
        re.position.alias("pos_num"),
        re.points,
        dr.driverRef.alias("driver"),
        dr.nationality.alias("driver_nation"),
        co.constructorRef.alias("constructor"),
        co.nationality.alias("constructor_nation"),
        st.status,
    )
)
gr = gp_results_df.alias("gr")
logging.info(" gp_results dataframe has been created!")

circutis_laps_df = (
    lT.join(dr, lT.driverId == dr.driverId, how="inner")
    .join(ra, lT.raceId == ra.raceId, how="inner")
    .join(ci, ra.circuitId == ci.circuitId, how="inner")
    .where(col("milliseconds").isNotNull())
    .select(
        ci.circuitRef.alias("circuit"),
        ra.year.alias("season"),
        dr.driverRef.alias("driver"),
        lT.time.alias("time_str"),
        lT.milliseconds.alias("time_ms"),
    )
)
cl = circutis_laps_df.alias("cl")
logging.info(" circutis_laps dataframe has been created!")

# Write data to parquets files
logging.info(" Writing obtained dataframes to parquet files...")
gr.write.parquet("output/gp_results.parquet")
cl.write.parquet("output/circuits_laps.parquet")

logging.info(" The job has been successfully finished!")
