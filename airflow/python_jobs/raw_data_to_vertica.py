from pyspark import SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col

conf = SparkConf()  # create the configuration
conf.set("spark.jars", "./*.jar")  # set the spark.jars

spark = (
    SparkSession.builder.config(conf=conf)
    .master("local")
    .appName("ETL_F1_data")
    .getOrCreate()
)

sc = spark.sparkContext
sqlContext = SQLContext(sc)


# Collecting raw data from mysql
# circuits
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


# Transform data
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

opts_gr = {
    "table": "gp_results",
    "db": "docker",
    "user": "dbadmin",
    "password": "",
    "host": "vertica",
}

gr.write.save(
    format="com.vertica.spark.datasource.DefaultSource", mode="overwrite", **opts_gr
)
