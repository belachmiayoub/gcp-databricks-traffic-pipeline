# Databricks notebook source
# Incremental ingestion of raw traffic CSV files from GCS using Auto Loader.

from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

dbutils.widgets.text("env", "dev", "Environment")
env = dbutils.widgets.get("env").strip().lower()
if env not in {"dev", "uat", "prd"}:
    raise ValueError("env must be one of: dev, uat, prd")

catalog = f"{env}_catalog"

def external_location_url(name: str) -> str:
    rows = spark.sql(f"DESCRIBE EXTERNAL LOCATION `{name}_{env}`").select("url").collect()
    if not rows:
        raise RuntimeError(f"External location {name}_{env} was not found")
    return rows[0][0].rstrip("/")

landing = external_location_url("landing")
checkpoint = external_location_url("checkpoints")

traffic_schema = StructType([
    StructField("Record_ID", IntegerType(), True),
    StructField("Count_point_id", IntegerType(), True),
    StructField("Direction_of_travel", StringType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Count_date", StringType(), True),
    StructField("hour", IntegerType(), True),
    StructField("Region_id", IntegerType(), True),
    StructField("Region_name", StringType(), True),
    StructField("Local_authority_name", StringType(), True),
    StructField("Road_name", StringType(), True),
    StructField("Road_Category_ID", IntegerType(), True),
    StructField("Start_junction_road_name", StringType(), True),
    StructField("End_junction_road_name", StringType(), True),
    StructField("Latitude", DoubleType(), True),
    StructField("Longitude", DoubleType(), True),
    StructField("Link_length_km", DoubleType(), True),
    StructField("Pedal_cycles", IntegerType(), True),
    StructField("Two_wheeled_motor_vehicles", IntegerType(), True),
    StructField("Cars_and_taxis", IntegerType(), True),
    StructField("Buses_and_coaches", IntegerType(), True),
    StructField("LGV_Type", IntegerType(), True),
    StructField("HGV_Type", IntegerType(), True),
    StructField("EV_Car", IntegerType(), True),
    StructField("EV_Bike", IntegerType(), True),
])

traffic_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{checkpoint}/bronze/raw_traffic/schema")
    .option("header", "true")
    .schema(traffic_schema)
    .load(f"{landing}/raw_traffic/")
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
)

query = (
    traffic_stream.writeStream
    .format("delta")
    .option("checkpointLocation", f"{checkpoint}/bronze/raw_traffic/checkpoint")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(f"{catalog}.bronze.raw_traffic")
)

query.awaitTermination()
print(f"Bronze ingestion complete: {catalog}.bronze.raw_traffic")
