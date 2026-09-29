# Databricks notebook source
# Incremental ingestion of raw roads CSV files from GCS using Auto Loader.

from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

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

roads_schema = StructType([
    StructField("Road_ID", IntegerType(), True),
    StructField("Road_Category_Id", IntegerType(), True),
    StructField("Road_Category", StringType(), True),
    StructField("Region_ID", IntegerType(), True),
    StructField("Region_Name", StringType(), True),
    StructField("Total_Link_Length_Km", DoubleType(), True),
    StructField("Total_Link_Length_Miles", DoubleType(), True),
    StructField("All_Motor_Vehicles", DoubleType(), True),
])

roads_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{checkpoint}/bronze/raw_roads/schema")
    .option("header", "true")
    .schema(roads_schema)
    .load(f"{landing}/raw_roads/")
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
)

query = (
    roads_stream.writeStream
    .format("delta")
    .option("checkpointLocation", f"{checkpoint}/bronze/raw_roads/checkpoint")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(f"{catalog}.bronze.raw_roads")
)

query.awaitTermination()
print(f"Bronze ingestion complete: {catalog}.bronze.raw_roads")
