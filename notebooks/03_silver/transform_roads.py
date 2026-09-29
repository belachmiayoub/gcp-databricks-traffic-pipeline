# Databricks notebook source
# Cleans and standardizes Bronze roads data, then writes the Silver Delta table.

from pyspark.sql import functions as F

dbutils.widgets.text("env", "dev", "Environment")
env = dbutils.widgets.get("env").strip().lower()
if env not in {"dev", "uat", "prd"}:
    raise ValueError("env must be one of: dev, uat, prd")

catalog = f"{env}_catalog"

source = spark.table(f"{catalog}.bronze.raw_roads")

df = source
for old_name in source.columns:
    new_name = old_name.strip().lower()
    df = df.withColumnRenamed(old_name, new_name)

df = (
    df
    .dropDuplicates(["road_id"])
    .fillna(
        {
            "road_category": "Unknown",
            "region_name": "Unknown",
            "total_link_length_km": 0.0,
            "total_link_length_miles": 0.0,
            "all_motor_vehicles": 0.0,
        }
    )
    .withColumn("_transformed_at", F.current_timestamp())
)

(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.silver.roads")
)

print(f"Silver transformation complete: {catalog}.silver.roads")
