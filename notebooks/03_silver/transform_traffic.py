# Databricks notebook source
# Cleans and enriches Bronze traffic data, then writes the Silver Delta table.

from pyspark.sql import functions as F

dbutils.widgets.text("env", "dev", "Environment")
env = dbutils.widgets.get("env").strip().lower()
if env not in {"dev", "uat", "prd"}:
    raise ValueError("env must be one of: dev, uat, prd")

catalog = f"{env}_catalog"

source = spark.table(f"{catalog}.bronze.raw_traffic")

# Normalize column names to snake_case/lowercase for easier SQL usage.
df = source
for old_name in source.columns:
    new_name = old_name.strip().lower()
    df = df.withColumnRenamed(old_name, new_name)

string_columns = [
    "direction_of_travel",
    "region_name",
    "local_authority_name",
    "road_name",
    "start_junction_road_name",
    "end_junction_road_name",
]

numeric_columns = [
    "pedal_cycles",
    "two_wheeled_motor_vehicles",
    "cars_and_taxis",
    "buses_and_coaches",
    "lgv_type",
    "hgv_type",
    "ev_car",
    "ev_bike",
]

df = (
    df
    .dropDuplicates(["record_id"])
    .fillna("Unknown", subset=string_columns)
    .fillna(0, subset=numeric_columns)
    .withColumn("count_date", F.to_date("count_date"))
    .withColumn(
        "electric_vehicles_count",
        F.col("ev_car") + F.col("ev_bike"),
    )
    .withColumn(
        "motor_vehicles_count",
        F.col("two_wheeled_motor_vehicles")
        + F.col("cars_and_taxis")
        + F.col("buses_and_coaches")
        + F.col("lgv_type")
        + F.col("hgv_type")
        + F.col("ev_car")
        + F.col("ev_bike"),
    )
    .withColumn(
        "total_traffic_count",
        F.col("motor_vehicles_count") + F.col("pedal_cycles"),
    )
    .withColumn("_transformed_at", F.current_timestamp())
)

(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.silver.traffic")
)

print(f"Silver transformation complete: {catalog}.silver.traffic")
