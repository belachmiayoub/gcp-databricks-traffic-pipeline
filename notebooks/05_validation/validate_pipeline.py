# Databricks notebook source
# Fails the job when critical post-pipeline data quality rules are violated.

dbutils.widgets.text("env", "dev", "Environment")
env = dbutils.widgets.get("env").strip().lower()
if env not in {"dev", "uat", "prd"}:
    raise ValueError("env must be one of: dev, uat, prd")

catalog = f"{env}_catalog"

checks = {
    "duplicate traffic record_id": f"""
        SELECT COUNT(*) - COUNT(DISTINCT record_id)
        FROM {catalog}.silver.traffic
    """,
    "duplicate road_id": f"""
        SELECT COUNT(*) - COUNT(DISTINCT road_id)
        FROM {catalog}.silver.roads
    """,
    "negative vehicle counts": f"""
        SELECT COUNT(*)
        FROM {catalog}.silver.traffic
        WHERE pedal_cycles < 0
           OR two_wheeled_motor_vehicles < 0
           OR cars_and_taxis < 0
           OR buses_and_coaches < 0
           OR lgv_type < 0
           OR hgv_type < 0
           OR ev_car < 0
           OR ev_bike < 0
    """,
    "inconsistent total traffic": f"""
        SELECT COUNT(*)
        FROM {catalog}.silver.traffic
        WHERE total_traffic_count <> motor_vehicles_count + pedal_cycles
    """,
}

failures = []
for name, query in checks.items():
    value = spark.sql(query).first()[0]
    print(f"{name}: {value}")
    if value != 0:
        failures.append(f"{name}={value}")

required_gold_tables = [
    "traffic_volume_by_year",
    "traffic_by_region",
    "road_usage_statistics",
    "ev_adoption_trend",
    "road_network_summary",
]

for table in required_gold_tables:
    count = spark.table(f"{catalog}.gold.{table}").count()
    print(f"gold.{table}: {count} rows")
    if count == 0:
        failures.append(f"gold.{table}=empty")

if failures:
    raise RuntimeError("Data quality validation failed: " + "; ".join(failures))

print("All critical data quality checks passed.")
