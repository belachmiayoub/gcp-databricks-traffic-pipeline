-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.text("env", "dev", "Environment")
-- MAGIC env = dbutils.widgets.get("env").strip().lower()
-- MAGIC if env not in {"dev", "uat", "prd"}:
-- MAGIC     raise ValueError("env must be one of: dev, uat, prd")
-- MAGIC spark.sql(f"USE CATALOG {env}_catalog")

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.traffic_volume_by_year AS
SELECT
    year,
    SUM(total_traffic_count) AS total_traffic,
    SUM(motor_vehicles_count) AS total_motor_vehicles,
    SUM(cars_and_taxis) AS total_cars_and_taxis,
    SUM(pedal_cycles) AS total_cycles,
    SUM(electric_vehicles_count) AS total_electric_vehicles
FROM silver.traffic
GROUP BY year
ORDER BY year;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.traffic_by_region AS
SELECT
    region_id,
    region_name,
    local_authority_name,
    SUM(total_traffic_count) AS total_traffic,
    SUM(cars_and_taxis) AS cars_and_taxis,
    SUM(lgv_type) AS lgv,
    SUM(hgv_type) AS hgv,
    SUM(buses_and_coaches) AS buses_and_coaches,
    SUM(pedal_cycles) AS pedal_cycles,
    SUM(electric_vehicles_count) AS electric_vehicles
FROM silver.traffic
GROUP BY region_id, region_name, local_authority_name;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.road_usage_statistics AS
SELECT
    road_name,
    road_category_id,
    COUNT(*) AS observation_count,
    SUM(motor_vehicles_count) AS total_motor_vehicles,
    SUM(pedal_cycles) AS total_cycles,
    SUM(total_traffic_count) AS total_traffic,
    AVG(total_traffic_count) AS avg_traffic_per_observation
FROM silver.traffic
GROUP BY road_name, road_category_id;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.ev_adoption_trend AS
SELECT
    year,
    SUM(ev_car) AS total_ev_cars,
    SUM(ev_bike) AS total_ev_bikes,
    SUM(electric_vehicles_count) AS total_ev,
    SUM(motor_vehicles_count) AS total_motor_vehicles,
    CASE
        WHEN SUM(motor_vehicles_count) = 0 THEN 0
        ELSE SUM(electric_vehicles_count) / SUM(motor_vehicles_count)
    END AS ev_share
FROM silver.traffic
GROUP BY year
ORDER BY year;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.road_network_summary AS
SELECT
    region_id,
    region_name,
    road_category_id,
    road_category,
    COUNT(DISTINCT road_id) AS road_count,
    SUM(total_link_length_km) AS total_link_length_km,
    SUM(all_motor_vehicles) AS all_motor_vehicles
FROM silver.roads
GROUP BY region_id, region_name, road_category_id, road_category;
