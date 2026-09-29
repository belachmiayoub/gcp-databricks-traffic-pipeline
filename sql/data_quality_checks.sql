-- Data quality checks to run after the pipeline.
-- In Databricks SQL, set the catalog before running, for example:
-- USE CATALOG dev_catalog;

-- 1) Bronze row counts
SELECT 'bronze.raw_traffic' AS table_name, COUNT(*) AS row_count FROM bronze.raw_traffic
UNION ALL
SELECT 'bronze.raw_roads', COUNT(*) FROM bronze.raw_roads;

-- 2) Silver uniqueness checks
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT record_id) AS distinct_record_ids,
    COUNT(*) - COUNT(DISTINCT record_id) AS duplicate_record_ids
FROM silver.traffic;

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT road_id) AS distinct_road_ids,
    COUNT(*) - COUNT(DISTINCT road_id) AS duplicate_road_ids
FROM silver.roads;

-- 3) Required-key null checks
SELECT
    SUM(CASE WHEN record_id IS NULL THEN 1 ELSE 0 END) AS null_record_id,
    SUM(CASE WHEN year IS NULL THEN 1 ELSE 0 END) AS null_year,
    SUM(CASE WHEN region_id IS NULL THEN 1 ELSE 0 END) AS null_region_id
FROM silver.traffic;

-- 4) Business-rule checks: vehicle counts should never be negative
SELECT COUNT(*) AS invalid_negative_vehicle_rows
FROM silver.traffic
WHERE pedal_cycles < 0
   OR two_wheeled_motor_vehicles < 0
   OR cars_and_taxis < 0
   OR buses_and_coaches < 0
   OR lgv_type < 0
   OR hgv_type < 0
   OR ev_car < 0
   OR ev_bike < 0;

-- 5) Reconciliation check for derived total
SELECT COUNT(*) AS inconsistent_total_rows
FROM silver.traffic
WHERE total_traffic_count <> motor_vehicles_count + pedal_cycles;

-- 6) Gold freshness / availability
SELECT 'traffic_volume_by_year' AS table_name, COUNT(*) AS row_count FROM gold.traffic_volume_by_year
UNION ALL
SELECT 'traffic_by_region', COUNT(*) FROM gold.traffic_by_region
UNION ALL
SELECT 'road_usage_statistics', COUNT(*) FROM gold.road_usage_statistics
UNION ALL
SELECT 'ev_adoption_trend', COUNT(*) FROM gold.ev_adoption_trend
UNION ALL
SELECT 'road_network_summary', COUNT(*) FROM gold.road_network_summary;
