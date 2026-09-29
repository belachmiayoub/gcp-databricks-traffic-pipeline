-- Example analytics queries for the Gold layer.
-- USE CATALOG dev_catalog;

-- Traffic evolution
SELECT year, total_traffic, total_electric_vehicles
FROM gold.traffic_volume_by_year
ORDER BY year;

-- Highest-traffic regions
SELECT region_name, SUM(total_traffic) AS total_traffic
FROM gold.traffic_by_region
GROUP BY region_name
ORDER BY total_traffic DESC
LIMIT 10;

-- Most-used roads
SELECT road_name, total_traffic, avg_traffic_per_observation
FROM gold.road_usage_statistics
ORDER BY total_traffic DESC
LIMIT 20;

-- EV adoption
SELECT
    year,
    total_ev,
    total_motor_vehicles,
    ROUND(ev_share * 100, 2) AS ev_share_pct
FROM gold.ev_adoption_trend
ORDER BY year;

-- Road network by region/category
SELECT
    region_name,
    road_category,
    road_count,
    total_link_length_km
FROM gold.road_network_summary
ORDER BY total_link_length_km DESC;
