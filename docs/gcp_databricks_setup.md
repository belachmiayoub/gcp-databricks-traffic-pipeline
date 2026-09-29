# GCP and Databricks Setup

This document describes the infrastructure expected by the notebooks in this repository.

## 1. GCP project and bucket

For a first implementation, start with the `dev` environment.

Create a GCP project and a GCS bucket in the same region as the Databricks workspace/metastore.

Recommended bucket layout:

```text
landing/
  raw_traffic/
  raw_roads/
checkpoints/
medallion/
  bronze/
  silver/
  gold/
```

Do not commit service-account JSON keys or credentials to this repository.

## 2. Unity Catalog

Create or reuse a Unity Catalog metastore and attach the Databricks workspace to it.

Create a Storage Credential that allows Databricks to access the GCS bucket.

Create these External Locations for DEV:

- `landing_dev` -> `gs://<bucket>/landing`
- `checkpoints_dev` -> `gs://<bucket>/checkpoints`
- `bronze_dev` -> `gs://<bucket>/medallion/bronze`
- `silver_dev` -> `gs://<bucket>/medallion/silver`
- `gold_dev` -> `gs://<bucket>/medallion/gold`

The ingestion notebooks resolve `landing_<env>` and `checkpoints_<env>` dynamically.

## 3. Expected input files

### Traffic CSV

Expected columns:

```text
Record_ID
Count_point_id
Direction_of_travel
Year
Count_date
hour
Region_id
Region_name
Local_authority_name
Road_name
Road_Category_ID
Start_junction_road_name
End_junction_road_name
Latitude
Longitude
Link_length_km
Pedal_cycles
Two_wheeled_motor_vehicles
Cars_and_taxis
Buses_and_coaches
LGV_Type
HGV_Type
EV_Car
EV_Bike
```

Upload traffic CSV files to:

```text
gs://<bucket>/landing/raw_traffic/
```

### Roads CSV

Expected columns:

```text
Road_ID
Road_Category_Id
Road_Category
Region_ID
Region_Name
Total_Link_Length_Km
Total_Link_Length_Miles
All_Motor_Vehicles
```

Upload roads CSV files to:

```text
gs://<bucket>/landing/raw_roads/
```

## 4. Run order

Run the tasks in this order:

1. `notebooks/01_setup/setup_catalog.py`
2. `notebooks/02_bronze/ingest_traffic.py`
3. `notebooks/02_bronze/ingest_roads.py`
4. `notebooks/03_silver/transform_traffic.py`
5. `notebooks/03_silver/transform_roads.py`
6. `notebooks/04_gold/gold_transformations.sql`
7. `notebooks/05_validation/validate_pipeline.py`

Each notebook accepts an `env` parameter. Start with `dev`.

## 5. Multi-environment promotion

Once DEV works, reproduce the infrastructure for UAT and PRD with:

- `uat_catalog`, `prd_catalog`
- `landing_uat`, `landing_prd`
- `checkpoints_uat`, `checkpoints_prd`
- corresponding Bronze/Silver/Gold External Locations.

The code itself stays environment-independent; only the `env` parameter changes.
