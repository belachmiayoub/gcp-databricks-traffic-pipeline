# GCP Databricks Traffic Data Pipeline

End-to-end Data Engineering project built on Google Cloud Platform and Databricks.

## Architecture

GCS → Databricks Auto Loader → Bronze → Silver → Gold

## Technologies

- Google Cloud Storage
- Databricks
- PySpark
- Spark SQL
- Delta Lake
- Unity Catalog
- Auto Loader
- Databricks Workflows
- Git / GitHub

## Project Structure

- `notebooks/01_setup` – environment and catalog setup
- `notebooks/02_bronze` – raw data ingestion
- `notebooks/03_silver` – cleaning and transformations
- `notebooks/04_gold` – analytical models
- `sql/` – SQL transformations and data quality checks
- `tests/` – validation and test assets
- `architecture/` – architecture diagrams
- `workflows/` – orchestration assets

## Status

🚧 Project under development
