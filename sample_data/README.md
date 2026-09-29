# Sample data

Raw datasets are intentionally not committed here.

The pipeline expects two CSV datasets:

- traffic observations in `landing/raw_traffic/`
- road reference data in `landing/raw_roads/`

See [GCP and Databricks Setup](../docs/gcp_databricks_setup.md) for the expected schemas.

Keeping raw data outside GitHub makes the repository smaller and avoids accidentally publishing restricted or licensed datasets.
