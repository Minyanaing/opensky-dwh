# opensky-dwh

Flight-movement data warehouse built on live [OpenSky Network](https://opensky-network.org) data for Southeast Asia airports. Ingests locally or via a Databricks notebook, lands into a bronze/silver/gold medallion pipeline (Databricks, plus a Snowflake counterpart), deployed across `dev`/`qa`/`prod` via GitHub Actions.

## Structure

| Path | Contents |
|---|---|
| `de-pipeline/ingestion/` | OpenSky + enrichment API ingestion, runnable locally (`run_daily.bat`) or on Databricks (notebook) |
| `de-pipeline/databricks/` | Bronze/silver/gold pipeline: setup, landing, per-env catalogs, transforms |
| `de-pipeline/snowflake/` | Snowflake landing counterpart |
| `.github/workflows/` | CI/CD — deploy/destroy per layer, per environment |

## Docs

Full setup steps, module breakdown, data model, and workflow reference: **[de-pipeline/databricks_project_readme.md](de-pipeline/databricks_project_readme.md)**.
