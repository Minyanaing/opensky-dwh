# de-pipeline

Implements the plan in `.claude/databricks-opensky-de-pipeline-project-plan.md`. **Currently implemented: Step 2, ingestion only** — it fetches OpenSky flight-movement data and writes raw JSON to disk. Loading into Databricks (Unity Catalog, dbt, orchestration) comes in a later step.

## Ingestion setup (local)

1. **Register an OpenSky OAuth2 client** at [opensky-network.org/my-opensky](https://opensky-network.org/my-opensky) → API Client, to get the standard 4,000-credits/day tier (anonymous access is not enough for this pipeline's scope).

2. **Create a virtual environment and install dependencies:**

   ```
   cd de-pipeline/ingestion
   python -m venv .venv
   .venv\Scripts\activate      # Windows
   pip install -r requirements.txt
   ```

3. **Configure credentials:**

   ```
   copy .env.example .env
   ```

   Then fill in `OPENSKY_CLIENT_ID` and `OPENSKY_CLIENT_SECRET` in `.env`.

## Running it locally

From `de-pipeline/ingestion`, with the venv active:

```
# Quick smoke test - one airport, last 1 day
python ingest_opensky.py --airports WSSS --lookback-days 1

# Full run - all 85 curated airports, default 3-day window
python ingest_opensky.py
```

Each run writes one JSON file to `data/flights_raw_<timestamp>.json` — the raw OpenSky `Flight` records (arrivals + departures) for every curated airport, tagged with `queried_airport`, `movement_type`, and `fetched_at`. The `data/` folder is gitignored.

## Notes

- The curated airport list (all 85 designated international airports across Southeast Asia) lives in `ingestion/config.py`.
- Ingestion is stateless: every run re-fetches the full trailing window rather than tracking a watermark, so a repeated or manual run is always safe to re-run.
- `--airports` accepts a comma-separated ICAO subset, for testing without pulling the full list.
