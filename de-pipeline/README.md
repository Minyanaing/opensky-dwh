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

Each run writes three files to `data/` (gitignored), **overwriting the previous run's output** (no timestamp in the filename):

| File | Contents |
|---|---|
| `flights_raw.json` | Raw OpenSky `Flight` records (arrivals + departures) for every curated airport, tagged with `queried_airport`, `movement_type`, `fetched_at` |
| `callsigns.csv` | Distinct, whitespace-stripped `callsign` values seen in that run |
| `airports.csv` | Distinct ICAO airport codes seen in that run — both ends of every flight, so this includes foreign counterpart airports outside the curated list, not just the 85 queried ones |

These three are meant to land in Databricks as separate Bronze objects (`flights_raw`, `callsigns`, `airports`) once the Databricks write path is built — noted here for that later step, not implemented yet.

### Manual test: one aircraft's flight history

`fetch_aircraft_history.py` is a standalone utility (not part of the scheduled ingestion) for inspecting `/flights/aircraft` — pick an `icao24` you saw in a previous output file:

```
python fetch_aircraft_history.py --icao24 7823bc --lookback-days 3
```

Writes `data/aircraft_flights_<icao24>_<timestamp>.json`.

### Manual test: airline / aircraft / callsign enrichment (adsbdb.com)

`ingest_adsbdb.py` enriches the callsigns and aircraft seen by `ingest_opensky.py` using the free, keyless [adsbdb.com](https://www.adsbdb.com/) API. Run it after `ingest_opensky.py` (it reads `data/callsigns.csv` and `data/flights_raw.json`):

```
python ingest_adsbdb.py
```

Writes three files to `data/`, overwritten each run (no timestamp):

| File | Contents |
|---|---|
| `adsbdb_callsigns.json` | Per-callsign route lookup (`/callsign/{callsign}`) — origin/destination airport + embedded airline, or `found: false` if adsbdb doesn't have it |
| `adsbdb_aircraft.json` | Per-`icao24` aircraft lookup (`/aircraft/{mode_s}`) — type, manufacturer, registration, owner |
| `adsbdb_airlines.json` | Per-airline lookup (`/airline/{icao}`), for ICAO airline codes derived from callsign prefixes (e.g. `CSZ` from `CSZ306`) — a deduped airline reference set, separate from the airline info embedded per-callsign above |

adsbdb is community-maintained, not authoritative — expect `found: false` for aircraft/callsigns it simply doesn't have (this is normal, not a bug). It has no airport-lookup endpoint, so `airports.csv` isn't used here; airport metadata is a separate concern (see Notes).

## Notes

- The curated airport list (all 85 designated international airports across Southeast Asia) lives in `ingestion/config.py`.
- Ingestion is stateless: every run re-fetches the full trailing window rather than tracking a watermark, so a repeated or manual run is always safe to re-run.
- `--airports` accepts a comma-separated ICAO subset, for testing without pulling the full list.
