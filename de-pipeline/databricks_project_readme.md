# de-pipeline

## 1. Databricks landing setup (one-time)

Creates `opensky_raw.bronze` (catalog + schema + tables + volume).

1. **Service principal:** Workspace → Settings → Identity and access → Service principals → Add. Note its **Application ID** (`DATABRICKS_CLIENT_ID`), generate an OAuth secret.
2. **Grants** (needs a metastore admin):
   ```sql
   GRANT CREATE CATALOG ON METASTORE TO `<application-id>`;
   ```
   Plus `CAN_USE` on the SQL Warehouse (Warehouse → Permissions tab, UI picker is fine there).
   > `PRINCIPAL_DOES_NOT_EXIST`? Use the Application ID (UUID), not the display name.
3. **Connection details:** any SQL Warehouse → Connection details → `Server hostname` (`DATABRICKS_HOST`) + `HTTP path` (`DATABRICKS_HTTP_PATH`).
4. **Test locally:**
   ```
   cd de-pipeline/databricks
   python -m venv .venv && .venv\Scripts\activate
   pip install -r requirements.txt
   copy .env.example .env
   ```
   Fill in `.env` (`DATABRICKS_HOST/HTTP_PATH/CLIENT_ID/CLIENT_SECRET`, or `DATABRICKS_TOKEN` for PAT fallback; optionally `ADMIN_PRINCIPAL` = your login email, so you can see the catalog too — see below). Then:
   ```
   cd setup
   python databricks_setup.py --sql-file setup.sql
   ```
   (`.env` still resolves correctly one directory up — `--sql-file` just needs to be run from inside `setup/`, since it's a plain relative path.)
   Verify in Catalog Explorer: `opensky_raw.bronze` with `flights_raw`/`callsigns`/`airlines`/`airports`/`aircrafts` + a `landing` volume. Safe to re-run (idempotent). `--sql-file` is required — pass `destroy.sql` instead to drop just the 5 tables (catalog/schema/volume untouched), or trigger `databricks-infra-destroy.yml` (`workflow_dispatch` only, requires typing `destroy` to confirm).

   > **Why `ADMIN_PRINCIPAL`:** the service principal *owns* what it creates, so your own admin login can't see `opensky_raw` unless it's the true metastore admin or explicitly granted. Set `ADMIN_PRINCIPAL` and `setup.sql`'s last statement grants you `ALL PRIVILEGES` + `MANAGE` automatically every run (`MANAGE` has to be listed separately — Unity Catalog's `ALL PRIVILEGES` deliberately excludes it).

5. **Test via CI/CD:** add the same secrets to GitHub (repo → Settings → Secrets → Actions), then run `databricks-infra-deploy.yml` (auto on push to `main` touching `de-pipeline/databricks/**`, or manual dispatch). If it fails on `CREATE CATALOG`, the metastore grant from step 2 didn't take.

6. **Auto-load Volume files into their tables (one-time):**
   ```
   cd de-pipeline/databricks/landing
   python landing.py
   ```
   Creates 5 Databricks Jobs (one per table), each with a file-arrival trigger scoped to that table's own Volume folder — landing a file in `callsigns/` only loads `callsigns`, never the other four. Re-run this any time a file under `landing/sql/` changes; it updates the existing Jobs in place rather than duplicating them.

---

## 2. Ingestion setup (local)

1. Register an OpenSky OAuth2 client at [opensky-network.org/my-opensky](https://opensky-network.org/my-opensky) (4,000 credits/day tier).
2. ```
   cd de-pipeline/ingestion
   python -m venv .venv && .venv\Scripts\activate
   pip install -r requirements.txt
   copy .env.example .env
   ```
3. Fill in `.env`: `OPENSKY_CLIENT_ID`/`SECRET` + the `DATABRICKS_*` values from Step 1 (needed for the upload step).

## 3. Running ingestion locally

Always local CSV first — nothing talks to Databricks except the upload step.

**Fetch from OpenSky:**
```
python ingest_opensky.py --airports WSSS --lookback-days 1   # quick test
python ingest_opensky.py                                     # full run, default lookback 1 day (yesterday)
```
Writes `flights_raw.csv` (full export), `callsigns.csv`/`airports.csv` (new-only since last run, tracked via `callsigns_old.csv`/`airports_old.csv`). Partial API-limit failures still export what was fetched.

**Enrich via adsbdb.com:**
```
python ingest_adsbdb.py              # callsigns + airlines + airports (default)
python ingest_adsbdb.py --aircraft   # aircraft only, run manually as needed
python ingest_adsbdb.py --limit 10   # cap items, for a quick test
```
See Section 4 for output files.

**Upload to the Databricks landing Volume:**
```
python load_to_databricks.py flights_raw airlines airports callsigns
```
Uploads named CSV(s) to `/Volumes/opensky_raw/bronze/landing/<folder>/`. Missing files are skipped with a warning. Loading a Volume file into its table (`COPY INTO`) happens automatically from here — a file-arrival-triggered Databricks Job picks it up within about a minute (see Section 1, step 6). No further action needed as long as that one-time Job setup has been run.

## 4. Manual-test utilities

**One aircraft's history:**
```
python fetch_aircraft_history.py --icao24 7823bc --lookback-days 3
```
Writes `data/aircraft_flights_<icao24>_<timestamp>.json`.

**adsbdb enrichment** (`ingest_adsbdb.py`, community data — `found=False` is normal, not a bug):

| File | Written by | Contents |
|---|---|---|
| `adsbdb_callsigns.csv` | default | route, airline, origin/destination airport (+ lat/lon) |
| `adsbdb_airlines.csv` | default | distinct airlines, derived from the callsigns above |
| `adsbdb_airports.csv` | default | distinct airports, derived from the callsigns above |
| `adsbdb_aircraft.csv` | `--aircraft` | type, manufacturer, registration, owner |

No separate airline/airport API calls — both are derived from the callsign route response. A `400` (invalid identifier) is logged and skipped like a `404`.

## 5. GitHub Actions (CI/CD)

| Workflow | Trigger | Status |
|---|---|---|
| `databricks-infra-deploy.yml` | push to `main` (`de-pipeline/databricks/**`), or manual | **Working** — Section 1 |
| `databricks-infra-destroy.yml` | manual only, requires typing `destroy` to confirm | **Working** — drops the 5 Bronze tables (see Section 1, step 4) |
| `de-ingest.yml` | manual / `repository_dispatch` | **Stale, doesn't work** — OpenSky blocks GitHub Actions IPs; also out of date vs. the current scripts. Kept for reference only. |

**Secrets:** `DATABRICKS_HOST`/`HTTP_PATH`/`CLIENT_ID`/`CLIENT_SECRET` (or `DATABRICKS_TOKEN`), `ADMIN_PRINCIPAL` (optional) — all for `databricks-infra-deploy.yml`/`databricks-infra-destroy.yml`. `OPENSKY_*` creds live in local `.env` only.

## 6. Running ingestion daily (local)

GitHub Actions can't reach OpenSky (IP-blocked) and Databricks compute can't either (network policy) — so daily ingestion runs on a machine you control.

**`run_daily.bat`** runs, in sequence, logging to `ingest_daily.log`:
1. `ingest_opensky.py`
2. `ingest_adsbdb.py` (default mode)
3. `load_to_databricks.py flights_raw airlines airports callsigns`

(`--aircraft` / `aircrafts` are manual, not part of this daily run.)

**Task Scheduler:** Create Basic Task → Trigger: Daily → Action: Start a program → path to `run_daily.bat` (leave "Start in" blank). Run once to test, check `ingest_daily.log`.

> With the default `lookback_days=1`, a missed day is **not** auto-backfilled — re-run manually with a wider `--lookback-days` to catch up.

## Notes

- Curated airport list (85 SEA international airports) lives in `ingestion/config.py`.
- `--airports` takes a comma-separated ICAO subset for a quick test; `ingest_adsbdb.py --limit N` is the equivalent for enrichment.
