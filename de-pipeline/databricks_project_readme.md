# de-pipeline

OpenSky flight-movement pipeline: local ingestion → Databricks bronze/silver/gold (medallion), deployed via GitHub Actions across dev/qa/prod.

---

## 1. Setup

### 1.1 Databricks setup

1. **Service principal**: Workspace → Settings → Identity and access → Service principals → Add. Note the **Application ID** (`DATABRICKS_CLIENT_ID`), generate an OAuth secret (`DATABRICKS_CLIENT_SECRET`). PAT (`DATABRICKS_TOKEN`) works as a fallback everywhere OAuth M2M is used.
2. **Grants** (needs a metastore admin): `GRANT CREATE CATALOG ON METASTORE TO <application-id>;` + `CAN_USE` on the SQL Warehouse.
3. **Connection details**: any SQL Warehouse → Connection details → `Server hostname` (`DATABRICKS_HOST`) + `HTTP path` (`DATABRICKS_HTTP_PATH`).
4. **Bronze/landing infra (one-time, env-independent)** — run locally or via CI (§4):
   ```
   cd de-pipeline/databricks/setup
   python databricks_setup.py --sql-file setup.sql
   ```
   Creates `opensky_raw.bronze` (7 tables + `landing` Volume).
5. **Landing → bronze auto-load (one-time)**:
   ```
   cd de-pipeline/databricks/landing
   python landing.py
   ```
   Creates 7 file-arrival-triggered Jobs (one per table).
6. **Per-environment catalogs (dev/qa/prod)** — run once per environment, or via CI (§4):
   ```
   cd de-pipeline/databricks/env_setup
   python env_setup.py --sql-file setup.sql --catalog dev_catalog
   python env_setup.py --sql-file tables_master.sql --catalog dev_catalog
   python env_setup.py --sql-file tables_silver.sql --catalog dev_catalog
   python env_setup.py --sql-file tables_gold.sql --catalog dev_catalog
   ```
   Repeat with `qa_catalog` / `prod_catalog`. Creates `{{CATALOG}}.silver_flights`, `.gold_flights`, `.master`.
7. **Silver/gold/master transforms + ingestion job** — deployed by CI only (§4); no manual step needed once secrets are set.

### 1.2 GitHub setup

1. **Branches**: create `main`, `main_qa`, `main_prod`.
2. **Repo secrets** (Settings → Secrets and variables → Actions → Repository secrets):
   - `ADMIN_PRINCIPAL`
   - `DATABRICKS_CLIENT_ID`
   - `DATABRICKS_CLIENT_SECRET`
   - `DATABRICKS_HOST`
   - `DATABRICKS_HTTP_PATH`
   - `OPENSKY_CLIENT_ID`
   - `OPENSKY_CLIENT_SECRET`
3. **Branch model**: `pr-target-check.yml` enforces PRs flow `main → main_qa → main_prod` only.
   - `main` → `dev_catalog`, 
   - `main_qa` → `qa_catalog`, 
   - `main_prod` → `prod_catalog`. 
4. Push to the right branch (or use `workflow_dispatch`) to trigger the matching deploy workflow — see §4.

### 1.3 Where things get copied

| What | Local path | Destination |
|---|---|---|
| Ingestion secrets | `de-pipeline/ingestion/.env` | Copy manually to Databricks Workspace `/Shared/opensky/ingestion/.env` (only needed if running ingestion notebook/job on Databricks) |
| Databricks/env_setup secrets | `de-pipeline/databricks/.env`, `de-pipeline/databricks/env_setup` uses same | Not uploaded anywhere — local/CI only |
| Ingestion code (for on-Databricks runs) | `de-pipeline/ingestion/{common,pipeline,loaders,databricks}/*` | `/Shared/opensky/ingestion/...` (via `deploy_opensky_ingestion.py`) |
| Silver/gold/master SQL | `de-pipeline/databricks/transformation/{silver,gold,master}/*.sql` | `/Shared/opensky/silver/<catalog>/...`, `/Shared/opensky/gold/<catalog>/...`, `/Shared/master/<catalog>/...` |
| Landing `COPY INTO` SQL | `de-pipeline/databricks/landing/sql/*.sql` | `/Shared/opensky/copy_into_<table>.sql` |

---

## 2. Ingestion (`de-pipeline/ingestion/`)

| Folder | Contents |
|---|---|
| `common/` | `config.py` (env/config), `fetch_data.py` (OpenSky OAuth + fetch), `transforms.py` (tagging/dedup helpers) |
| `pipeline/` | `ingest_opensky.py`, `export_airports.py`, `export_callsigns.py`, `ingest_adsbdb.py`, `ingest_airports.py` — the daily steps |
| `loaders/` | `load_to_databricks.py`, `load_to_snowflake.py` — upload CSVs to each platform's landing stage |
| `manual/` | `fetch_aircraft_history.py`, `ingest_json_csv.py` — ad hoc utilities, not scheduled |
| `databricks/` | `databricks_ingest.py` (orchestrator), `run_ingestion_notebook.py`, `deploy_opensky_ingestion.py`, `schedule_opensky_ingestion.py` — runs the pipeline *on* Databricks instead of locally |

**How data is ingested (current)**: OpenSky API → `flights_raw.csv` → extract distinct airports/callsigns (new-only delta) → enrich via adsbdb.com + airport-data.com → land the resulting CSVs in the Databricks Volume → file-arrival trigger runs `COPY INTO` into bronze tables. For now this runs **on Databricks**: `databricks/run_ingestion_notebook.py` is triggered to call the ingestion scripts step-by-step (via `databricks_ingest.py`'s `run_all()`), then lands the CSVs straight into the Volume — no separate upload step needed, since the notebook already runs inside Databricks.

**Sequence** (as called by the notebook / `databricks_ingest.py`):
```
1. ingest_opensky.py       -> flights_raw.csv
2. export_airports.py     -> airports.csv (new-only)
3. export_callsigns.py    -> callsigns.csv (new-only)
4. ingest_adsbdb.py        -> adsbdb_callsigns/airlines/airports.csv
5. ingest_airports.py      -> airport_data.csv
   -> land_to_volume()     -> Databricks landing Volume -> file-arrival trigger -> COPY INTO bronze tables
```
`ingest_adsbdb.py --aircraft` (aircraft enrichment) and the one-off `airports_master` load are manual, not part of this sequence.

**Local alternative**: `run_daily.bat` can run the same pipeline locally, plus upload steps for both platforms:
```
1-5. pipeline\ingest_opensky.py / export_airports.py / export_callsigns.py / ingest_adsbdb.py / ingest_airports.py
6.   loaders\load_to_databricks.py ...   -> Databricks landing Volume
7.   loaders\load_to_snowflake.py ...    -> Snowflake landing stage
```
Available to run via Windows Task Scheduler, but not how ingestion currently runs.

---

## 3. Databricks (`de-pipeline/databricks/`)

### Architecture (medallion)

```
opensky_raw.bronze          (setup/, one-time, env-independent)
       │  file-arrival trigger -> COPY INTO   (landing/)
       v
{{CATALOG}}.silver_flights  (transformation/silver/, per dev/qa/prod)
       │  MERGE/INSERT
       v
{{CATALOG}}.gold_flights    (transformation/gold/, per dev/qa/prod)

{{CATALOG}}.master          (transformation/master/, date/time dims, project-agnostic)
```

### Modules

| Folder | Purpose |
|---|---|
| `setup/` | One-time bronze infra: `opensky_raw` catalog/schema/Volume + 7 raw tables |
| `landing/` | Per-table file-arrival-triggered Jobs, bronze `COPY INTO` |
| `env_setup/` | Per-environment (`dev`/`qa`/`prod`) catalog + silver/gold/master schema DDL, plus `destroy_*.sql` |
| `transformation/` | Deploy scripts + SQL for silver, gold, master transforms |

### Data model

| Layer | Tables |
|---|---|
| Bronze | `flights_raw`, `callsigns`, `airlines`, `airports`, `aircrafts`, `airport_data`, `airports_master` |
| Silver | `flights`, `airlines`, `aircrafts`, `callsigns`, `airports` |
| Gold | `fct_flight_movement`, `dim_airline`, `dim_aircraft`, `dim_airport`, `dim_callsign` |
| Master | `date`, `time` |

### Transformation logic

- **Silver**: content-hash keyed (`md5` of business columns) — a changed value inserts a new row instead of overwriting, so silver keeps full history. `flights` is the one exception (updates departure/arrival in place).
- **Gold dimensions**: SCD-2 (`effective_start`/`effective_end`/`is_current`, `9999-12-12` sentinel for open rows). Each run only processes silver's latest batch (or full history if the dim is empty) — silver's content-hash dedup guarantees that's always genuinely new/changed data. Change detection recomputes each dimension's own content hash at runtime and compares it to silver's stored key; `dim_callsign.airline_sk` resolves via a point-in-time join to `dim_airline` (not just whichever version is current now).
- **`fct_flight_movement`**: a departure/arrival change soft-deletes the old row (`is_deleted=true`) and inserts a new one (`record_type='RESCHEDULE'`); FKs resolve point-in-time against each dimension's effective window as of the flight's departure.

### Deploy scripts & schedule (Asia/Bangkok)

| Script | Job | Task order | dev | qa | prod |
|---|---|---|---|---|---|
| `databricks_silver.py` | `opensky-silver-transform-<catalog>` | flights → callsigns → airlines → airports → aircrafts | 11:00 | 11:15 | 11:30 |
| `databricks_gold.py` | `opensky-gold-transform-<catalog>` | dim_airline → dim_aircraft → dim_airport → dim_callsign → fct_flight_movement | 12:00 | 12:15 | 12:30 |
| `databricks_master.py` | `master-transform-<catalog>` | date → time | Jan 1, 00:00 (same for all catalogs) | | |

---

## 4. Workflows (`.github/workflows/`)

**Deploy** (auto on push to path + branch, or manual `workflow_dispatch`):

| Workflow | Triggers on | Use when |
|---|---|---|
| `databricks-infra-deploy.yml` | `de-pipeline/databricks/setup/**` (main) | Changing bronze/landing table DDL |
| `databricks-landing-deploy.yml` | `de-pipeline/databricks/landing/**` (main) | Changing a `COPY INTO` SQL file or adding a bronze table |
| `databricks-env-deploy.yml` | `de-pipeline/databricks/env_setup/**` (main/main_qa/main_prod) | Changing catalog/schema/table DDL for silver/gold/master |
| `databricks-silver-deploy.yml` | `transformation/silver/**` or `databricks_silver.py` | Changing silver transform logic or schedule |
| `databricks-gold-deploy.yml` | `transformation/gold/**` or `databricks_gold.py` | Changing gold transform logic or schedule |
| `databricks-master-deploy.yml` | `transformation/master/**` or `databricks_master.py` | Changing date/time dimension logic |
| `databricks-ingestion-deploy.yml` | `de-pipeline/ingestion/**` | Changing ingestion code, syncs it to Databricks and updates the daily 10:00 job |
| `snowflake-infra-deploy.yml` | `de-pipeline/snowflake/setup/**` (main) | Changing Snowflake bronze setup |

**Destroy** (`workflow_dispatch` only, type `destroy` to confirm):

`databricks-infra-destroy.yml`, `databricks-silver-destroy.yml`, `databricks-gold-destroy.yml`, `databricks-master-destroy.yml`, `snowflake-infra-destroy.yml` — each drops exactly the tables its matching deploy workflow creates (bronze keeps `airports_master`; catalog/schema/Volume untouched).

**Other:**

| Workflow | Trigger | Purpose |
|---|---|---|
| `pr-target-check.yml` | any pull request | Enforces `main → main_qa → main_prod` PR flow, blocks skipping a stage |
| `de-ingest.yml` | manual/`repository_dispatch` | **Not working** — OpenSky blocks GitHub Actions runner IPs at the TCP level. Kept for reference only; run ingestion locally instead (§2). |
