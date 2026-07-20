# de-pipeline

Implements the plan in `.claude/databricks-opensky-de-pipeline-project-plan.md`. **Built today:** ingestion (Step 2), Databricks landing setup (Step 1), and both GitHub Actions workflows (`infra-deploy.yml`, `de-ingest.yml`). dbt, orchestration, and promotion (Step 4 onward) are still just the plan document.

---

## 1. Databricks landing setup (one-time)

Creates `opensky_raw.bronze` (catalog + schema + the three tables ingestion writes into) and is what makes `infra-deploy.yml` work. Do this before anything else.

### 1a. Create the service principal

In your Databricks workspace: **Settings → Identity and access → Service principals → Add service principal.** Name it e.g. `opensky_deploy_sp`, then open it and note its **Application ID** (a UUID, shown on its details page — this is the same value you'll use as `DATABRICKS_CLIENT_ID`), then generate an **OAuth secret** — the **Client Secret** is shown only once.

### 1b. Grant it what it needs

Two grants, both one-time, both need an account/metastore admin (if you're the only user on your Free Edition account, that's you):

- **`CREATE CATALOG` on the metastore** — the actual unlock; without it, `databricks_setup.py`'s first statement fails. In a SQL editor/notebook logged in as an admin, use the **Application ID**, not the display name:
  ```sql
  GRANT CREATE CATALOG ON METASTORE TO `<application-id>`;
  ```
  > **`PRINCIPAL_DOES_NOT_EXIST`?** Unity Catalog's `GRANT ... TO` only recognizes service principals by their Application ID (a UUID) — the display name you gave it in step 1a (e.g. `opensky_deploy_sp` or `deploy_sp`) isn't a valid reference here and will always produce this error. Copy the Application ID from the service principal's details page instead.
- **`CAN_USE` on the SQL Warehouse** — via the warehouse's **Permissions** tab in the UI, add the service principal (searchable by display name here — the UI picker is fine, it's only raw SQL `GRANT` that requires the Application ID) with **Can Use**.

### 1c. Get the SQL Warehouse connection details

Any SQL Warehouse → **Connection details** tab → copy **Server hostname** (this is `DATABRICKS_HOST`, no `https://` prefix) and **HTTP path** (`DATABRICKS_HTTP_PATH`).

### 1d. Test it locally first

```
cd de-pipeline/databricks
python -m venv .venv
.venv\Scripts\activate      # Windows
pip install -r requirements.txt
copy .env.example .env
```

Fill in `.env`: `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET` (or just `DATABRICKS_TOKEN` if you're using the PAT fallback instead). Also set `ADMIN_PRINCIPAL` to your own account's login email — see the callout below for why. Then:

```
python databricks_setup.py
```

Success looks like log lines for each DDL statement (5, or 6 if `ADMIN_PRINCIPAL` is set), ending in `Databricks landing setup complete`. Verify in **Catalog Explorer**: `opensky_raw` → `bronze` → `flights_raw` / `callsigns` / `airports` should all exist (empty is fine, they've just been created). Safe to re-run any time — every statement is `IF NOT EXISTS` (or a plain re-grant, for the `ADMIN_PRINCIPAL` one).

> **Why `ADMIN_PRINCIPAL` matters, and what happens if you skip it:** the service principal that runs this script *owns* everything it creates (Unity Catalog's ownership-by-creation model — see the plan's Key design decisions). That means **your own admin login won't be able to see `opensky_raw` in Catalog Explorer** unless it's either the true metastore admin (different from, and easily confused with, being a *workspace* admin — this exact confusion is how the catalog went briefly invisible during initial setup) or has been explicitly granted access. Setting `ADMIN_PRINCIPAL` to your email makes `setup.sql`'s last statement grant you `USE CATALOG`/`USE SCHEMA`/`SELECT` automatically, every run — no manual one-off `GRANT` needed. Leave it unset and that statement is skipped entirely; you'll still be able to run everything via the service principal, you just won't be able to browse the catalog yourself without a separate manual grant.

### 1e. Test it via CI/CD (`infra-deploy.yml`)

This is what actually needs to work end-to-end before ingestion can land anywhere.

1. **Add the GitHub secrets** — repo → Settings → Secrets and variables → Actions → New repository secret: `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET` (same values as your local `.env`).
2. **Trigger it** — the workflow runs automatically on a push to `main` touching `de-pipeline/databricks/**`, or on demand: GitHub repo → **Actions** → **Databricks infra deploy** → **Run workflow**. (Or from the CLI: `gh workflow run infra-deploy.yml`.)
3. **Check the run** — the one job (`deploy`) should go green; its log should show the same 5-statement output as the local run.
4. **Verify the result** — same as 1d: Catalog Explorer should show `opensky_raw.bronze` with all three tables. If this is the very first run, this is also the first time the catalog itself gets created — you're watching CI/CD stand up real Databricks infrastructure from nothing.

If step 3 fails on the very first statement (`CREATE CATALOG`), the metastore-level grant from 1b almost certainly didn't take — that's the most common failure mode here, not a bug in the script.

---

## 2. Ingestion setup (local)

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

   Fill in `OPENSKY_CLIENT_ID` / `OPENSKY_CLIENT_SECRET`. If you'll test `INGEST_MODE=databricks` (below), also fill in the same `DATABRICKS_*` values from Step 1.

## 3. Running ingestion locally

Two modes, controlled by `INGEST_MODE` in `.env` (or the environment):

### Local mode (default, `INGEST_MODE` unset or `""`)

Writes three files to `data/` (gitignored), **overwriting the previous run's output** — no Databricks connection needed at all:

```
# Quick smoke test - one airport, last 1 day
python ingest_opensky.py --airports WSSS --lookback-days 1

# Full run - all 85 curated airports, default 3-day window
python ingest_opensky.py
```

| File | Contents |
|---|---|
| `flights_raw.json` | Raw OpenSky `Flight` records (arrivals + departures), tagged with `queried_airport`, `movement_type`, `fetched_at` |
| `callsigns.csv` | Distinct, whitespace-stripped callsigns seen in that run |
| `airports.csv` | Distinct ICAO airport codes seen — both ends of every flight, so this includes foreign counterpart airports outside the curated 85 |

A previous local run's files can be (re)loaded into Databricks separately: `python load_to_databricks.py`.

### Databricks mode (`INGEST_MODE=databricks`)

Skips the local files entirely and lands the same data straight into `opensky_raw.bronze` (needs the `DATABRICKS_*` vars from Step 1 in `.env`):

```
set INGEST_MODE=databricks          # Windows (cmd)
python ingest_opensky.py --airports WSSS --lookback-days 1
```

`flights_raw` is appended to unconditionally (it's an event log). `callsigns`/`airports` are **append-only-if-new** — each run queries what's already there and only inserts genuinely new values, stamped with that run's `_loaded_at`. Re-running the same window twice should insert flight rows both times but zero new callsigns/airports the second time — that's the expected behavior, not a bug.

## 4. Manual-test utilities

### One aircraft's flight history

```
python fetch_aircraft_history.py --icao24 7823bc --lookback-days 3
```

Writes `data/aircraft_flights_<icao24>_<timestamp>.json`.

### Airline / aircraft / callsign enrichment (adsbdb.com)

`ingest_adsbdb.py` reads directly from Databricks — **only the latest discovery batch** (`MAX(_loaded_at)`) from the `callsigns` and `flights_raw` tables — and enriches it via the free, keyless [adsbdb.com](https://www.adsbdb.com/) API. Needs the same `DATABRICKS_*` vars as Step 1/3, and only makes sense to run after at least one `INGEST_MODE=databricks` ingest:

```
python ingest_adsbdb.py
```

Writes three files to `data/`, overwritten each run:

| File | Contents |
|---|---|
| `adsbdb_callsigns.json` | Per-callsign route lookup (`/callsign/{callsign}`) — origin/destination airport + embedded airline, or `found: false` if adsbdb doesn't have it |
| `adsbdb_aircraft.json` | Per-`icao24` aircraft lookup (`/aircraft/{mode_s}`) — type, manufacturer, registration, owner |
| `adsbdb_airlines.json` | Per-airline lookup (`/airline/{icao}`), for ICAO airline codes derived from callsign prefixes (e.g. `CSZ` from `CSZ306`) |

Because callsigns/airports are append-only-if-new, most runs process a small "what's new" batch rather than the full history — `MAX_LOCAL_TEST_ITEMS` (20) is a safety cap for an unusually large batch, not the everyday throttle. adsbdb is community-maintained, not authoritative — expect `found: false` sometimes, that's normal. It has no airport-lookup endpoint, so the `airports` table is read (for visibility in the logs) but never queried against adsbdb.

## 5. GitHub Actions (CI/CD)

| Workflow | Trigger | What it does |
|---|---|---|
| `infra-deploy.yml` | push to `main` touching `de-pipeline/databricks/**`, or manual dispatch | Runs `databricks_setup.py` — see Section 1e to test it |
| `de-ingest.yml` | manual dispatch (`lookback_days` input), or `repository_dispatch` type `de-ingest` | Runs `ingest_opensky.py` with `INGEST_MODE=databricks` — fetches and lands in one step, no separate load step |

**Secrets needed** (repo → Settings → Secrets and variables → Actions):

| Secret | Used by |
|---|---|
| `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH` | both workflows |
| `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET` | both workflows |
| `DATABRICKS_TOKEN` | fallback, instead of the two above |
| `ADMIN_PRINCIPAL` | `infra-deploy.yml` (optional — see Section 1d) |
| `OPENSKY_CLIENT_ID`, `OPENSKY_CLIENT_SECRET` | `de-ingest.yml` |

`de-ingest.yml` isn't on any schedule yet — GitHub's built-in `schedule:` is deliberately not used (only fires from the default branch, delayed/dropped under load); wiring up an external cron (e.g. cron-job.org) to hit its `repository_dispatch` endpoint is a manual step outside this repo.

## Notes

- The curated airport list (all 85 designated international airports across Southeast Asia) lives in `ingestion/config.py`.
- Ingestion's *window* is stateless: every run re-fetches the full trailing window rather than tracking a watermark, so a repeated or manual run is always safe to re-run. `callsigns`/`airports` landing is the one place that *is* stateful (append-only-if-new).
- `--airports` accepts a comma-separated ICAO subset, for testing without pulling the full list.
