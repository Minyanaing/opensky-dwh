-- Landing (Bronze) objects for OpenSky ingestion. Idempotent - safe to re-run.
--
-- Scope: the raw landing catalog only. The dev/qa/prod catalogs for the dbt Silver/Gold
-- layers are a separate, later step - not created here.
--
-- No GRANT statements: whichever identity runs this (the opensky_deploy_sp service principal
-- in CI/CD) becomes the OWNER of everything it creates in Unity Catalog, so it already has
-- full rights on these objects. The one privilege it needs *before* this can run at all -
-- CREATE CATALOG on the metastore - has to be granted once, manually, by an account/metastore
-- admin (see README.md's one-time setup section); that can't be self-granted by the script.

CREATE CATALOG IF NOT EXISTS opensky_raw;

CREATE SCHEMA IF NOT EXISTS opensky_raw.bronze;

-- Both /flights/departure and /flights/arrival return the same Flight shape, so one table
-- holds both, discriminated by movement_type. Matches ingest_opensky.py's flights_raw.json.
CREATE TABLE IF NOT EXISTS opensky_raw.bronze.flights_raw (
    `icao24` STRING,
    `callsign` STRING,
    `estDepartureAirport` STRING,
    `estArrivalAirport` STRING,
    `firstSeen` BIGINT,
    `lastSeen` BIGINT,
    `estDepartureAirportHorizDistance` BIGINT,
    `estDepartureAirportVertDistance` BIGINT,
    `estArrivalAirportHorizDistance` BIGINT,
    `estArrivalAirportVertDistance` BIGINT,
    `departureAirportCandidatesCount` BIGINT,
    `arrivalAirportCandidatesCount` BIGINT,
    `queried_airport` STRING,
    `movement_type` STRING,
    `fetched_at` STRING,
    `_loaded_at` TIMESTAMP
) USING DELTA;

-- Matches ingest_opensky.py's callsigns.csv. Append-only, same as flights_raw - a callsign
-- seen again in a later run lands as a new row rather than upserting; de-duplication is a
-- Silver-layer concern, not Bronze's.
CREATE TABLE IF NOT EXISTS opensky_raw.bronze.callsigns (
    `callsign` STRING,
    `_loaded_at` TIMESTAMP
) USING DELTA;

-- Matches ingest_opensky.py's airports.csv (both ends of every flight, not just the curated
-- queried airports - see that script's docstring).
CREATE TABLE IF NOT EXISTS opensky_raw.bronze.airports (
    `icao` STRING,
    `_loaded_at` TIMESTAMP
) USING DELTA;
