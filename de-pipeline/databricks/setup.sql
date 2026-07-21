-- Landing (Bronze) objects for OpenSky ingestion. Idempotent - safe to re-run.
-- Scope: raw landing catalog only; dev/qa/prod catalogs are a later step.

CREATE CATALOG IF NOT EXISTS opensky_raw;

CREATE SCHEMA IF NOT EXISTS opensky_raw.bronze;

-- Landing volume: load_to_databricks.py uploads CSVs here (fast file copy) instead of
CREATE VOLUME IF NOT EXISTS opensky_raw.bronze.landing;

-- Departures + arrivals share one table, split by movement_type.
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

-- Append-only-if-new; _loaded_at marks the first-seen batch.
CREATE TABLE IF NOT EXISTS opensky_raw.bronze.callsigns (
    `callsign` STRING,
    `_loaded_at` TIMESTAMP
) USING DELTA;

-- Same append-only-if-new behavior as callsigns.
CREATE TABLE IF NOT EXISTS opensky_raw.bronze.airports (
    `icao` STRING,
    `_loaded_at` TIMESTAMP
) USING DELTA;

-- Optional: full access for a human account. Skipped if ADMIN_PRINCIPAL is unset.
GRANT ALL PRIVILEGES ON CATALOG opensky_raw TO {{ADMIN_PRINCIPAL}};
