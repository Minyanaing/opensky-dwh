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

-- adsbdb.com enrichment landing tables - columns match ingest_adsbdb.py's CALLSIGN_COLUMNS.
CREATE TABLE IF NOT EXISTS opensky_raw.bronze.callsigns (
    `callsign` STRING,
    `found` BOOLEAN,
    `callsign_icao` STRING,
    `callsign_iata` STRING,
    `airline_icao` STRING,
    `airline_iata` STRING,
    `airline_name` STRING,
    `airline_country` STRING,
    `airline_callsign` STRING,
    `origin_icao` STRING,
    `origin_iata` STRING,
    `origin_name` STRING,
    `origin_country` STRING,
    `origin_lat` DOUBLE,
    `origin_lon` DOUBLE,
    `destination_icao` STRING,
    `destination_iata` STRING,
    `destination_name` STRING,
    `destination_country` STRING,
    `destination_lat` DOUBLE,
    `destination_lon` DOUBLE,
    `fetched_at` STRING,
    `_loaded_at` TIMESTAMP
) USING DELTA;

-- Matches ingest_adsbdb.py's AIRLINE_COLUMNS - derived from callsigns, not a separate API call.
CREATE TABLE IF NOT EXISTS opensky_raw.bronze.airlines (
    `icao` STRING,
    `iata` STRING,
    `name` STRING,
    `country` STRING,
    `callsign` STRING,
    `_loaded_at` TIMESTAMP
) USING DELTA;

-- Matches ingest_adsbdb.py's AIRPORT_COLUMNS - derived from callsigns, not a separate API call.
CREATE TABLE IF NOT EXISTS opensky_raw.bronze.airports (
    `icao` STRING,
    `iata` STRING,
    `name` STRING,
    `country` STRING,
    `lat` DOUBLE,
    `lon` DOUBLE,
    `_loaded_at` TIMESTAMP
) USING DELTA;

-- Matches ingest_adsbdb.py's AIRCRAFT_COLUMNS - only populated via ingest_adsbdb.py --aircraft.
CREATE TABLE IF NOT EXISTS opensky_raw.bronze.aircrafts (
    `icao24` STRING,
    `found` BOOLEAN,
    `type` STRING,
    `icao_type` STRING,
    `manufacturer` STRING,
    `registration` STRING,
    `registered_owner` STRING,
    `registered_owner_country` STRING,
    `fetched_at` STRING,
    `_loaded_at` TIMESTAMP
) USING DELTA;

-- Optional: full access for a human account. Skipped if ADMIN_PRINCIPAL is unset.
-- MANAGE is listed separately - ALL PRIVILEGES deliberately excludes it (anti-privilege-escalation),
-- but without it ADMIN_PRINCIPAL can't DROP/ALTER tables it doesn't own.
GRANT ALL PRIVILEGES, MANAGE ON CATALOG opensky_raw TO {{ADMIN_PRINCIPAL}};
