-- Silver/Gold table definitions - written once here, identical across dev_catalog/qa_catalog/
-- prod_catalog. Use the {{CATALOG}} placeholder instead of hardcoding a catalog name - env_setup.py
-- --catalog <dev_catalog|qa_catalog|prod_catalog> substitutes it before applying, so running this
-- file once per environment produces identical tables in each.

-- Populated by transformation/silver/silver_flights.sql - flight_key (md5 of the match key) lets
-- MERGE compare one column instead of 5; see that file for why movement_type is excluded from it.
-- year/month/day are plain columns, not GENERATED (MERGE ... INSERT * requires the source to
-- supply every target column - see delta-io/delta#3318), derived from flight_date in the SELECT.
CREATE TABLE IF NOT EXISTS {{CATALOG}}.silver_flights.flights (
    `flight_key` STRING,
    `flight_date` DATE,
    `year` INT,
    `month` INT,
    `day` INT,
    `first_seen` TIMESTAMP,
    `last_seen` TIMESTAMP,
    `icao24` STRING,
    `callsign` STRING,
    `estDepartureAirport` STRING,
    `estArrivalAirport` STRING,
    `movement_type` STRING,
    `_loaded_at_raw` TIMESTAMP,
    `_loaded_at` TIMESTAMP
) USING DELTA
PARTITIONED BY (year, month, day);

-- Populated by transformation/silver/silver_airlines.sql. Bronze airlines are already deduped by
-- icao (ingest_adsbdb.py), so the merge key is icao alone - no hash column needed for one column.
CREATE TABLE IF NOT EXISTS {{CATALOG}}.silver_flights.airlines (
    `icao` STRING,
    `iata` STRING,
    `name` STRING,
    `country` STRING,
    `callsign` STRING,
    `_loaded_at_raw` TIMESTAMP,
    `_loaded_at` TIMESTAMP
) USING DELTA;

-- Populated by transformation/silver/silver_aircrafts.sql. Merge key is icao24 alone (one row per
-- aircraft, from ingest_adsbdb.py --aircraft).
CREATE TABLE IF NOT EXISTS {{CATALOG}}.silver_flights.aircrafts (
    `icao24` STRING,
    `type` STRING,
    `icao_type` STRING,
    `manufacturer` STRING,
    `registration` STRING,
    `registered_owner` STRING,
    `registered_owner_country` STRING,
    `_loaded_at_raw` TIMESTAMP,
    `_loaded_at` TIMESTAMP
) USING DELTA
PARTITIONED BY (registered_owner_country);

-- Populated by transformation/silver/silver_callsigns.sql. Merge key is callsign alone (already
-- deduped upstream by export_callsigns.py's new-only diff). year/month/day partition by ingestion
-- date - callsigns has no per-row flight-event timestamp of its own.
CREATE TABLE IF NOT EXISTS {{CATALOG}}.silver_flights.callsigns (
    `flight_date` DATE,
    `year` INT,
    `month` INT,
    `day` INT,
    `callsign` STRING,
    `callsign_icao` STRING,
    `callsign_iata` STRING,
    `airline_icao` STRING,
    `airline_iata` STRING,
    `origin_icao` STRING,
    `origin_iata` STRING,
    `destination_icao` STRING,
    `destination_iata` STRING,
    `_loaded_at_raw` TIMESTAMP,
    `_loaded_at` TIMESTAMP
) USING DELTA
PARTITIONED BY (year, month, day);

-- Populated by transformation/silver/silver_airports.sql - opensky_raw.bronze.airports (adsbdb)
-- and opensky_raw.bronze.airport_data (airport-data.com), each scoped to its own latest batch and
-- deduped to one row per (icao, iata), full-outer-joined on icao, then left-joined to
-- opensky_raw.bronze.airports_master (OurAirports) for name/country/lat/lon fallback plus
-- master-only attributes. Merge key is (icao, iata), and unlike the other silver tables this one
-- also UPDATEs on match - see that file for why.
CREATE TABLE IF NOT EXISTS {{CATALOG}}.silver_flights.airports (
    `icao` STRING,
    `iata` STRING,
    `name` STRING,
    `continent` STRING,
    `country_code` STRING,
    `iso_region` STRING,
    `country` STRING,
    `location` STRING,
    `municipality` STRING,
    `lat` DOUBLE,
    `lon` DOUBLE,
    `type` STRING,
    `elevation_ft` BIGINT,
    `icao_code` STRING,
    `iata_code` STRING,
    `gps_code` STRING,
    `local_code` STRING,
    `home_link` STRING,
    `_loaded_at_raw` TIMESTAMP,
    `_loaded_at` TIMESTAMP
) USING DELTA;
