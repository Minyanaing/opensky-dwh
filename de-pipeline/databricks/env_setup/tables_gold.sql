-- {{CATALOG}} substituted per environment by env_setup.py --catalog.

-- flights_sk is the row PK (flight_key alone repeats across reschedules) - see
-- transformation/gold/fct_flight_movement.sql for the soft-delete/reschedule logic.
CREATE TABLE IF NOT EXISTS {{CATALOG}}.gold_flights.fct_flight_movement (
    `flights_sk` STRING,
    `flight_key` STRING,
    `departure_date_sk` INT,
    `departure_time_sk` INT,
    `arrival_date_sk` INT,
    `arrival_time_sk` INT,
    `aircraft_sk` STRING,
    `callsign_sk` STRING,
    `origin_airport_sk` STRING,
    `destination_airport_sk` STRING,
    `departure` TIMESTAMP,
    `arrival` TIMESTAMP,
    `movement_type` STRING,
    `flight_duration_minutes` INT,
    `record_type` STRING,
    `is_deleted` BOOLEAN,
    `_loaded_at` TIMESTAMP
) USING DELTA;

-- FLAG: FKs resolve against each dim's is_current row at merge time (not point-in-time as of
-- departure) - revisit if that distinction matters.
CREATE TABLE IF NOT EXISTS {{CATALOG}}.gold_flights.dim_airline (
    `airline_sk` STRING,
    `icao` STRING,
    `iata` STRING,
    `name` STRING,
    `country` STRING,
    `callsign` STRING,
    `effective_start` TIMESTAMP,
    `effective_end` TIMESTAMP,
    `is_current` BOOLEAN
) USING DELTA;

CREATE TABLE IF NOT EXISTS {{CATALOG}}.gold_flights.dim_aircraft (
    `aircraft_sk` STRING,
    `icao24` STRING,
    `type` STRING,
    `icao_type` STRING,
    `manufacturer` STRING,
    `registration` STRING,
    `registered_owner` STRING,
    `registered_owner_country` STRING,
    `effective_start` TIMESTAMP,
    `effective_end` TIMESTAMP,
    `is_current` BOOLEAN
) USING DELTA;

-- FLAG: re-keyed on icao alone (unlike silver's icao+iata+name hash key) - see dim_airport.sql.
CREATE TABLE IF NOT EXISTS {{CATALOG}}.gold_flights.dim_airport (
    `airport_sk` STRING,
    `icao` STRING,
    `iata` STRING,
    `name` STRING,
    `country` STRING,
    `country_code` STRING,
    `continent` STRING,
    `iso_region` STRING,
    `municipality` STRING,
    `location` STRING,
    `lat` DOUBLE,
    `lon` DOUBLE,
    `type` STRING,
    `elevation_ft` BIGINT,
    `icao_code` STRING,
    `iata_code` STRING,
    `gps_code` STRING,
    `local_code` STRING,
    `home_link` STRING,
    `effective_start` TIMESTAMP,
    `effective_end` TIMESTAMP,
    `is_current` BOOLEAN
) USING DELTA;

CREATE TABLE IF NOT EXISTS {{CATALOG}}.gold_flights.dim_callsign (
    `callsign_sk` STRING,
    `callsign` STRING,
    `callsign_icao` STRING,
    `callsign_iata` STRING,
    `airline_sk` STRING,
    `origin_icao` STRING,
    `origin_iata` STRING,
    `destination_icao` STRING,
    `destination_iata` STRING,
    `effective_start` TIMESTAMP,
    `effective_end` TIMESTAMP,
    `is_current` BOOLEAN
) USING DELTA;
