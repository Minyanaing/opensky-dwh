-- {{CATALOG}} substituted per environment by env_setup.py --catalog. Gold tables: tables_gold.sql.

-- flight_key = md5 of the match key, which excludes movement_type - see silver_flights.sql.
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

CREATE TABLE IF NOT EXISTS {{CATALOG}}.silver_flights.airlines (
    `icao` STRING,
    `iata` STRING,
    `name` STRING,
    `country` STRING,
    `callsign` STRING,
    `_loaded_at_raw` TIMESTAMP,
    `_loaded_at` TIMESTAMP
) USING DELTA;

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

-- Unlike the other silver tables, this one UPDATEs on match - see silver_airports.sql.
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
