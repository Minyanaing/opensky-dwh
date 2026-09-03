TRUNCATE TABLE opensky_raw.bronze.airlines;

COPY INTO opensky_raw.bronze.airlines
FROM (
  SELECT
    TRIM(icao) AS icao,
    TRIM(iata) AS iata,
    TRIM(name) AS name,
    TRIM(country) AS country,
    TRIM(callsign) AS callsign,
    current_timestamp() AS _loaded_at
  FROM '/Volumes/opensky_raw/bronze/landing/airlines/'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'nullValue' = '');
