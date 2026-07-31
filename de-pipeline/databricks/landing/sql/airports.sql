COPY INTO opensky_raw.bronze.airports
FROM (
  SELECT
    TRIM(icao) AS icao,
    TRIM(iata) AS iata,
    TRIM(name) AS name,
    TRIM(country) AS country,
    CAST(lat AS DOUBLE) AS lat,
    CAST(lon AS DOUBLE) AS lon,
    current_timestamp() AS _loaded_at
  FROM '/Volumes/opensky_raw/bronze/landing/airports/'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'nullValue' = '');
