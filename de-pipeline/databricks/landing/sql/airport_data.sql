COPY INTO opensky_raw.bronze.airport_data
FROM (
  SELECT
    TRIM(queried_icao) AS icao,
    TRIM(iata) AS iata,
    TRIM(name) AS name,
    TRIM(location) AS location,
    TRIM(country) AS country,
    TRIM(country_code) AS country_code,
    CAST(longitude AS DOUBLE) AS longitude,
    CAST(latitude AS DOUBLE) AS latitude,
    TRIM(fetched_at) AS fetched_at,
    current_timestamp() AS _loaded_at
  FROM '/Volumes/opensky_raw/bronze/landing/airport_data/'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'nullValue' = '');
