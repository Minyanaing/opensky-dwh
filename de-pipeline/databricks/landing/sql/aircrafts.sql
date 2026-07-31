COPY INTO opensky_raw.bronze.aircrafts
FROM (
  SELECT
    TRIM(icao24) AS icao24,
    CAST(found AS BOOLEAN) AS found,
    TRIM(type) AS type,
    TRIM(icao_type) AS icao_type,
    TRIM(manufacturer) AS manufacturer,
    TRIM(registration) AS registration,
    TRIM(registered_owner) AS registered_owner,
    TRIM(registered_owner_country) AS registered_owner_country,
    TRIM(fetched_at) AS fetched_at,
    current_timestamp() AS _loaded_at
  FROM '/Volumes/opensky_raw/bronze/landing/aircrafts/'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'nullValue' = '');
