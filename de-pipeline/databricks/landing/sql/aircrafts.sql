COPY INTO opensky_raw.bronze.aircrafts
FROM (
  SELECT
    icao24,
    CAST(found AS BOOLEAN) AS found,
    type,
    icao_type,
    manufacturer,
    registration,
    registered_owner,
    registered_owner_country,
    fetched_at,
    current_timestamp() AS _loaded_at
  FROM '/Volumes/opensky_raw/bronze/landing/aircrafts/'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'nullValue' = '');
