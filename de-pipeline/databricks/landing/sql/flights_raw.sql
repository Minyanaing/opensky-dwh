COPY INTO opensky_raw.bronze.flights_raw
FROM (
  SELECT *, current_timestamp() AS _loaded_at
  FROM '/Volumes/opensky_raw/bronze/landing/flights_raw/'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'nullValue' = '');
