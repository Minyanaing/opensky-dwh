COPY INTO opensky_raw.bronze.airlines
FROM (
  SELECT *, current_timestamp() AS _loaded_at
  FROM '/Volumes/opensky_raw/bronze/landing/airlines/'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'nullValue' = '');
