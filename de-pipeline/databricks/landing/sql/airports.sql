COPY INTO opensky_raw.bronze.airports
FROM (
  SELECT *, current_timestamp() AS _loaded_at
  FROM '/Volumes/opensky_raw/bronze/landing/airports/'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'nullValue' = '');
