COPY INTO opensky_raw.bronze.aircrafts
FROM (
  SELECT *, current_timestamp() AS _loaded_at
  FROM '/Volumes/opensky_raw/bronze/landing/aircrafts/'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'nullValue' = '');
