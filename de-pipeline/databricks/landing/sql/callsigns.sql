COPY INTO opensky_raw.bronze.callsigns
FROM (
  SELECT *, current_timestamp() AS _loaded_at
  FROM '/Volumes/opensky_raw/bronze/landing/callsigns/'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'nullValue' = '');
