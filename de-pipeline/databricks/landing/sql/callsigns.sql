COPY INTO opensky_raw.bronze.callsigns
FROM (
  SELECT
    callsign,
    CAST(found AS BOOLEAN) AS found,
    callsign_icao,
    callsign_iata,
    airline_icao,
    airline_iata,
    airline_name,
    airline_country,
    airline_callsign,
    origin_icao,
    origin_iata,
    origin_name,
    origin_country,
    CAST(origin_lat AS DOUBLE) AS origin_lat,
    CAST(origin_lon AS DOUBLE) AS origin_lon,
    destination_icao,
    destination_iata,
    destination_name,
    destination_country,
    CAST(destination_lat AS DOUBLE) AS destination_lat,
    CAST(destination_lon AS DOUBLE) AS destination_lon,
    fetched_at,
    current_timestamp() AS _loaded_at
  FROM '/Volumes/opensky_raw/bronze/landing/callsigns/'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'nullValue' = '');
