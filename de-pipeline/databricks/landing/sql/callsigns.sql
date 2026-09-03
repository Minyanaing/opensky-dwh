TRUNCATE TABLE opensky_raw.bronze.callsigns;

COPY INTO opensky_raw.bronze.callsigns
FROM (
  SELECT
    TRIM(callsign) AS callsign,
    CAST(found AS BOOLEAN) AS found,
    TRIM(callsign_icao) AS callsign_icao,
    TRIM(callsign_iata) AS callsign_iata,
    TRIM(airline_icao) AS airline_icao,
    TRIM(airline_iata) AS airline_iata,
    TRIM(airline_name) AS airline_name,
    TRIM(airline_country) AS airline_country,
    TRIM(airline_callsign) AS airline_callsign,
    TRIM(origin_icao) AS origin_icao,
    TRIM(origin_iata) AS origin_iata,
    TRIM(origin_name) AS origin_name,
    TRIM(origin_country) AS origin_country,
    CAST(origin_lat AS DOUBLE) AS origin_lat,
    CAST(origin_lon AS DOUBLE) AS origin_lon,
    TRIM(destination_icao) AS destination_icao,
    TRIM(destination_iata) AS destination_iata,
    TRIM(destination_name) AS destination_name,
    TRIM(destination_country) AS destination_country,
    CAST(destination_lat AS DOUBLE) AS destination_lat,
    CAST(destination_lon AS DOUBLE) AS destination_lon,
    TRIM(fetched_at) AS fetched_at,
    current_timestamp() AS _loaded_at
  FROM '/Volumes/opensky_raw/bronze/landing/callsigns/'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'nullValue' = '');
