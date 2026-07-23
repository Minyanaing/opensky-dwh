COPY INTO opensky_raw.bronze.airports_master
FROM (
  SELECT
    CAST(id AS BIGINT) AS id,
    ident,
    type,
    name,
    CAST(latitude_deg AS DOUBLE) AS latitude_deg,
    CAST(longitude_deg AS DOUBLE) AS longitude_deg,
    CAST(elevation_ft AS BIGINT) AS elevation_ft,
    continent,
    iso_country,
    iso_region,
    municipality,
    scheduled_service,
    icao_code,
    iata_code,
    gps_code,
    local_code,
    home_link,
    wikipedia_link,
    keywords,
    current_timestamp() AS _loaded_at
  FROM '/Volumes/opensky_raw/bronze/landing/airports_master/'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'nullValue' = '');
