TRUNCATE TABLE opensky_raw.bronze.airports_master;

COPY INTO opensky_raw.bronze.airports_master
FROM (
  SELECT
    CAST(id AS BIGINT) AS id,
    TRIM(ident) AS ident,
    TRIM(type) AS type,
    TRIM(name) AS name,
    CAST(latitude_deg AS DOUBLE) AS latitude_deg,
    CAST(longitude_deg AS DOUBLE) AS longitude_deg,
    CAST(elevation_ft AS BIGINT) AS elevation_ft,
    TRIM(continent) AS continent,
    TRIM(iso_country) AS iso_country,
    TRIM(iso_region) AS iso_region,
    TRIM(municipality) AS municipality,
    TRIM(scheduled_service) AS scheduled_service,
    TRIM(icao_code) AS icao_code,
    TRIM(iata_code) AS iata_code,
    TRIM(gps_code) AS gps_code,
    TRIM(local_code) AS local_code,
    TRIM(home_link) AS home_link,
    TRIM(wikipedia_link) AS wikipedia_link,
    TRIM(keywords) AS keywords,
    current_timestamp() AS _loaded_at
  FROM '/Volumes/opensky_raw/bronze/landing/airports_master/'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'nullValue' = '');
