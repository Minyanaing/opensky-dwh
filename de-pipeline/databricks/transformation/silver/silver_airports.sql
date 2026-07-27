-- {{CATALOG}} substituted per environment. airport_key hashes icao/iata/name - a changed name
-- gets its own new row instead of overwriting the old one, so this table keeps history. Other
-- coalesced fields (country/lat/lon/etc) aren't part of the key, so they freeze at whatever was
-- first captured for that icao/iata/name unless name also changes.
BEGIN
  DECLARE row_count BIGINT DEFAULT 0;
  SET row_count = (SELECT COUNT(*) FROM {{CATALOG}}.silver_flights.airports);

  IF row_count = 0 THEN
    MERGE INTO {{CATALOG}}.silver_flights.airports AS target
    USING (
      WITH airports_src AS (
        SELECT icao, iata, name, country, lat, lon, _loaded_at
        FROM opensky_raw.bronze.airports
        QUALIFY ROW_NUMBER() OVER (PARTITION BY icao, iata ORDER BY _loaded_at DESC) = 1
      ),
      airport_data_src AS (
        SELECT
          icao, iata, name, country, location, country_code,
          latitude AS lat, longitude AS lon,
          _loaded_at
        FROM opensky_raw.bronze.airport_data
        WHERE icao IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY icao, iata ORDER BY _loaded_at DESC) = 1
      ),
      joined AS (
        SELECT
          COALESCE(a.icao, ad.icao) AS icao,
          COALESCE(a.iata, ad.iata) AS iata,
          COALESCE(a.name, ad.name) AS name,
          COALESCE(a.country, ad.country) AS country,
          ad.location,
          ad.country_code,
          COALESCE(NULLIF(a.lat, 0), NULLIF(ad.lat, 0)) AS lat,
          COALESCE(NULLIF(a.lon, 0), NULLIF(ad.lon, 0)) AS lon,
          COALESCE(a._loaded_at, ad._loaded_at) AS _loaded_at
        FROM airports_src AS a
        FULL OUTER JOIN airport_data_src AS ad ON a.icao = ad.icao
      ),
      master_dedup AS (
        SELECT
          ident, type, name, latitude_deg, longitude_deg, elevation_ft,
          continent, iso_country, iso_region, municipality,
          icao_code, iata_code, gps_code, local_code, home_link
        FROM opensky_raw.bronze.airports_master
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ident ORDER BY id) = 1
      )
      SELECT
        md5(
          concat_ws(
            '||', 
            j.icao, 
            j.iata, 
            COALESCE(j.name, m.name)
          )
        ) AS airport_key,
        j.icao,
        j.iata,
        COALESCE(j.name, m.name) AS name,
        m.continent,
        COALESCE(j.country_code, m.iso_country) AS country_code,
        m.iso_region,
        COALESCE(j.country, m.iso_country) AS country,
        COALESCE(j.location, m.municipality) AS location,
        m.municipality,
        COALESCE(j.lat, m.latitude_deg) AS lat,
        COALESCE(j.lon, m.longitude_deg) AS lon,
        m.type,
        m.elevation_ft,
        m.icao_code,
        m.iata_code,
        m.gps_code,
        m.local_code,
        m.home_link,
        j._loaded_at AS _loaded_at_raw,
        current_timestamp() AS _loaded_at
      FROM joined AS j
      LEFT JOIN master_dedup AS m ON m.ident = j.icao
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY md5(concat_ws('||', j.icao, j.iata, COALESCE(j.name, m.name)))
        ORDER BY j._loaded_at DESC
      ) = 1
    ) AS source
    ON target.airport_key = source.airport_key
    WHEN NOT MATCHED THEN INSERT *;
  ELSE
    MERGE INTO {{CATALOG}}.silver_flights.airports AS target
    USING (
      WITH airports_src AS (
        SELECT icao, iata, name, country, lat, lon, _loaded_at
        FROM opensky_raw.bronze.airports
        WHERE _loaded_at = (SELECT MAX(_loaded_at) FROM opensky_raw.bronze.airports)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY icao, iata ORDER BY _loaded_at DESC) = 1
      ),
      airport_data_src AS (
        SELECT
          icao, iata, name, country, location, country_code,
          latitude AS lat, longitude AS lon,
          _loaded_at
        FROM opensky_raw.bronze.airport_data
        WHERE icao IS NOT NULL
          AND _loaded_at = (SELECT MAX(_loaded_at) FROM opensky_raw.bronze.airport_data)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY icao, iata ORDER BY _loaded_at DESC) = 1
      ),
      joined AS (
        SELECT
          COALESCE(a.icao, ad.icao) AS icao,
          COALESCE(a.iata, ad.iata) AS iata,
          COALESCE(a.name, ad.name) AS name,
          COALESCE(a.country, ad.country) AS country,
          ad.location,
          ad.country_code,
          COALESCE(NULLIF(a.lat, 0), NULLIF(ad.lat, 0)) AS lat,
          COALESCE(NULLIF(a.lon, 0), NULLIF(ad.lon, 0)) AS lon,
          COALESCE(a._loaded_at, ad._loaded_at) AS _loaded_at
        FROM airports_src AS a
        FULL OUTER JOIN airport_data_src AS ad ON a.icao = ad.icao
      ),
      master_dedup AS (
        SELECT
          ident, type, name, latitude_deg, longitude_deg, elevation_ft,
          continent, iso_country, iso_region, municipality,
          icao_code, iata_code, gps_code, local_code, home_link
        FROM opensky_raw.bronze.airports_master
        WHERE _loaded_at = (SELECT MAX(_loaded_at) FROM opensky_raw.bronze.airports_master)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ident ORDER BY id) = 1
      )
      SELECT
        md5(
          concat_ws(
            '||', 
            j.icao, 
            j.iata, 
            COALESCE(j.name, m.name)
          )
        ) AS airport_key,
        j.icao,
        j.iata,
        COALESCE(j.name, m.name) AS name,
        m.continent,
        COALESCE(j.country_code, m.iso_country) AS country_code,
        m.iso_region,
        COALESCE(j.country, m.iso_country) AS country,
        COALESCE(j.location, m.municipality) AS location,
        m.municipality,
        COALESCE(j.lat, m.latitude_deg) AS lat,
        COALESCE(j.lon, m.longitude_deg) AS lon,
        m.type,
        m.elevation_ft,
        m.icao_code,
        m.iata_code,
        m.gps_code,
        m.local_code,
        m.home_link,
        j._loaded_at AS _loaded_at_raw,
        current_timestamp() AS _loaded_at
      FROM joined AS j
      LEFT JOIN master_dedup AS m ON m.ident = j.icao
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY md5(concat_ws('||', j.icao, j.iata, COALESCE(j.name, m.name)))
        ORDER BY j._loaded_at DESC
      ) = 1
    ) AS source
    ON target.airport_key = source.airport_key
    WHEN NOT MATCHED THEN INSERT *;
  END IF;
END;

-- Sample resulting rows for one icao after its name changes between loads - same icao/iata,
-- only name differs, so it lands as a second row with a new airport_key:
--
-- airport_key  | icao | iata | name                      | _loaded_at
-- -------------|------|------|---------------------------|-------------------
-- 2c7e4b91...  | VTBS | BKK  | Suvarnabhumi Airport      | 2026-07-01 09:08:00
-- 9a3f0d56...  | VTBS | BKK  | Suvarnabhumi International | 2026-07-15 09:08:00
