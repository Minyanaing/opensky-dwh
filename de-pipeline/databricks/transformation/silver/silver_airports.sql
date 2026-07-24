-- {{CATALOG}} substituted per environment. See silver_flights.sql for why this needs to run as
-- ONE statement (BEGIN...END, Databricks Runtime 16.3+).
--
-- Each bronze source is deduped to one row per (icao, iata), then the two are FULL OUTER JOINed
-- on icao (not the (icao, iata) dedup key - a source with 2+ iata values for the same icao can fan
-- out here) so an airport present in only one source still survives.
--
-- lat/lon/name/country coalesce airports_master (OurAirports) as the fallback when the primary
-- join is missing them. lat/lon use NULLIF(..., 0) before the coalesce, not a plain NULL check -
-- airport-data.com returns 0/0 (not NULL) for airports it has no real coordinates for (see
-- ingest_airports.py), so a plain COALESCE would never fall through to master's real value.
--
-- This table UPDATEs on match, not just inserts - airports_master/bronze airports/airport_data can
-- all get corrected over time, and re-running should reflect that instead of freezing on whatever
-- was first loaded (the flights_raw-derived tables are append-only events, so INSERT-only is
-- correct there; this one is closer to a dimension, so it isn't).
BEGIN
  DECLARE row_count BIGINT DEFAULT 0;
  SET row_count = (SELECT COUNT(*) FROM {{CATALOG}}.silver_flights.airports);

  IF row_count = 0 THEN
    -- Bootstrap: target is empty, load every day currently in each bronze source at once. The
    -- QUALIFY dedups below already pick one row per (icao, iata) regardless of how many days are
    -- in scope, so only the latest-batch restrictions need dropping, not the QUALIFYs themselves.
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
    ) AS source
    ON target.icao <=> source.icao AND target.iata <=> source.iata
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *;
  ELSE
    -- Incremental: each source scoped to its own newest batch only.
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
    ) AS source
    ON target.icao <=> source.icao AND target.iata <=> source.iata
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *;
  END IF;
END;
