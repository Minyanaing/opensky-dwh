-- {{CATALOG}} substituted per environment. Requires Databricks Runtime 16.3+ (BEGIN...END/DECLARE).
-- v_cutoff = epoch if empty, else silver's latest batch only (content-hash deduped, so always new/changed).
-- Hash uses icao/iata/name only, matching silver_airports.sql's airport_key exactly (country/lat/lon/etc are deliberately excluded from that key); concat_ws skips NULLs so no COALESCE needed.
BEGIN
  DECLARE v_row_count BIGINT DEFAULT 0;
  DECLARE v_cutoff TIMESTAMP;

  SET v_row_count = (SELECT COUNT(*) FROM {{CATALOG}}.gold_flights.dim_airport);

  IF v_row_count = 0 THEN
    SET v_cutoff = TIMESTAMP('1970-01-01 00:00:00');
  ELSE
    SET v_cutoff = (SELECT MAX(_loaded_at) FROM {{CATALOG}}.silver_flights.airports);
  END IF;

  MERGE INTO {{CATALOG}}.gold_flights.dim_airport AS target
  USING (
    SELECT icao, iata, name, country, country_code, continent, iso_region, municipality,
      location, lat, lon, type, elevation_ft, icao_code, iata_code, gps_code, local_code,
      home_link, airport_key, _loaded_at
    FROM {{CATALOG}}.silver_flights.airports
    WHERE _loaded_at >= v_cutoff
    QUALIFY ROW_NUMBER() OVER (PARTITION BY icao ORDER BY _loaded_at DESC) = 1
  ) AS source
  ON target.icao = source.icao
    AND target.is_current
  WHEN MATCHED AND md5(
    concat_ws('||', target.icao, target.iata, target.name)
  ) != source.airport_key THEN
    UPDATE SET
      target.effective_end = source._loaded_at,
      target.is_current = false;

  INSERT INTO {{CATALOG}}.gold_flights.dim_airport (
    airport_sk, icao, iata, name, country, country_code, continent, iso_region,
    municipality, location, lat, lon, type, elevation_ft, icao_code, iata_code, gps_code,
    local_code, home_link, effective_start, effective_end, is_current
  )
  SELECT
    md5(concat_ws('||', s.icao, CAST(s._loaded_at AS STRING))) AS airport_sk,
    s.icao, s.iata, s.name, s.country, s.country_code, s.continent, s.iso_region,
    s.municipality, s.location, s.lat, s.lon, s.type, s.elevation_ft, s.icao_code, s.iata_code,
    s.gps_code, s.local_code, s.home_link,
    s._loaded_at AS effective_start,
    TIMESTAMP('9999-12-12 00:00:00') AS effective_end,
    TRUE AS is_current
  FROM (
    SELECT icao, iata, name, country, country_code, continent, iso_region, municipality,
      location, lat, lon, type, elevation_ft, icao_code, iata_code, gps_code, local_code,
      home_link, airport_key, _loaded_at
    FROM {{CATALOG}}.silver_flights.airports
    WHERE _loaded_at >= v_cutoff
    QUALIFY ROW_NUMBER() OVER (PARTITION BY icao ORDER BY _loaded_at DESC) = 1
  ) AS s
  LEFT JOIN {{CATALOG}}.gold_flights.dim_airport AS g
    ON g.icao = s.icao
    AND g.is_current
    AND md5(concat_ws('||', g.icao, g.iata, g.name)) = s.airport_key
  WHERE g.icao IS NULL;
END;
