-- {{CATALOG}} substituted per environment. Requires Databricks Runtime 16.3+ (BEGIN...END/DECLARE).
-- FLAG: a gap wider than lookback_days silently skips a version.
-- FLAG: silver's airport_key hashes icao+iata+name, not icao alone - 2 rows sharing an icao with
-- different iata/name in one window may not chain cleanly below (icao-partitioned LEAD).
-- Two MERGEs, order matters (close-out first, then insert). Chains gold's current row + all
-- incoming silver rows via LEAD, so every version in the window is tracked, not just the latest.
BEGIN
  DECLARE lookback_days INT DEFAULT 3;
  DECLARE cutoff TIMESTAMP;
  SET cutoff = (
    SELECT MAX(_loaded_at) FROM {{CATALOG}}.silver_flights.airports
  ) - make_interval(0, 0, 0, lookback_days, 0, 0, 0);

  MERGE INTO {{CATALOG}}.gold_flights.dim_airport AS target
  USING (
    WITH incoming AS (
      SELECT icao, _loaded_at
      FROM {{CATALOG}}.silver_flights.airports
      WHERE _loaded_at >= cutoff
      QUALIFY ROW_NUMBER() OVER (PARTITION BY airport_key ORDER BY _loaded_at) = 1
    ),
    combined AS (
      SELECT icao, effective_start AS _loaded_at
      FROM {{CATALOG}}.gold_flights.dim_airport WHERE is_current
      UNION ALL
      SELECT icao, _loaded_at FROM incoming
    )
    SELECT
      icao,
      _loaded_at AS effective_start,
      LEAD(_loaded_at) OVER (PARTITION BY icao ORDER BY _loaded_at) AS next_loaded_at
    FROM combined
  ) AS source
  ON target.icao = source.icao
    AND target.effective_start = source.effective_start
    AND target.is_current
    AND source.next_loaded_at IS NOT NULL
  WHEN MATCHED THEN
    UPDATE SET
      target.effective_end = source.next_loaded_at,
      target.is_current = false;

  MERGE INTO {{CATALOG}}.gold_flights.dim_airport AS target
  USING (
    WITH incoming AS (
      SELECT icao, iata, name, country, country_code, continent, iso_region, municipality,
        location, lat, lon, type, elevation_ft, icao_code, iata_code, gps_code, local_code,
        home_link, airport_key, _loaded_at
      FROM {{CATALOG}}.silver_flights.airports
      WHERE _loaded_at >= cutoff
      QUALIFY ROW_NUMBER() OVER (PARTITION BY airport_key ORDER BY _loaded_at) = 1
    ),
    combined AS (
      SELECT icao, iata, name, country, country_code, continent, iso_region, municipality,
        location, lat, lon, type, elevation_ft, icao_code, iata_code, gps_code, local_code,
        home_link, effective_start AS _loaded_at, TRUE AS is_existing
      FROM {{CATALOG}}.gold_flights.dim_airport WHERE is_current
      UNION ALL
      SELECT icao, iata, name, country, country_code, continent, iso_region, municipality,
        location, lat, lon, type, elevation_ft, icao_code, iata_code, gps_code, local_code,
        home_link, _loaded_at, FALSE AS is_existing
      FROM incoming
    ),
    scd AS (
      SELECT *, LEAD(_loaded_at) OVER (PARTITION BY icao ORDER BY _loaded_at) AS next_loaded_at
      FROM combined
    )
    SELECT
      md5(
        concat_ws(
          '||',
          icao,
          CAST(_loaded_at AS STRING)
        )
      ) AS airport_sk,
      icao, iata, name, country, country_code, continent, iso_region, municipality,
      location, lat, lon, type, elevation_ft, icao_code, iata_code, gps_code, local_code, home_link,
      _loaded_at AS effective_start,
      next_loaded_at AS effective_end,
      next_loaded_at IS NULL AS is_current
    FROM scd
    WHERE NOT is_existing
  ) AS source
  ON target.airport_sk = source.airport_sk
  WHEN NOT MATCHED THEN INSERT (
    airport_sk, icao, iata, name, country, country_code, continent, iso_region,
    municipality, location, lat, lon, type, elevation_ft, icao_code, iata_code, gps_code,
    local_code, home_link, effective_start, effective_end, is_current
  ) VALUES (
    source.airport_sk, source.icao, source.iata, source.name, source.country,
    source.country_code, source.continent, source.iso_region, source.municipality, source.location,
    source.lat, source.lon, source.type, source.elevation_ft, source.icao_code, source.iata_code,
    source.gps_code, source.local_code, source.home_link, source.effective_start, source.effective_end,
    source.is_current
  );
END;
