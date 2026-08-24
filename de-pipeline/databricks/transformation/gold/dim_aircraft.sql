-- {{CATALOG}} substituted per environment. Requires Databricks Runtime 16.3+ (BEGIN...END/DECLARE).
-- v_cutoff = epoch if empty, else silver's latest batch only (content-hash deduped, so always new/changed).
-- Target's hash is recomputed at runtime (gold doesn't persist aircraft_key) vs source.aircraft_key; concat_ws skips NULLs so no COALESCE needed.
BEGIN
  DECLARE v_row_count BIGINT DEFAULT 0;
  DECLARE v_cutoff TIMESTAMP;

  SET v_row_count = (SELECT COUNT(*) FROM {{CATALOG}}.gold_flights.dim_aircraft);

  IF v_row_count = 0 THEN
    SET v_cutoff = TIMESTAMP('1970-01-01 00:00:00');
  ELSE
    SET v_cutoff = (SELECT MAX(_loaded_at) FROM {{CATALOG}}.silver_flights.aircrafts);
  END IF;

  MERGE INTO {{CATALOG}}.gold_flights.dim_aircraft AS target
  USING (
    SELECT icao24, type, icao_type, manufacturer, registration, registered_owner,
      registered_owner_country, aircraft_key, _loaded_at
    FROM {{CATALOG}}.silver_flights.aircrafts
    WHERE _loaded_at >= v_cutoff
    QUALIFY ROW_NUMBER() OVER (PARTITION BY icao24 ORDER BY _loaded_at DESC) = 1
  ) AS source
  ON target.icao24 = source.icao24
    AND target.is_current
  WHEN MATCHED AND md5(
    concat_ws(
      '||',
      target.icao24, target.type, target.icao_type, target.manufacturer,
      target.registration, target.registered_owner, target.registered_owner_country
    )
  ) != source.aircraft_key THEN
    UPDATE SET
      target.effective_end = source._loaded_at,
      target.is_current = false;

  INSERT INTO {{CATALOG}}.gold_flights.dim_aircraft (
    aircraft_sk, icao24, type, icao_type, manufacturer, registration,
    registered_owner, registered_owner_country, effective_start, effective_end, is_current
  )
  SELECT
    md5(concat_ws('||', s.icao24, CAST(s._loaded_at AS STRING))) AS aircraft_sk,
    s.icao24, s.type, s.icao_type, s.manufacturer, s.registration, s.registered_owner,
    s.registered_owner_country,
    s._loaded_at AS effective_start,
    TIMESTAMP('9999-12-12 00:00:00') AS effective_end,
    TRUE AS is_current
  FROM (
    SELECT icao24, type, icao_type, manufacturer, registration, registered_owner,
      registered_owner_country, aircraft_key, _loaded_at
    FROM {{CATALOG}}.silver_flights.aircrafts
    WHERE _loaded_at >= v_cutoff
    QUALIFY ROW_NUMBER() OVER (PARTITION BY icao24 ORDER BY _loaded_at DESC) = 1
  ) AS s
  LEFT JOIN {{CATALOG}}.gold_flights.dim_aircraft AS g
    ON g.icao24 = s.icao24
    AND g.is_current
    AND md5(
      concat_ws(
        '||',
        g.icao24, g.type, g.icao_type, g.manufacturer,
        g.registration, g.registered_owner, g.registered_owner_country
      )
    ) = s.aircraft_key
  WHERE g.icao24 IS NULL;
END;
