-- {{CATALOG}} substituted per environment. Requires Databricks Runtime 16.3+ (BEGIN...END/DECLARE).
-- v_cutoff = epoch if empty, else silver's latest batch only (content-hash deduped, so always new/changed).
-- Target's hash is recomputed at runtime (gold doesn't persist airline_key) vs source.airline_key; concat_ws skips NULLs so no COALESCE needed.
BEGIN
  DECLARE v_row_count BIGINT DEFAULT 0;
  DECLARE v_cutoff TIMESTAMP;

  SET v_row_count = (SELECT COUNT(*) FROM {{CATALOG}}.gold_flights.dim_airline);

  IF v_row_count = 0 THEN
    SET v_cutoff = TIMESTAMP('1970-01-01 00:00:00');
  ELSE
    SET v_cutoff = (SELECT MAX(_loaded_at) FROM {{CATALOG}}.silver_flights.airlines);
  END IF;

  MERGE INTO {{CATALOG}}.gold_flights.dim_airline AS target
  USING (
    SELECT icao, iata, name, country, callsign, airline_key, _loaded_at
    FROM {{CATALOG}}.silver_flights.airlines
    WHERE _loaded_at >= v_cutoff
    QUALIFY ROW_NUMBER() OVER (PARTITION BY icao ORDER BY _loaded_at DESC) = 1
  ) AS source
  ON target.icao = source.icao
    AND target.is_current
  WHEN MATCHED AND md5(
    concat_ws(
      '||',
      target.icao, target.iata, target.name, target.country, target.callsign
    )
  ) != source.airline_key THEN
    UPDATE SET
      target.effective_end = source._loaded_at,
      target.is_current = false;

  INSERT INTO {{CATALOG}}.gold_flights.dim_airline (
    airline_sk, icao, iata, name, country, callsign, effective_start, effective_end, is_current
  )
  SELECT
    md5(concat_ws('||', s.icao, CAST(s._loaded_at AS STRING))) AS airline_sk,
    s.icao, s.iata, s.name, s.country, s.callsign,
    s._loaded_at AS effective_start,
    TIMESTAMP('9999-12-12 00:00:00') AS effective_end,
    TRUE AS is_current
  FROM (
    SELECT icao, iata, name, country, callsign, airline_key, _loaded_at
    FROM {{CATALOG}}.silver_flights.airlines
    WHERE _loaded_at >= v_cutoff
    QUALIFY ROW_NUMBER() OVER (PARTITION BY icao ORDER BY _loaded_at DESC) = 1
  ) AS s
  LEFT JOIN {{CATALOG}}.gold_flights.dim_airline AS g
    ON g.icao = s.icao
    AND g.is_current
    AND md5(
      concat_ws(
        '||',
        g.icao, g.iata, g.name, g.country, g.callsign
      )
    ) = s.airline_key
  WHERE g.icao IS NULL;
END;
