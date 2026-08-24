-- {{CATALOG}} substituted per environment. Requires Databricks Runtime 16.3+ (BEGIN...END/DECLARE).
-- Lookback: dim_callsign empty -> v_cutoff = epoch (all of silver); otherwise v_cutoff =
-- MAX(_loaded_at) (only the latest silver batch) - silver is content-hash deduped, so anything
-- at that batch is guaranteed genuinely new/changed content. ROW_NUMBER dedupes to one row per
-- callsign (defensive - also prevents join fan-out below).
-- gold doesn't persist callsign_key, so target's hash is recomputed at runtime with the same
-- formula silver used (callsign + all 8 route columns, see silver_callsigns.sql) and compared
-- directly against source.callsign_key. concat_ws's null-skipping means a field going from NULL
-- to a real value still changes the resulting hash, no COALESCE needed.
-- Two statements: MERGE closes an existing current row when its hash changed (unchanged content
-- simply matches no WHEN clause - a no-op); INSERT adds the fresh row for anything not already
-- reflected as an unchanged current row - covers both a brand-new callsign and a change.
BEGIN
  DECLARE v_row_count BIGINT DEFAULT 0;
  DECLARE v_cutoff TIMESTAMP;

  SET v_row_count = (SELECT COUNT(*) FROM {{CATALOG}}.gold_flights.dim_callsign);

  IF v_row_count = 0 THEN
    SET v_cutoff = TIMESTAMP('1970-01-01 00:00:00');
  ELSE
    SET v_cutoff = (SELECT MAX(_loaded_at) FROM {{CATALOG}}.silver_flights.callsigns);
  END IF;

  MERGE INTO {{CATALOG}}.gold_flights.dim_callsign AS target
  USING (
    SELECT callsign, callsign_icao, callsign_iata, airline_icao, airline_iata,
      origin_icao, origin_iata, destination_icao, destination_iata, callsign_key, _loaded_at
    FROM {{CATALOG}}.silver_flights.callsigns
    WHERE _loaded_at >= v_cutoff
    QUALIFY ROW_NUMBER() OVER (PARTITION BY callsign ORDER BY _loaded_at DESC) = 1
  ) AS source
  ON target.callsign = source.callsign
    AND target.is_current
  WHEN MATCHED AND md5(
    concat_ws(
      '||',
      target.callsign, target.callsign_icao, target.callsign_iata, target.airline_icao,
      target.airline_iata, target.origin_icao, target.origin_iata, target.destination_icao,
      target.destination_iata
    )
  ) != source.callsign_key THEN
    UPDATE SET
      target.effective_end = source._loaded_at,
      target.is_current = false;

  INSERT INTO {{CATALOG}}.gold_flights.dim_callsign (
    callsign_sk, callsign, callsign_icao, callsign_iata, airline_icao, airline_iata,
    origin_icao, origin_iata, destination_icao, destination_iata,
    effective_start, effective_end, is_current
  )
  SELECT
    md5(concat_ws('||', s.callsign, CAST(s._loaded_at AS STRING))) AS callsign_sk,
    s.callsign, s.callsign_icao, s.callsign_iata, s.airline_icao, s.airline_iata,
    s.origin_icao, s.origin_iata, s.destination_icao, s.destination_iata,
    s._loaded_at AS effective_start,
    TIMESTAMP('9999-12-12 00:00:00') AS effective_end,
    TRUE AS is_current
  FROM (
    SELECT callsign, callsign_icao, callsign_iata, airline_icao, airline_iata,
      origin_icao, origin_iata, destination_icao, destination_iata, callsign_key, _loaded_at
    FROM {{CATALOG}}.silver_flights.callsigns
    WHERE _loaded_at >= v_cutoff
    QUALIFY ROW_NUMBER() OVER (PARTITION BY callsign ORDER BY _loaded_at DESC) = 1
  ) AS s
  LEFT JOIN {{CATALOG}}.gold_flights.dim_callsign AS g
    ON g.callsign = s.callsign
    AND g.is_current
    AND md5(
      concat_ws(
        '||',
        g.callsign, g.callsign_icao, g.callsign_iata, g.airline_icao, g.airline_iata,
        g.origin_icao, g.origin_iata, g.destination_icao, g.destination_iata
      )
    ) = s.callsign_key
  WHERE g.callsign IS NULL;
END;
