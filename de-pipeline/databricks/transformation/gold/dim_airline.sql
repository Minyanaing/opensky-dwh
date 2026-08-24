-- {{CATALOG}} substituted per environment. Requires Databricks Runtime 16.3+ (BEGIN...END/DECLARE).
-- Lookback: dim_airline empty -> v_cutoff = epoch (all of silver); otherwise v_cutoff =
-- MAX(_loaded_at) (only the latest silver batch) - silver is content-hash deduped, so anything
-- at that batch is guaranteed genuinely new/changed content. ROW_NUMBER dedupes to one row per
-- icao (defensive - also prevents join fan-out below).
-- gold doesn't persist airline_key, so target's hash is recomputed at runtime with the same
-- formula silver used, and compared directly against source.airline_key (already computed by
-- silver) - one comparison instead of separate per-column checks, and concat_ws's null-skipping
-- means a field going from NULL to a real value still changes the resulting hash, no COALESCE
-- needed.
-- Two statements: MERGE closes an existing current row when its hash changed (unchanged content
-- simply matches no WHEN clause - a no-op); INSERT adds the fresh row for anything not already
-- reflected as an unchanged current row - covers both a brand-new icao and a change.
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
