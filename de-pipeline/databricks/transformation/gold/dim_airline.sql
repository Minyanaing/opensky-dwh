-- {{CATALOG}} substituted per environment. Requires Databricks Runtime 16.3+ (BEGIN...END/DECLARE).
-- FLAG: a gap wider than lookback_days silently skips a version.
-- Two MERGEs, order matters (close-out first, then insert). Chains gold's current row + all
-- incoming silver rows via LEAD, so every version in the window is tracked, not just the latest.
BEGIN
  DECLARE lookback_days INT DEFAULT 3;
  DECLARE row_count BIGINT DEFAULT 0;
  DECLARE cutoff TIMESTAMP;
  DECLARE open_ended_end TIMESTAMP DEFAULT TIMESTAMP('9999-12-12 00:00:00');

  SET row_count = (SELECT COUNT(*) FROM {{CATALOG}}.gold_flights.dim_airline);

  IF row_count = 0 THEN
    SET cutoff = TIMESTAMP('1970-01-01 00:00:00');
  ELSE
    SET cutoff = (
      SELECT MAX(_loaded_at) FROM {{CATALOG}}.silver_flights.airlines
    ) - make_interval(0, 0, 0, lookback_days, 0, 0, 0);
  END IF;

  MERGE INTO {{CATALOG}}.gold_flights.dim_airline AS target
  USING (
    WITH incoming AS (
      SELECT icao, _loaded_at
      FROM {{CATALOG}}.silver_flights.airlines
      WHERE _loaded_at >= cutoff
      QUALIFY ROW_NUMBER() OVER (PARTITION BY airline_key ORDER BY _loaded_at) = 1
    ),
    combined AS (
      SELECT icao, effective_start AS _loaded_at
      FROM {{CATALOG}}.gold_flights.dim_airline WHERE is_current
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

  MERGE INTO {{CATALOG}}.gold_flights.dim_airline AS target
  USING (
    WITH incoming AS (
      SELECT icao, iata, name, country, callsign, airline_key, _loaded_at
      FROM {{CATALOG}}.silver_flights.airlines
      WHERE _loaded_at >= cutoff
      QUALIFY ROW_NUMBER() OVER (PARTITION BY airline_key ORDER BY _loaded_at) = 1
    ),
    combined AS (
      SELECT icao, iata, name, country, callsign, effective_start AS _loaded_at, TRUE AS is_existing
      FROM {{CATALOG}}.gold_flights.dim_airline WHERE is_current
      UNION ALL
      SELECT icao, iata, name, country, callsign, _loaded_at, FALSE AS is_existing
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
      ) AS airline_sk,
      icao, iata, name, country, callsign,
      _loaded_at AS effective_start,
      COALESCE(next_loaded_at, open_ended_end) AS effective_end,
      next_loaded_at IS NULL AS is_current
    FROM scd
    WHERE NOT is_existing
  ) AS source
  ON target.airline_sk = source.airline_sk
  WHEN NOT MATCHED THEN INSERT (
    airline_sk, icao, iata, name, country, callsign, effective_start, effective_end, is_current
  ) VALUES (
    source.airline_sk, source.icao, source.iata, source.name, source.country,
    source.callsign, source.effective_start, source.effective_end, source.is_current
  );
END;
