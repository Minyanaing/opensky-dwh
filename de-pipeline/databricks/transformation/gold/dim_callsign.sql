-- {{CATALOG}} substituted per environment. Requires Databricks Runtime 16.3+ (BEGIN...END/DECLARE).
-- FLAG: a gap wider than lookback_days silently skips a version.
-- Two MERGEs, order matters (close-out first, then insert). Chains gold's current row + all
-- incoming silver rows via LEAD, so every version in the window is tracked, not just the latest.
BEGIN
  DECLARE lookback_days INT DEFAULT 3;
  DECLARE row_count BIGINT DEFAULT 0;
  DECLARE cutoff TIMESTAMP;

  SET row_count = (SELECT COUNT(*) FROM {{CATALOG}}.gold_flights.dim_callsign);

  IF row_count = 0 THEN
    SET cutoff = TIMESTAMP('1970-01-01 00:00:00');
  ELSE
    SET cutoff = (
      SELECT MAX(_loaded_at) FROM {{CATALOG}}.silver_flights.callsigns
    ) - make_interval(0, 0, 0, lookback_days, 0, 0, 0);
  END IF;

  MERGE INTO {{CATALOG}}.gold_flights.dim_callsign AS target
  USING (
    WITH incoming AS (
      SELECT callsign, _loaded_at
      FROM {{CATALOG}}.silver_flights.callsigns
      WHERE _loaded_at >= cutoff
      QUALIFY ROW_NUMBER() OVER (PARTITION BY callsign_key ORDER BY _loaded_at) = 1
    ),
    combined AS (
      SELECT callsign, effective_start AS _loaded_at
      FROM {{CATALOG}}.gold_flights.dim_callsign WHERE is_current
      UNION ALL
      SELECT callsign, _loaded_at FROM incoming
    )
    SELECT
      callsign,
      _loaded_at AS effective_start,
      LEAD(_loaded_at) OVER (PARTITION BY callsign ORDER BY _loaded_at) AS next_loaded_at
    FROM combined
  ) AS source
  ON target.callsign = source.callsign
    AND target.effective_start = source.effective_start
    AND target.is_current
    AND source.next_loaded_at IS NOT NULL
  WHEN MATCHED THEN
    UPDATE SET
      target.effective_end = source.next_loaded_at,
      target.is_current = false;

  MERGE INTO {{CATALOG}}.gold_flights.dim_callsign AS target
  USING (
    WITH incoming AS (
      SELECT callsign, callsign_icao, callsign_iata, airline_icao, airline_iata,
        origin_icao, origin_iata, destination_icao, destination_iata, callsign_key, _loaded_at
      FROM {{CATALOG}}.silver_flights.callsigns
      WHERE _loaded_at >= cutoff
      QUALIFY ROW_NUMBER() OVER (PARTITION BY callsign_key ORDER BY _loaded_at) = 1
    ),
    combined AS (
      SELECT callsign, callsign_icao, callsign_iata, airline_icao, airline_iata,
        origin_icao, origin_iata, destination_icao, destination_iata,
        effective_start AS _loaded_at, TRUE AS is_existing
      FROM {{CATALOG}}.gold_flights.dim_callsign WHERE is_current
      UNION ALL
      SELECT callsign, callsign_icao, callsign_iata, airline_icao, airline_iata,
        origin_icao, origin_iata, destination_icao, destination_iata,
        _loaded_at, FALSE AS is_existing
      FROM incoming
    ),
    scd AS (
      SELECT *, LEAD(_loaded_at) OVER (PARTITION BY callsign ORDER BY _loaded_at) AS next_loaded_at
      FROM combined
    )
    SELECT
      md5(
        concat_ws(
          '||',
          callsign,
          CAST(_loaded_at AS STRING)
        )
      ) AS callsign_sk,
      callsign, callsign_icao, callsign_iata, airline_icao, airline_iata,
      origin_icao, origin_iata, destination_icao, destination_iata,
      _loaded_at AS effective_start,
      next_loaded_at AS effective_end,
      next_loaded_at IS NULL AS is_current
    FROM scd
    WHERE NOT is_existing
  ) AS source
  ON target.callsign_sk = source.callsign_sk
  WHEN NOT MATCHED THEN INSERT (
    callsign_sk, callsign, callsign_icao, callsign_iata, airline_icao, airline_iata,
    origin_icao, origin_iata, destination_icao, destination_iata, effective_start, effective_end, is_current
  ) VALUES (
    source.callsign_sk, source.callsign, source.callsign_icao, source.callsign_iata,
    source.airline_icao, source.airline_iata, source.origin_icao, source.origin_iata,
    source.destination_icao, source.destination_iata, source.effective_start, source.effective_end, source.is_current
  );
END;
