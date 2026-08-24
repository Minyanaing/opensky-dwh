-- {{CATALOG}} substituted per environment. Requires Databricks Runtime 16.3+ (BEGIN...END/DECLARE).
-- airline_sk replaces airline_icao/airline_iata - resolved via a point-in-time join to
-- dim_airline (is_earliest lets a callsign predating dim_airline's first version still match),
-- frozen at insert time like every other column here, never re-resolved for an existing row.
-- v_cutoff = epoch if empty, else silver's latest batch only (content-hash deduped, so always new/changed).
-- Hash = silver's callsign_key columns with airline_icao/iata swapped for the resolved airline_sk; concat_ws skips NULLs so no COALESCE needed.
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
    WITH silver_latest AS (
      SELECT callsign, callsign_icao, callsign_iata, airline_icao, origin_icao, origin_iata,
        destination_icao, destination_iata, _loaded_at
      FROM {{CATALOG}}.silver_flights.callsigns
      WHERE _loaded_at >= v_cutoff
      QUALIFY ROW_NUMBER() OVER (PARTITION BY callsign ORDER BY _loaded_at DESC) = 1
    ),
    airline_ranged AS (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY icao ORDER BY effective_start) = 1 AS is_earliest
      FROM {{CATALOG}}.gold_flights.dim_airline
    )
    SELECT
      s.callsign,
      s._loaded_at,
      md5(
        concat_ws(
          '||',
          s.callsign, s.callsign_icao, s.callsign_iata, CAST(al.airline_sk AS STRING),
          s.origin_icao, s.origin_iata, s.destination_icao, s.destination_iata
        )
      ) AS content_hash
    FROM silver_latest AS s
    LEFT JOIN airline_ranged AS al
      ON al.icao = s.airline_icao
      AND (
        (s._loaded_at >= al.effective_start AND (al.effective_end IS NULL OR s._loaded_at < al.effective_end))
        OR (al.is_earliest AND s._loaded_at < al.effective_start)
      )
  ) AS source
  ON target.callsign = source.callsign
    AND target.is_current
  WHEN MATCHED AND md5(
    concat_ws(
      '||',
      target.callsign, target.callsign_icao, target.callsign_iata, 
      CAST(target.airline_sk AS STRING),
      target.origin_icao, target.origin_iata, 
      target.destination_icao, target.destination_iata
    )
  ) != source.content_hash THEN
    UPDATE SET
      target.effective_end = source._loaded_at,
      target.is_current = false;

  INSERT INTO {{CATALOG}}.gold_flights.dim_callsign (
    callsign_sk, callsign, callsign_icao, callsign_iata, airline_sk,
    origin_icao, origin_iata, destination_icao, destination_iata,
    effective_start, effective_end, is_current
  )
  SELECT
    md5(concat_ws('||', c.callsign, CAST(c._loaded_at AS STRING))) AS callsign_sk,
    c.callsign, c.callsign_icao, c.callsign_iata, c.airline_sk,
    c.origin_icao, c.origin_iata, c.destination_icao, c.destination_iata,
    c._loaded_at AS effective_start,
    TIMESTAMP('9999-12-12 00:00:00') AS effective_end,
    TRUE AS is_current
  FROM (
    WITH silver_latest AS (
      SELECT callsign, callsign_icao, callsign_iata, airline_icao, origin_icao, origin_iata,
        destination_icao, destination_iata, _loaded_at
      FROM {{CATALOG}}.silver_flights.callsigns
      WHERE _loaded_at >= v_cutoff
      QUALIFY ROW_NUMBER() OVER (PARTITION BY callsign ORDER BY _loaded_at DESC) = 1
    ),
    airline_ranged AS (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY icao ORDER BY effective_start) = 1 AS is_earliest
      FROM {{CATALOG}}.gold_flights.dim_airline
    )
    SELECT
      s.callsign, s.callsign_icao, s.callsign_iata, al.airline_sk,
      s.origin_icao, s.origin_iata, s.destination_icao, s.destination_iata, s._loaded_at,
      md5(
        concat_ws(
          '||',
          s.callsign, s.callsign_icao, s.callsign_iata, CAST(al.airline_sk AS STRING),
          s.origin_icao, s.origin_iata, s.destination_icao, s.destination_iata
        )
      ) AS content_hash
    FROM silver_latest AS s
    LEFT JOIN airline_ranged AS al
      ON al.icao = s.airline_icao
      AND (
        (s._loaded_at >= al.effective_start AND (al.effective_end IS NULL OR s._loaded_at < al.effective_end))
        OR (al.is_earliest AND s._loaded_at < al.effective_start)
      )
  ) AS c
  LEFT JOIN {{CATALOG}}.gold_flights.dim_callsign AS g
    ON g.callsign = c.callsign
    AND g.is_current
    AND md5(
      concat_ws(
        '||',
        g.callsign, g.callsign_icao, g.callsign_iata, CAST(g.airline_sk AS STRING),
        g.origin_icao, g.origin_iata, g.destination_icao, g.destination_iata
      )
    ) = c.content_hash
  WHERE g.callsign IS NULL;
END;
