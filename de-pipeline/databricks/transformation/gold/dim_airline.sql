-- {{CATALOG}} substituted per environment. Incremental: only silver rows from the last 3 days, not
-- full history. Two statements - close out the row a new version supersedes, then insert the new
-- version(s); order matters (close-out must run first).
-- FLAG: 3-day lookback assumes a version is never missed for 3+ days; a gap wider than that would
-- silently skip it. Revisit if needed.

MERGE INTO {{CATALOG}}.gold_flights.dim_airline AS target
USING (
  WITH incoming AS (
    SELECT DISTINCT icao, iata, name, country, callsign, _loaded_at
    FROM {{CATALOG}}.silver_flights.airlines
    WHERE _loaded_at >= current_date() - INTERVAL 3 DAYS
  ),
  combined AS (
    SELECT icao, effective_start AS _loaded_at FROM {{CATALOG}}.gold_flights.dim_airline WHERE is_current
    UNION ALL
    SELECT icao, _loaded_at FROM incoming
  )
  SELECT icao, _loaded_at AS effective_start, LEAD(_loaded_at) OVER (PARTITION BY icao ORDER BY _loaded_at) AS next_loaded_at
  FROM combined
) AS source
ON target.icao = source.icao
  AND target.effective_start = source.effective_start
  AND target.is_current
  AND source.next_loaded_at IS NOT NULL
WHEN MATCHED THEN UPDATE SET target.effective_end = source.next_loaded_at, target.is_current = false;

MERGE INTO {{CATALOG}}.gold_flights.dim_airline AS target
USING (
  WITH incoming AS (
    SELECT DISTINCT icao, iata, name, country, callsign, _loaded_at
    FROM {{CATALOG}}.silver_flights.airlines
    WHERE _loaded_at >= current_date() - INTERVAL 3 DAYS
  ),
  combined AS (
    SELECT icao, iata, name, country, callsign, effective_start AS _loaded_at, TRUE AS is_existing
    FROM {{CATALOG}}.gold_flights.dim_airline WHERE is_current
    UNION ALL
    SELECT icao, iata, name, country, callsign, _loaded_at, FALSE AS is_existing FROM incoming
  ),
  scd AS (
    SELECT *, LEAD(_loaded_at) OVER (PARTITION BY icao ORDER BY _loaded_at) AS next_loaded_at
    FROM combined
  )
  SELECT
    md5(concat_ws('||', icao, CAST(_loaded_at AS STRING))) AS airline_sk,
    icao, iata, name, country, callsign,
    _loaded_at AS effective_start,
    next_loaded_at AS effective_end,
    next_loaded_at IS NULL AS is_current
  FROM scd
  WHERE NOT is_existing
) AS source
ON target.airline_sk = source.airline_sk
WHEN NOT MATCHED THEN INSERT *;
