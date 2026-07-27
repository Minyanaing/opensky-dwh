-- {{CATALOG}} substituted per environment. Requires Databricks Runtime 16.3+ for SQL scripting
-- (BEGIN...END/DECLARE) - must run as ONE statement (env_setup.py's _statements() special-cases
-- a file starting with BEGIN for this reason).
-- FLAG: lookback_days assumes a version is never missed for that long; a gap wider than that would
-- silently skip it. Revisit if needed.
BEGIN
  DECLARE lookback_days INT DEFAULT 5;
  DECLARE cutoff TIMESTAMP;
  SET cutoff = (
    SELECT MAX(_loaded_at) FROM {{CATALOG}}.silver_flights.aircrafts
  ) - make_interval(0, 0, 0, lookback_days, 0, 0, 0);

  MERGE INTO {{CATALOG}}.gold_flights.dim_aircraft AS target
  USING (
    WITH incoming AS (
      SELECT 
        DISTINCT icao24, type, icao_type, manufacturer, registration, registered_owner,
        registered_owner_country, _loaded_at
      FROM {{CATALOG}}.silver_flights.aircrafts
      WHERE _loaded_at >= cutoff
    ),
    combined AS (
      SELECT icao24, effective_start AS _loaded_at FROM {{CATALOG}}.gold_flights.dim_aircraft WHERE is_current
      UNION ALL
      SELECT icao24, _loaded_at FROM incoming
    )
    SELECT icao24, _loaded_at AS effective_start, LEAD(_loaded_at) OVER (PARTITION BY icao24 ORDER BY _loaded_at) AS next_loaded_at
    FROM combined
  ) AS source
  ON target.icao24 = source.icao24
    AND target.effective_start = source.effective_start
    AND target.is_current
    AND source.next_loaded_at IS NOT NULL
  WHEN MATCHED THEN UPDATE SET target.effective_end = source.next_loaded_at, target.is_current = false;

  MERGE INTO {{CATALOG}}.gold_flights.dim_aircraft AS target
  USING (
    WITH incoming AS (
      SELECT DISTINCT icao24, type, icao_type, manufacturer, registration, registered_owner,
        registered_owner_country, _loaded_at
      FROM {{CATALOG}}.silver_flights.aircrafts
      WHERE _loaded_at >= cutoff
    ),
    combined AS (
      SELECT icao24, type, icao_type, manufacturer, registration, registered_owner,
        registered_owner_country, effective_start AS _loaded_at, TRUE AS is_existing
      FROM {{CATALOG}}.gold_flights.dim_aircraft WHERE is_current
      UNION ALL
      SELECT icao24, type, icao_type, manufacturer, registration, registered_owner,
        registered_owner_country, _loaded_at, FALSE AS is_existing
      FROM incoming
    ),
    scd AS (
      SELECT *, LEAD(_loaded_at) OVER (PARTITION BY icao24 ORDER BY _loaded_at) AS next_loaded_at
      FROM combined
    )
    SELECT
      md5(concat_ws('||', icao24, CAST(_loaded_at AS STRING))) AS aircraft_sk,
      icao24, type, icao_type, manufacturer, registration, registered_owner, registered_owner_country,
      _loaded_at AS effective_start,
      next_loaded_at AS effective_end,
      next_loaded_at IS NULL AS is_current
    FROM scd
    WHERE NOT is_existing
  ) AS source
  ON target.aircraft_sk = source.aircraft_sk
  WHEN NOT MATCHED THEN INSERT *;
END;
