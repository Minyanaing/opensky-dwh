-- {{CATALOG}} substituted per environment. Requires Databricks Runtime 16.3+ (BEGIN...END/DECLARE).
-- Lookback: dim_aircraft empty -> cutoff = epoch (all of silver); otherwise cutoff = MAX(_loaded_at)
-- (only the latest silver batch) - silver is content-hash deduped, so anything at that batch is
-- guaranteed genuinely new/changed content, never a re-appearance of unchanged data.
-- temp_dim_aircraft_changes: silver_latest (ROW_NUMBER-deduped to one row per icao24, preventing
-- join fan-out) LEFT/INNER-joined to gold's is_current rows on icao24. status = UPDATE for an
-- existing current row that must close, NEW for the version to insert - an icao24 with an
-- existing current row gets both; a brand-new icao24 gets only NEW.
BEGIN
  DECLARE row_count BIGINT DEFAULT 0;
  DECLARE cutoff TIMESTAMP;
  DECLARE open_ended_end TIMESTAMP DEFAULT TIMESTAMP('9999-12-12 00:00:00');

  SET row_count = (SELECT COUNT(*) FROM {{CATALOG}}.gold_flights.dim_aircraft);

  IF row_count = 0 THEN
    SET cutoff = TIMESTAMP('1970-01-01 00:00:00');
  ELSE
    SET cutoff = (SELECT MAX(_loaded_at) FROM {{CATALOG}}.silver_flights.aircrafts);
  END IF;

  CREATE OR REPLACE TEMPORARY VIEW temp_dim_aircraft_changes AS
  WITH silver_latest AS (
    SELECT icao24, type, icao_type, manufacturer, registration, registered_owner,
      registered_owner_country, _loaded_at
    FROM {{CATALOG}}.silver_flights.aircrafts
    WHERE _loaded_at >= cutoff
    QUALIFY ROW_NUMBER() OVER (PARTITION BY icao24 ORDER BY _loaded_at DESC) = 1
  ),
  gold_current AS (
    SELECT aircraft_sk, icao24
    FROM {{CATALOG}}.gold_flights.dim_aircraft
    WHERE is_current
  ),
  updates AS (
    SELECT
      'UPDATE' AS status,
      g.aircraft_sk,
      CAST(NULL AS STRING) AS icao24, CAST(NULL AS STRING) AS type, CAST(NULL AS STRING) AS icao_type,
      CAST(NULL AS STRING) AS manufacturer, CAST(NULL AS STRING) AS registration,
      CAST(NULL AS STRING) AS registered_owner, CAST(NULL AS STRING) AS registered_owner_country,
      CAST(NULL AS TIMESTAMP) AS effective_start,
      s._loaded_at AS effective_end,
      FALSE AS is_current
    FROM silver_latest AS s
    JOIN gold_current AS g ON g.icao24 = s.icao24
  ),
  inserts AS (
    SELECT
      'NEW' AS status,
      md5(concat_ws('||', icao24, CAST(_loaded_at AS STRING))) AS aircraft_sk,
      icao24, type, icao_type, manufacturer, registration, registered_owner, registered_owner_country,
      _loaded_at AS effective_start,
      open_ended_end AS effective_end,
      TRUE AS is_current
    FROM silver_latest
  )
  SELECT * FROM updates
  UNION ALL
  SELECT * FROM inserts;

  MERGE INTO {{CATALOG}}.gold_flights.dim_aircraft AS target
  USING temp_dim_aircraft_changes AS source
  ON target.aircraft_sk = source.aircraft_sk
  WHEN MATCHED AND source.status = 'UPDATE' THEN
    UPDATE SET
      target.effective_end = source.effective_end,
      target.is_current = false
  WHEN NOT MATCHED AND source.status = 'NEW' THEN
    INSERT (
      aircraft_sk, icao24, type, icao_type, manufacturer, registration,
      registered_owner, registered_owner_country, effective_start, effective_end, is_current
    ) VALUES (
      source.aircraft_sk, source.icao24, source.type, source.icao_type,
      source.manufacturer, source.registration, source.registered_owner, source.registered_owner_country,
      source.effective_start, source.effective_end, source.is_current
    );

  DROP VIEW IF EXISTS temp_dim_aircraft_changes;
END;
