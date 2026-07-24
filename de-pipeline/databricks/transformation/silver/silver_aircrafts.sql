-- {{CATALOG}} substituted per environment. icao24 alone is the merge key - see silver_flights.sql
-- for why this needs to run as ONE statement (BEGIN...END, Databricks Runtime 16.3+).
BEGIN
  DECLARE row_count BIGINT DEFAULT 0;
  SET row_count = (SELECT COUNT(*) FROM {{CATALOG}}.silver_flights.aircrafts);

  IF row_count = 0 THEN
    -- Bootstrap: full bronze history can repeat the same icao24 across days - QUALIFY picks the
    -- newest row per icao24.
    MERGE INTO {{CATALOG}}.silver_flights.aircrafts AS target
    USING (
      SELECT
        icao24,
        type,
        icao_type,
        manufacturer,
        registration,
        registered_owner,
        registered_owner_country,
        _loaded_at AS _loaded_at_raw,
        current_timestamp() AS _loaded_at
      FROM opensky_raw.bronze.aircrafts
      WHERE icao24 IS NOT NULL
      QUALIFY ROW_NUMBER() OVER (PARTITION BY icao24 ORDER BY _loaded_at DESC) = 1
    ) AS source
    ON target.icao24 = source.icao24
    WHEN NOT MATCHED THEN INSERT *;
  ELSE
    -- Incremental: only the newest bronze batch needs merging in.
    MERGE INTO {{CATALOG}}.silver_flights.aircrafts AS target
    USING (
      SELECT
        icao24,
        type,
        icao_type,
        manufacturer,
        registration,
        registered_owner,
        registered_owner_country,
        _loaded_at AS _loaded_at_raw,
        current_timestamp() AS _loaded_at
      FROM opensky_raw.bronze.aircrafts
      WHERE icao24 IS NOT NULL
        AND _loaded_at = (SELECT MAX(_loaded_at) FROM opensky_raw.bronze.aircrafts)
    ) AS source
    ON target.icao24 = source.icao24
    WHEN NOT MATCHED THEN INSERT *;
  END IF;
END;
