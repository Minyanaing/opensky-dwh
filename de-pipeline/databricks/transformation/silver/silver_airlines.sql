-- {{CATALOG}} substituted per environment. icao alone is the merge key - see silver_flights.sql
-- for why this needs to run as ONE statement (BEGIN...END, Databricks Runtime 16.3+).
BEGIN
  DECLARE row_count BIGINT DEFAULT 0;
  SET row_count = (SELECT COUNT(*) FROM {{CATALOG}}.silver_flights.airlines);

  IF row_count = 0 THEN
    -- Bootstrap: full bronze history can repeat the same icao across days (each day's batch is
    -- already deduped by ingest_adsbdb.py, but that guarantee doesn't hold across batches) -
    -- QUALIFY picks the newest row per icao.
    MERGE INTO {{CATALOG}}.silver_flights.airlines AS target
    USING (
      SELECT
        icao,
        iata,
        name,
        country,
        callsign,
        _loaded_at AS _loaded_at_raw,
        current_timestamp() AS _loaded_at
      FROM opensky_raw.bronze.airlines
      WHERE icao IS NOT NULL
      QUALIFY ROW_NUMBER() OVER (PARTITION BY icao ORDER BY _loaded_at DESC) = 1
    ) AS source
    ON target.icao = source.icao
    WHEN NOT MATCHED THEN INSERT *;
  ELSE
    -- Incremental: bronze airlines are already deduped by icao within one run (ingest_adsbdb.py),
    -- so no QUALIFY is needed against a single batch.
    MERGE INTO {{CATALOG}}.silver_flights.airlines AS target
    USING (
      SELECT
        icao,
        iata,
        name,
        country,
        callsign,
        _loaded_at AS _loaded_at_raw,
        current_timestamp() AS _loaded_at
      FROM opensky_raw.bronze.airlines
      WHERE icao IS NOT NULL
        AND _loaded_at = (SELECT MAX(_loaded_at) FROM opensky_raw.bronze.airlines)
    ) AS source
    ON target.icao = source.icao
    WHEN NOT MATCHED THEN INSERT *;
  END IF;
END;
