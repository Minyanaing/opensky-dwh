-- {{CATALOG}} substituted per environment. callsign alone is the merge key - see
-- silver_flights.sql for why this needs to run as ONE statement (BEGIN...END, Databricks Runtime
-- 16.3+). year/month/day partition by ingestion date, not a per-row flight date.
BEGIN
  DECLARE row_count BIGINT DEFAULT 0;
  SET row_count = (SELECT COUNT(*) FROM {{CATALOG}}.silver_flights.callsigns);

  IF row_count = 0 THEN
    -- Bootstrap: full bronze history can repeat the same callsign across days - QUALIFY picks the
    -- newest row per callsign.
    MERGE INTO {{CATALOG}}.silver_flights.callsigns AS target
    USING (
      SELECT
        date(fetched_at) AS flight_date,
        year(date(fetched_at)) AS year,
        month(date(fetched_at)) AS month,
        day(date(fetched_at)) AS day,
        callsign,
        callsign_icao,
        callsign_iata,
        airline_icao,
        airline_iata,
        origin_icao,
        origin_iata,
        destination_icao,
        destination_iata,
        _loaded_at AS _loaded_at_raw,
        current_timestamp() AS _loaded_at
      FROM opensky_raw.bronze.callsigns
      WHERE callsign_icao IS NOT NULL
      QUALIFY ROW_NUMBER() OVER (PARTITION BY callsign ORDER BY _loaded_at DESC) = 1
    ) AS source
    ON target.callsign = source.callsign
    WHEN NOT MATCHED THEN INSERT *;
  ELSE
    -- Incremental: only the newest bronze batch needs merging in.
    MERGE INTO {{CATALOG}}.silver_flights.callsigns AS target
    USING (
      SELECT
        date(fetched_at) AS flight_date,
        year(date(fetched_at)) AS year,
        month(date(fetched_at)) AS month,
        day(date(fetched_at)) AS day,
        callsign,
        callsign_icao,
        callsign_iata,
        airline_icao,
        airline_iata,
        origin_icao,
        origin_iata,
        destination_icao,
        destination_iata,
        _loaded_at AS _loaded_at_raw,
        current_timestamp() AS _loaded_at
      FROM opensky_raw.bronze.callsigns
      WHERE callsign_icao IS NOT NULL
        AND _loaded_at = (SELECT MAX(_loaded_at) FROM opensky_raw.bronze.callsigns)
    ) AS source
    ON target.callsign = source.callsign
    WHEN NOT MATCHED THEN INSERT *;
  END IF;
END;
