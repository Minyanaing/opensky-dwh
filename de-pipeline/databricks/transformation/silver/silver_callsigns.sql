-- {{CATALOG}} substituted per environment. callsign_key hashes callsign/callsign_icao/
-- callsign_iata/airline_icao/airline_iata/origin_icao/origin_iata/destination_icao/
-- destination_iata - a changed value (e.g. a route change) gets its own new row instead of
-- overwriting the old one, so this table keeps history. year/month/day partition by ingestion
-- date, not a per-row flight date.
BEGIN
  DECLARE row_count BIGINT DEFAULT 0;
  SET row_count = (SELECT COUNT(*) FROM {{CATALOG}}.silver_flights.callsigns);

  IF row_count = 0 THEN
    -- Bootstrap: full bronze history can repeat the same callsign across days with identical
    -- data - QUALIFY collapses those to one row per callsign_key (lossless, since a matching key
    -- means matching content); a changed value hashes differently and survives as its own row.
    MERGE INTO {{CATALOG}}.silver_flights.callsigns AS target
    USING (
      SELECT
        md5(
          concat_ws(
            '||',
            callsign, callsign_icao, callsign_iata,
            airline_icao, airline_iata,
            origin_icao, origin_iata,
            destination_icao, destination_iata
          )
        ) AS callsign_key,
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
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY md5(
          concat_ws(
            '||',
            callsign, callsign_icao, callsign_iata,
            airline_icao, airline_iata,
            origin_icao, origin_iata,
            destination_icao, destination_iata
          )
        )
        ORDER BY _loaded_at DESC
      ) = 1
    ) AS source
    ON target.callsign_key = source.callsign_key
    WHEN NOT MATCHED THEN INSERT *;
  ELSE
    -- Incremental: only the newest bronze batch needs merging in.
    MERGE INTO {{CATALOG}}.silver_flights.callsigns AS target
    USING (
      SELECT
        md5(
          concat_ws(
            '||',
            callsign, callsign_icao, callsign_iata,
            airline_icao, airline_iata,
            origin_icao, origin_iata,
            destination_icao, destination_iata
          )
        ) AS callsign_key,
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
    ON target.callsign_key = source.callsign_key
    WHEN NOT MATCHED THEN INSERT *;
  END IF;
END;

-- Sample resulting rows for one callsign after its destination changes between loads - same
-- callsign/callsign_icao/callsign_iata/airline_icao/airline_iata/origin_icao/origin_iata, only
-- destination differs, so it lands as a second row with a new callsign_key. This is the shape
-- gold's SCD2 dims should expect as input (callsign is the natural key; every distinct
-- callsign_key for that callsign is a version to track, ordered by _loaded_at):
--
-- callsign_key | callsign | origin_icao | destination_icao | _loaded_at
-- -------------|----------|-------------|------------------|-------------------
-- a1c4e7f0...  | CSN312   | ZGGG        | VTBS             | 2026-07-01 09:08:00
-- d92b6a13...  | CSN312   | ZGGG        | WSSS             | 2026-07-15 09:08:00
