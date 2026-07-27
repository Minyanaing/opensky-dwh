-- {{CATALOG}} substituted per environment. flight_key hashes the match key so MERGE compares one
-- column instead of 5; it excludes movement_type on purpose, which collapses a flight's departure-
-- and arrival-queried rows into one - QUALIFY picks a deterministic survivor (MERGE itself won't
-- dedupe unmatched source rows). year/month/day stay plain columns, not GENERATED, since
-- MERGE ... INSERT * requires the source to supply every target column.
--
-- Requires Databricks Runtime 16.3+ for SQL scripting (BEGIN...END/IF) - unconfirmed on Free
-- Edition's serverless warehouse. Must run as ONE statement, not split on internal ';' -
-- env_setup.py's _statements() special-cases a file starting with BEGIN for this reason.
BEGIN
  DECLARE row_count BIGINT DEFAULT 0;
  SET row_count = (SELECT COUNT(*) FROM {{CATALOG}}.silver_flights.flights);

  IF row_count = 0 THEN
    -- Bootstrap: target is empty, load every day currently in bronze at once.
    MERGE INTO {{CATALOG}}.silver_flights.flights AS target
    USING (
      SELECT
        md5(
          concat_ws(
            '||',
            icao24,
            callsign,
            estDepartureAirport,
            estArrivalAirport,
            CAST(date(from_unixtime(firstSeen)) AS STRING),
            CAST(date(from_unixtime(lastSeen)) AS STRING)
          )
        ) AS flight_key,
        date(fetched_at) AS flight_date,
        year(date(fetched_at)) AS year,
        month(date(fetched_at)) AS month,
        day(date(fetched_at)) AS day,
        CAST(from_unixtime(firstSeen) AS TIMESTAMP) AS departure,
        CAST(from_unixtime(lastSeen) AS TIMESTAMP) AS arrival,
        icao24,
        callsign,
        estDepartureAirport,
        estArrivalAirport,
        movement_type,
        _loaded_at AS _loaded_at_raw,
        current_timestamp() AS _loaded_at
      FROM opensky_raw.bronze.flights_raw
      WHERE estArrivalAirport IS NOT NULL
        AND estDepartureAirport IS NOT NULL
        AND estDepartureAirport != estArrivalAirport
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
          icao24, callsign, estDepartureAirport, estArrivalAirport,
          date(from_unixtime(firstSeen)), date(from_unixtime(lastSeen))
        ORDER BY movement_type
      ) = 1
    ) AS source
    ON target.flight_key = source.flight_key
    WHEN NOT MATCHED THEN INSERT *;
  ELSE
    -- Incremental: target already has data, only the newest bronze batch needs merging in.
    MERGE INTO {{CATALOG}}.silver_flights.flights AS target
    USING (
      SELECT
        md5(
          concat_ws(
            '||',
            icao24,
            callsign,
            estDepartureAirport,
            estArrivalAirport,
            CAST(date(from_unixtime(firstSeen)) AS STRING),
            CAST(date(from_unixtime(lastSeen)) AS STRING)
          )
        ) AS flight_key,
        date(fetched_at) AS flight_date,
        year(date(fetched_at)) AS year,
        month(date(fetched_at)) AS month,
        day(date(fetched_at)) AS day,
        CAST(from_unixtime(firstSeen) AS TIMESTAMP) AS departure,
        CAST(from_unixtime(lastSeen) AS TIMESTAMP) AS arrival,
        icao24,
        callsign,
        estDepartureAirport,
        estArrivalAirport,
        movement_type,
        _loaded_at AS _loaded_at_raw,
        current_timestamp() AS _loaded_at
      FROM opensky_raw.bronze.flights_raw
      WHERE estArrivalAirport IS NOT NULL
        AND estDepartureAirport IS NOT NULL
        AND estDepartureAirport != estArrivalAirport
        AND _loaded_at = (SELECT MAX(_loaded_at) FROM opensky_raw.bronze.flights_raw)
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
          icao24, callsign, estDepartureAirport, estArrivalAirport,
          date(from_unixtime(firstSeen)), date(from_unixtime(lastSeen))
        ORDER BY movement_type
      ) = 1
    ) AS source
    ON target.flight_key = source.flight_key
    WHEN NOT MATCHED THEN INSERT *;
  END IF;
END;
