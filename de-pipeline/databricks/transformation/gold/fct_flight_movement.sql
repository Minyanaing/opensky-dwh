-- {{CATALOG}} substituted per environment. Requires Databricks Runtime 16.3+ (BEGIN...END/DECLARE).
-- FLAG: a gap wider than lookback_days silently skips a reschedule.
-- FKs join point-in-time (effective_start/effective_end, not is_current); is_earliest lets a flight predating a dim's first version still match.
-- f.callsign is TRIM()'d - OpenSky's raw callsign is space-padded, dim_callsign's isn't.
-- A changed flight_key soft-deletes the old row + inserts a RESCHEDULE row; latest_gold ignores is_deleted since flipping it doesn't change _loaded_at.
BEGIN
  DECLARE lookback_days INT DEFAULT 3;
  DECLARE row_count BIGINT DEFAULT 0;
  DECLARE cutoff TIMESTAMP;

  SET row_count = (SELECT COUNT(*) FROM {{CATALOG}}.gold_flights.fct_flight_movement);

  IF row_count = 0 THEN
    SET cutoff = TIMESTAMP('1970-01-01 00:00:00');
  ELSE
    SET cutoff = (
      SELECT MAX(_loaded_at) FROM {{CATALOG}}.silver_flights.flights
    ) - make_interval(0, 0, 0, lookback_days, 0, 0, 0);
  END IF;

  MERGE INTO {{CATALOG}}.gold_flights.fct_flight_movement AS target
  USING (
    WITH latest_gold AS (
      SELECT flight_key, departure, arrival, _loaded_at
      FROM {{CATALOG}}.gold_flights.fct_flight_movement
      WHERE f._loaded_at >= cutoff
      QUALIFY ROW_NUMBER() OVER (PARTITION BY flight_key ORDER BY _loaded_at DESC) = 1
    )
    SELECT g.flight_key, g._loaded_at AS target_loaded_at
    FROM latest_gold AS g
    JOIN {{CATALOG}}.silver_flights.flights AS f ON f.flight_key = g.flight_key
    WHERE f._loaded_at >= cutoff
      AND (f.departure != g.departure OR f.arrival != g.arrival)
  ) AS source
  ON target.flight_key = source.flight_key
    AND target._loaded_at = source.target_loaded_at
    AND NOT target.is_deleted
  WHEN MATCHED THEN UPDATE SET target.is_deleted = true;

  MERGE INTO {{CATALOG}}.gold_flights.fct_flight_movement AS target
  USING (
    WITH latest_gold AS (
      SELECT flight_key, departure, arrival
      FROM {{CATALOG}}.gold_flights.fct_flight_movement
      WHERE f._loaded_at >= cutoff
      QUALIFY ROW_NUMBER() OVER (PARTITION BY flight_key ORDER BY _loaded_at DESC) = 1
    ),
    aircraft_ranged AS (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY icao24 ORDER BY effective_start) = 1 AS is_earliest
      FROM {{CATALOG}}.gold_flights.dim_aircraft
    ),
    callsign_ranged AS (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY callsign ORDER BY effective_start) = 1 AS is_earliest
      FROM {{CATALOG}}.gold_flights.dim_callsign
    ),
    airport_ranged AS (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY icao ORDER BY effective_start) = 1 AS is_earliest
      FROM {{CATALOG}}.gold_flights.dim_airport
    )
    SELECT
      md5(
        concat_ws(
          '||',
          f.flight_key,
          CAST(f._loaded_at AS STRING)
        )
      ) AS flights_sk,
      f.flight_key,
      CAST(date_format(f.departure, 'yyyyMMdd') AS INT) AS departure_date_sk,
      hour(f.departure) * 100 + minute(f.departure) AS departure_time_sk,
      CAST(date_format(f.arrival, 'yyyyMMdd') AS INT) AS arrival_date_sk,
      hour(f.arrival) * 100 + minute(f.arrival) AS arrival_time_sk,
      ac.aircraft_sk,
      cs.callsign_sk,
      orig.airport_sk AS origin_airport_sk,
      dest.airport_sk AS destination_airport_sk,
      f.departure,
      f.arrival,
      f.movement_type,
      CAST((unix_timestamp(f.arrival) - unix_timestamp(f.departure)) / 60 AS INT) AS flight_duration_minutes,
      CASE WHEN g.flight_key IS NULL THEN 'ORIGINAL' ELSE 'RESCHEDULE' END AS record_type,
      FALSE AS is_deleted,
      f._loaded_at
    FROM {{CATALOG}}.silver_flights.flights AS f
    LEFT JOIN latest_gold AS g ON g.flight_key = f.flight_key
    LEFT JOIN aircraft_ranged AS ac
      ON ac.icao24 = f.icao24
      AND (
        (f.departure >= ac.effective_start AND (ac.effective_end IS NULL OR f.departure < ac.effective_end))
        OR (ac.is_earliest AND f.departure < ac.effective_start)
      )
    LEFT JOIN callsign_ranged AS cs
      ON cs.callsign = f.callsign
      AND (
        (f.departure >= cs.effective_start AND (cs.effective_end IS NULL OR f.departure < cs.effective_end))
        OR (cs.is_earliest AND f.departure < cs.effective_start)
      )
    LEFT JOIN airport_ranged AS orig
      ON orig.icao = f.estDepartureAirport
      AND (
        (f.departure >= orig.effective_start AND (orig.effective_end IS NULL OR f.departure < orig.effective_end))
        OR (orig.is_earliest AND f.departure < orig.effective_start)
      )
    LEFT JOIN airport_ranged AS dest
      ON dest.icao = f.estArrivalAirport
      AND (
        (f.departure >= dest.effective_start AND (dest.effective_end IS NULL OR f.departure < dest.effective_end))
        OR (dest.is_earliest AND f.departure < dest.effective_start)
      )
    WHERE f._loaded_at >= cutoff
      AND (g.flight_key IS NULL OR g.departure != f.departure OR g.arrival != f.arrival)
  ) AS source
  ON target.flights_sk = source.flights_sk
  WHEN NOT MATCHED THEN INSERT *;
END;
