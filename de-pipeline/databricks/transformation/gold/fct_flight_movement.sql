-- {{CATALOG}} substituted per environment. Must run after the dim_*.sql rebuilds (no
-- execution-order enforcement of its own).
-- FLAG: FKs resolve against each dim's is_current row, not point-in-time as of departure.
-- FLAG: a gap wider than 3 days silently skips the update.
MERGE INTO {{CATALOG}}.gold_flights.fct_flight_movement AS target
USING (
  SELECT
    f.flight_key,
    CAST(date_format(f.departure, 'yyyyMMdd') AS INT) AS date_sk,
    hour(f.departure) * 100 + minute(f.departure) AS time_sk,
    ac.aircraft_sk,
    cs.callsign_sk,
    orig.airport_sk AS origin_airport_sk,
    dest.airport_sk AS destination_airport_sk,
    f.departure,
    f.arrival,
    f.movement_type,
    CAST((unix_timestamp(f.arrival) - unix_timestamp(f.departure)) / 60 AS INT) AS flight_duration_minutes,
    f._loaded_at
  FROM {{CATALOG}}.silver_flights.flights AS f
  LEFT JOIN {{CATALOG}}.gold_flights.dim_aircraft AS ac ON ac.icao24 = f.icao24 AND ac.is_current
  LEFT JOIN {{CATALOG}}.gold_flights.dim_callsign AS cs ON cs.callsign = f.callsign AND cs.is_current
  LEFT JOIN {{CATALOG}}.gold_flights.dim_airport AS orig ON orig.icao = f.estDepartureAirport AND orig.is_current
  LEFT JOIN {{CATALOG}}.gold_flights.dim_airport AS dest ON dest.icao = f.estArrivalAirport AND dest.is_current
  WHERE f._loaded_at >= current_date() - INTERVAL 3 DAYS
) AS source
ON target.flight_key = source.flight_key
WHEN MATCHED AND (target.departure != source.departure OR target.arrival != source.arrival)
  THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
