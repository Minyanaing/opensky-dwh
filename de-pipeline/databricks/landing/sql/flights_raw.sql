TRUNCATE TABLE opensky_raw.bronze.flights_raw;

COPY INTO opensky_raw.bronze.flights_raw
FROM (
  SELECT
    TRIM(icao24) AS icao24,
    TRIM(callsign) AS callsign,
    TRIM(estDepartureAirport) AS estDepartureAirport,
    TRIM(estArrivalAirport) AS estArrivalAirport,
    CAST(firstSeen AS BIGINT) AS firstSeen,
    CAST(lastSeen AS BIGINT) AS lastSeen,
    CAST(estDepartureAirportHorizDistance AS BIGINT) AS estDepartureAirportHorizDistance,
    CAST(estDepartureAirportVertDistance AS BIGINT) AS estDepartureAirportVertDistance,
    CAST(estArrivalAirportHorizDistance AS BIGINT) AS estArrivalAirportHorizDistance,
    CAST(estArrivalAirportVertDistance AS BIGINT) AS estArrivalAirportVertDistance,
    CAST(departureAirportCandidatesCount AS BIGINT) AS departureAirportCandidatesCount,
    CAST(arrivalAirportCandidatesCount AS BIGINT) AS arrivalAirportCandidatesCount,
    TRIM(queried_airport) AS queried_airport,
    TRIM(movement_type) AS movement_type,
    TRIM(fetched_at) AS fetched_at,
    current_timestamp() AS _loaded_at
  FROM '/Volumes/opensky_raw/bronze/landing/flights_raw/'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'nullValue' = '');
