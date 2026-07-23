COPY INTO opensky_raw.bronze.flights_raw
FROM (
  SELECT
    icao24,
    callsign,
    estDepartureAirport,
    estArrivalAirport,
    CAST(firstSeen AS BIGINT) AS firstSeen,
    CAST(lastSeen AS BIGINT) AS lastSeen,
    CAST(estDepartureAirportHorizDistance AS BIGINT) AS estDepartureAirportHorizDistance,
    CAST(estDepartureAirportVertDistance AS BIGINT) AS estDepartureAirportVertDistance,
    CAST(estArrivalAirportHorizDistance AS BIGINT) AS estArrivalAirportHorizDistance,
    CAST(estArrivalAirportVertDistance AS BIGINT) AS estArrivalAirportVertDistance,
    CAST(departureAirportCandidatesCount AS BIGINT) AS departureAirportCandidatesCount,
    CAST(arrivalAirportCandidatesCount AS BIGINT) AS arrivalAirportCandidatesCount,
    queried_airport,
    movement_type,
    fetched_at,
    current_timestamp() AS _loaded_at
  FROM '/Volumes/opensky_raw/bronze/landing/flights_raw/'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'nullValue' = '');
