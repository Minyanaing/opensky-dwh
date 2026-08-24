-- {{CATALOG}} substituted per environment. airline_key hashes every column except the loaded_at
-- audit columns - a changed value (name, country, etc.) gets its own new row on the next merge
-- instead of overwriting the old one, so this table keeps history rather than losing it.
BEGIN
  DECLARE row_count BIGINT DEFAULT 0;
  SET row_count = (SELECT COUNT(*) FROM {{CATALOG}}.silver_flights.airlines);

  IF row_count = 0 THEN
    -- Bootstrap: full bronze history can repeat the same icao across days with identical data -
    -- QUALIFY collapses those to one row per airline_key (lossless, since a matching key means
    -- matching content); a changed value hashes differently and survives as its own row.
    MERGE INTO {{CATALOG}}.silver_flights.airlines AS target
    USING (
      SELECT
        md5(
          concat_ws(
            '||', 
            icao, 
            iata, 
            name, 
            country, 
            callsign
          )
        ) AS airline_key,
        icao,
        iata,
        name,
        country,
        callsign,
        _loaded_at AS _loaded_at_raw,
        current_timestamp() AS _loaded_at
      FROM opensky_raw.bronze.airlines
      WHERE icao IS NOT NULL
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY md5(concat_ws('||', icao, iata, name, country, callsign))
        ORDER BY _loaded_at DESC
      ) = 1
    ) AS source
    ON target.airline_key = source.airline_key
    WHEN NOT MATCHED THEN INSERT *;
  ELSE
    -- Incremental: bronze airlines are already deduped by icao within one run (ingest_adsbdb.py),
    -- so airline_key is already unique within a single batch - no QUALIFY needed.
    MERGE INTO {{CATALOG}}.silver_flights.airlines AS target
    USING (
      SELECT
        md5(
          concat_ws(
            '||', 
            icao, 
            iata, 
            name, 
            country, 
            callsign
          )
        ) AS airline_key,
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
    ON target.airline_key = source.airline_key
    WHEN NOT MATCHED THEN INSERT *;
  END IF;
END;

-- ###################################################################################################
-- Sample resulting rows for one icao after its name changes between loads - same icao/iata/
-- country/callsign, only name differs, so it lands as a second row with a new airline_key. This
-- is the shape gold's SCD2 dim_airline should expect as input (icao is the natural key; every
-- distinct airline_key for that icao is a version to track, ordered by _loaded_at):
--
-- airline_key | icao | iata | name                    | country | callsign | _loaded_at
-- ------------|------|------|-------------------------|---------|----------|-------------------
-- 3f2a9c1e... | CSN  | CZ   | China Southern          | China   | CSN      | 2026-07-01 09:08:00
-- 7b8d4f02... | CSN  | CZ   | China Southern Airlines | China   | CSN      | 2026-07-15 09:08:00
-- ###################################################################################################