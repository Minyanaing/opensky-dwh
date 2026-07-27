-- {{CATALOG}} substituted per environment. aircraft_key hashes every column except the loaded_at
-- audit columns - a changed value (registration, owner, etc.) gets its own new row instead of
-- overwriting the old one, so this table keeps history.
BEGIN
  DECLARE row_count BIGINT DEFAULT 0;
  SET row_count = (SELECT COUNT(*) FROM {{CATALOG}}.silver_flights.aircrafts);

  IF row_count = 0 THEN
    -- Bootstrap: full bronze history can repeat the same icao24 across days with identical data -
    -- QUALIFY collapses those to one row per aircraft_key (lossless, since a matching key means
    -- matching content); a changed value hashes differently and survives as its own row.
    MERGE INTO {{CATALOG}}.silver_flights.aircrafts AS target
    USING (
      SELECT
        md5(
          concat_ws(
            '||',
            icao24, type, icao_type, manufacturer,
            registration, registered_owner, registered_owner_country
          )
        ) AS aircraft_key,
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
        AND type IS NOT NULL
        AND icao_type IS NOT NULL
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY md5(
          concat_ws(
            '||',
            icao24, type, icao_type, manufacturer,
            registration, registered_owner, registered_owner_country
          )
        )
        ORDER BY _loaded_at DESC
      ) = 1
    ) AS source
    ON target.aircraft_key = source.aircraft_key
    WHEN NOT MATCHED THEN INSERT *;
  ELSE
    -- Incremental: bronze aircrafts are already deduped by icao24 within one run
    -- (ingest_adsbdb.py --aircraft), so aircraft_key is already unique within a single batch - no
    -- QUALIFY needed.
    MERGE INTO {{CATALOG}}.silver_flights.aircrafts AS target
    USING (
      SELECT
        md5(
          concat_ws(
            '||',
            icao24, type, icao_type, manufacturer,
            registration, registered_owner, registered_owner_country
          )
        ) AS aircraft_key,
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
        AND type IS NOT NULL
        AND icao_type IS NOT NULL
        AND _loaded_at = (SELECT MAX(_loaded_at) FROM opensky_raw.bronze.aircrafts)
    ) AS source
    ON target.aircraft_key = source.aircraft_key
    WHEN NOT MATCHED THEN INSERT *;
  END IF;
END;

-- Sample resulting rows for one icao24 after its registered_owner changes between loads - same
-- icao24/type/icao_type/manufacturer/registration/registered_owner_country, only
-- registered_owner differs, so it lands as a second row with a new aircraft_key. This is the
-- shape gold's SCD2 dims should expect as input (icao24 is the natural key; every distinct
-- aircraft_key for that icao24 is a version to track, ordered by _loaded_at):
--
-- aircraft_key | icao24  | registration | registered_owner  | _loaded_at
-- -------------|---------|--------------|-------------------|-------------------
-- 5e1f8a2c...  | 7823bc  | HS-ABC       | Thai Airways      | 2026-07-01 09:08:00
-- b04d9e71...  | 7823bc  | HS-ABC       | Bangkok Airways   | 2026-07-15 09:08:00
