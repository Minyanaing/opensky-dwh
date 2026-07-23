-- Drops only the tables setup.sql creates - catalog, schema, and the landing volume are left
-- alone. Destructive - run only via infra-destroy.yml (workflow_dispatch, never on merge).
DROP TABLE IF EXISTS opensky_raw.bronze.flights_raw;
DROP TABLE IF EXISTS opensky_raw.bronze.callsigns;
DROP TABLE IF EXISTS opensky_raw.bronze.airlines;
DROP TABLE IF EXISTS opensky_raw.bronze.airports;
DROP TABLE IF EXISTS opensky_raw.bronze.aircrafts;
DROP TABLE IF EXISTS opensky_raw.bronze.airport_data;
DROP TABLE IF EXISTS opensky_raw.bronze.airports_master;
