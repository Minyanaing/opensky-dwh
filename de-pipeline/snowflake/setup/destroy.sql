-- Drops only the tables setup.sql creates - database, schema, file format, and stage are left
-- alone. Destructive - mirrors de-pipeline/databricks/setup/destroy.sql.
DROP TABLE IF EXISTS OPENSKY_DB.BRONZE.FLIGHTS_RAW;
DROP TABLE IF EXISTS OPENSKY_DB.BRONZE.CALLSIGNS;
DROP TABLE IF EXISTS OPENSKY_DB.BRONZE.AIRLINES;
DROP TABLE IF EXISTS OPENSKY_DB.BRONZE.AIRPORTS;
DROP TABLE IF EXISTS OPENSKY_DB.BRONZE.AIRCRAFTS;
