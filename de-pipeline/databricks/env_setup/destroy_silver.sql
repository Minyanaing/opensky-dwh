-- {{CATALOG}} substituted per environment by env_setup.py --catalog. Drops the tables created by
-- tables_silver.sql - schema/catalog untouched.
DROP TABLE IF EXISTS {{CATALOG}}.silver_flights.flights;
DROP TABLE IF EXISTS {{CATALOG}}.silver_flights.airlines;
DROP TABLE IF EXISTS {{CATALOG}}.silver_flights.aircrafts;
DROP TABLE IF EXISTS {{CATALOG}}.silver_flights.callsigns;
DROP TABLE IF EXISTS {{CATALOG}}.silver_flights.airports;
