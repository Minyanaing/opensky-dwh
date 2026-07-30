-- {{CATALOG}} substituted per environment by env_setup.py --catalog. Drops the tables created by
-- tables_gold.sql - schema/catalog untouched.
DROP TABLE IF EXISTS {{CATALOG}}.gold_flights.fct_flight_movement;
DROP TABLE IF EXISTS {{CATALOG}}.gold_flights.dim_airline;
DROP TABLE IF EXISTS {{CATALOG}}.gold_flights.dim_aircraft;
DROP TABLE IF EXISTS {{CATALOG}}.gold_flights.dim_airport;
DROP TABLE IF EXISTS {{CATALOG}}.gold_flights.dim_callsign;
