"""Databricks write functions for opensky_raw.bronze - used directly, in-process, by
ingest_opensky.py (INGEST_MODE=databricks), and via this file's CLI to (re)load a previous
local export. flights_raw is append-only; callsigns/airports are append-only-*if-new*, stamped
with that run's _loaded_at (see insert_new_callsigns/insert_new_airports)."""

import argparse
import csv
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from databricks import sql
from databricks.sdk.core import Config, oauth_service_principal

import config

logger = logging.getLogger(__name__)

CATALOG = "opensky_raw"
SCHEMA = "bronze"

FLIGHT_COLUMNS = [
    "icao24",
    "callsign",
    "estDepartureAirport",
    "estArrivalAirport",
    "firstSeen",
    "lastSeen",
    "estDepartureAirportHorizDistance",
    "estDepartureAirportVertDistance",
    "estArrivalAirportHorizDistance",
    "estArrivalAirportVertDistance",
    "departureAirportCandidatesCount",
    "arrivalAirportCandidatesCount",
    "queried_airport",
    "movement_type",
    "fetched_at",
]


def _require_env(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is not set")
    return value


def get_connection():
    server_hostname = _require_env("DATABRICKS_HOST")
    http_path = _require_env("DATABRICKS_HTTP_PATH")

    token = os.environ.get("DATABRICKS_TOKEN")
    if token:
        logger.info("Authenticating with a personal access token (fallback)")
        return sql.connect(server_hostname=server_hostname, http_path=http_path, access_token=token)

    client_id = os.environ.get("DATABRICKS_CLIENT_ID")
    client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET")
    if client_id and client_secret:
        logger.info("Authenticating as a service principal (OAuth M2M)")

        def credential_provider():
            cfg = Config(host=f"https://{server_hostname}", client_id=client_id, client_secret=client_secret)
            return oauth_service_principal(cfg)

        return sql.connect(
            server_hostname=server_hostname, http_path=http_path, credentials_provider=credential_provider
        )

    raise RuntimeError(
        "Set DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET (service principal, preferred) "
        "or DATABRICKS_TOKEN (personal access token fallback)."
    )


def _insert_rows(cursor, table, columns, rows):
    if not rows:
        logger.info("No new row(s) for %s, skipping", table)
        return
    column_list = ", ".join(f"`{c}`" for c in columns)
    placeholders = ", ".join(["?"] * len(columns))
    statement = f"INSERT INTO {CATALOG}.{SCHEMA}.{table} ({column_list}) VALUES ({placeholders})"
    cursor.executemany(statement, rows)
    logger.info("Inserted %s row(s) into %s.%s.%s", len(rows), CATALOG, SCHEMA, table)


def _existing_values(cursor, table, column):
    cursor.execute(f"SELECT DISTINCT `{column}` FROM {CATALOG}.{SCHEMA}.{table}")
    return {row[0] for row in cursor.fetchall()}


def insert_flights(cursor, flights, loaded_at):
    """Append every flight record - flights_raw is an event log, every row is genuinely new."""
    rows = [tuple(flight.get(col) for col in FLIGHT_COLUMNS) + (loaded_at,) for flight in flights]
    _insert_rows(cursor, "flights_raw", FLIGHT_COLUMNS + ["_loaded_at"], rows)
    return flights


def insert_new_callsigns(cursor, callsigns, loaded_at):
    """Insert only callsigns not already present, stamped with this run's loaded_at."""
    existing = _existing_values(cursor, "callsigns", "callsign")
    new_values = sorted({c for c in callsigns if c} - existing)
    _insert_rows(cursor, "callsigns", ["callsign", "_loaded_at"], [(v, loaded_at) for v in new_values])
    return new_values


def insert_new_airports(cursor, airports, loaded_at):
    """Insert only airport codes not already present, stamped with this run's loaded_at."""
    existing = _existing_values(cursor, "airports", "icao")
    new_values = sorted({a for a in airports if a} - existing)
    _insert_rows(cursor, "airports", ["icao", "_loaded_at"], [(v, loaded_at) for v in new_values])
    return new_values


def land_in_databricks(flights, callsigns, airports):
    """Open one connection, insert all three tables, close it - the INGEST_MODE=databricks path."""
    loaded_at = datetime.now(timezone.utc)
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            insert_flights(cursor, flights, loaded_at)
            new_callsigns = insert_new_callsigns(cursor, callsigns, loaded_at)
            new_airports = insert_new_airports(cursor, airports, loaded_at)
    finally:
        connection.close()

    logger.info(
        "Landed directly in Databricks: %s flight row(s), %s new callsign(s), %s new airport(s)",
        len(flights),
        len(new_callsigns),
        len(new_airports),
    )
    return {"flights": len(flights), "new_callsigns": new_callsigns, "new_airports": new_airports}


# --- CLI: load a previous local-mode ingest_opensky.py run's exported files ---


def _read_flights(flights_json_path):
    with open(flights_json_path, encoding="utf-8") as f:
        return json.load(f)


def _read_column(csv_path, column):
    with open(csv_path, newline="", encoding="utf-8") as f:
        return [row[column] for row in csv.DictReader(f) if row.get(column)]


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-dir",
        type=str,
        default=str(config.OUTPUT_DIR),
        help="Directory holding ingest_opensky.py's flights_raw.json/callsigns.csv/airports.csv "
        "(default: %(default)s)",
    )
    return parser.parse_args()


def run(input_dir):
    in_dir = Path(input_dir)
    flights = _read_flights(in_dir / "flights_raw.json")
    callsigns = _read_column(in_dir / "callsigns.csv", "callsign")
    airports = _read_column(in_dir / "airports.csv", "icao")
    return land_in_databricks(flights, callsigns, airports)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    run(args.input_dir)


if __name__ == "__main__":
    main()
