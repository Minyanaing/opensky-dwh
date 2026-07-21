"""Manually load an already-exported flights_raw.json / callsigns.csv / airports.csv into
opensky_raw.bronze - for data that was fetched and saved earlier (e.g. ingest_opensky.py in
local mode, or any other one-off export), without re-fetching from the OpenSky API.

Reuses load_to_databricks.py's Databricks-write functions (insert_flights/insert_new_callsigns/
insert_new_airports) rather than duplicating them - this script only adds file reading,
per-file validation, and a clearer manual CLI/summary on top.

One file loads to one table, fully, before the next file starts: each file is read only when
its own turn comes up (not all three upfront), and each file's rows are inserted into its table
in a single call - not chunked into batches. A failure partway through leaves earlier files
already landed, but a given file's own insert is one all-or-nothing call.
"""

import argparse
import csv
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import config
from load_to_databricks import get_connection, insert_flights, insert_new_airports, insert_new_callsigns

logger = logging.getLogger(__name__)


def _require_file(path):
    if not path.is_file():
        raise FileNotFoundError(f"{path} not found - check the path, or export the data first")
    return path


def read_flights(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def read_column(path, column):
    with open(path, newline="", encoding="utf-8") as f:
        return [row[column] for row in csv.DictReader(f) if row.get(column)]


def parse_args():
    default_dir = config.OUTPUT_DIR
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--flights-json",
        type=str,
        default=str(default_dir / "flights_raw.json"),
        help="Path to the raw flights JSON export (default: %(default)s)",
    )
    parser.add_argument(
        "--callsigns-csv",
        type=str,
        default=str(default_dir / "callsigns.csv"),
        help="Path to the distinct-callsigns CSV export (default: %(default)s)",
    )
    parser.add_argument(
        "--airports-csv",
        type=str,
        default=str(default_dir / "airports.csv"),
        help="Path to the distinct-airports CSV export (default: %(default)s)",
    )
    return parser.parse_args()


def _load_file_to_table(cursor, table, path, reader, writer, loaded_at):
    """Read one file completely, then insert it into its one target table in a single call -
    fully, before the caller moves on to the next file."""
    values = reader(path)
    logger.info("Read %s record(s) from %s", len(values), path)
    landed = writer(cursor, values, loaded_at)
    logger.info("%s: %s row(s) landed", table, len(landed))
    return landed


def run(flights_json_path, callsigns_csv_path, airports_csv_path):
    flights_path = _require_file(Path(flights_json_path))
    callsigns_path = _require_file(Path(callsigns_csv_path))
    airports_path = _require_file(Path(airports_csv_path))

    loaded_at = datetime.now(timezone.utc)

    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            flights = _load_file_to_table(
                cursor, "flights_raw", flights_path, read_flights, insert_flights, loaded_at
            )
            new_callsigns = _load_file_to_table(
                cursor,
                "callsigns",
                callsigns_path,
                lambda p: read_column(p, "callsign"),
                insert_new_callsigns,
                loaded_at,
            )
            new_airports = _load_file_to_table(
                cursor,
                "airports",
                airports_path,
                lambda p: read_column(p, "icao"),
                insert_new_airports,
                loaded_at,
            )
    finally:
        connection.close()

    logger.info(
        "Landed in opensky_raw.bronze: %s flight row(s), %s new callsign(s), %s new airport(s)",
        len(flights),
        len(new_callsigns),
        len(new_airports),
    )
    return {"flights": len(flights), "new_callsigns": new_callsigns, "new_airports": new_airports}


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    # databricks-sql-connector logs every HTTP request/response at INFO - drowns out our own
    # per-file progress lines. Keep our logging at INFO, quiet just this library.
    logging.getLogger("databricks").setLevel(logging.WARNING)
    args = parse_args()
    run(args.flights_json, args.callsigns_csv, args.airports_csv)


if __name__ == "__main__":
    main()
