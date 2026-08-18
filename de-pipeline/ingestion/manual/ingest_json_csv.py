"""Rebuild data/callsigns.csv and data/airports.csv from data/flights_raw.csv - all distinct
values currently in flights_raw.csv, not the new-since-last-run delta ingest_opensky.py tracks.

- callsigns.csv - distinct values of the callsign column.
- airports.csv  - distinct values across both estDepartureAirport and estArrivalAirport columns.

Overwrites the same files ingest_opensky.py maintains. Use this to force both back in sync with
the current flights_raw.csv - at the cost of losing ingest_opensky.py's new-only tracking until
its next run rebuilds the delta from scratch.
"""

import argparse
import csv
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "common"))

import config
from transforms import distinct_airports, distinct_callsigns

logger = logging.getLogger(__name__)


def _require_file(path):
    if not path.is_file():
        raise FileNotFoundError(f"{path} not found - check the path, or export the data first")
    return path


def read_flights_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_column_csv(path, header, values):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([header])
        for value in values:
            writer.writerow([value])


def parse_args():
    default_dir = config.OUTPUT_DIR
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--flights-csv",
        type=str,
        default=str(default_dir / "flights_raw.csv"),
        help="Path to the flights_raw.csv to rebuild from (default: %(default)s)",
    )
    parser.add_argument(
        "--callsigns-csv",
        type=str,
        default=str(default_dir / "callsigns.csv"),
        help="Path to overwrite with distinct callsigns (default: %(default)s)",
    )
    parser.add_argument(
        "--airports-csv",
        type=str,
        default=str(default_dir / "airports.csv"),
        help="Path to overwrite with distinct airports (default: %(default)s)",
    )
    return parser.parse_args()


def run(flights_csv_path, callsigns_csv_path, airports_csv_path):
    csv_path = _require_file(Path(flights_csv_path))
    flights = read_flights_csv(csv_path)
    logger.info("Read %s flight record(s) from %s", len(flights), csv_path)

    callsigns = distinct_callsigns(flights)
    write_column_csv(callsigns_csv_path, "callsign", callsigns)
    logger.info("Wrote %s distinct callsign(s) -> %s", len(callsigns), callsigns_csv_path)

    airports = distinct_airports(flights)
    write_column_csv(airports_csv_path, "icao", airports)
    logger.info("Wrote %s distinct airport(s) -> %s", len(airports), airports_csv_path)

    return {"callsigns": Path(callsigns_csv_path), "airports": Path(airports_csv_path)}


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    run(args.flights_csv, args.callsigns_csv, args.airports_csv)


if __name__ == "__main__":
    main()
