"""Rebuild distinct-value CSVs from an existing flights_raw.csv:
- convert_flight_raw.csv - flight records, re-written through databricks/setup.sql's
  opensky_raw.bronze.flights_raw column set.
- convert_callsigns.csv  - distinct callsigns extracted from the same file.
- convert_airports.csv   - distinct airport codes extracted from the same file (both ends of
  every flight, same as ingest_opensky.py's distinct_airports()).

Useful to regenerate a consistent callsigns/airports snapshot straight from a flights_raw.csv,
independent of ingest_opensky.py's own new-since-last-run tracking.
"""

import argparse
import csv
import logging
from pathlib import Path

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


def write_flights_csv(path, flights):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=config.FLIGHT_COLUMNS, extrasaction="ignore", restval="")
        writer.writeheader()
        for flight in flights:
            writer.writerow(flight)


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
        help="Path to the existing flights_raw.csv to rebuild from (default: %(default)s)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(default_dir),
        help="Directory to write convert_flight_raw.csv/convert_callsigns.csv/convert_airports.csv "
        "to (default: %(default)s)",
    )
    return parser.parse_args()


def run(flights_csv_path, output_dir):
    csv_path = _require_file(Path(flights_csv_path))
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    flights = read_flights_csv(csv_path)
    logger.info("Read %s flight record(s) from %s", len(flights), csv_path)

    flights_csv_out_path = out_dir / "convert_flight_raw.csv"
    write_flights_csv(flights_csv_out_path, flights)
    logger.info("Wrote %s flight record(s) -> %s", len(flights), flights_csv_out_path)

    callsigns = distinct_callsigns(flights)
    callsigns_csv_path = out_dir / "convert_callsigns.csv"
    write_column_csv(callsigns_csv_path, "callsign", callsigns)
    logger.info("Wrote %s distinct callsign(s) -> %s", len(callsigns), callsigns_csv_path)

    airports = distinct_airports(flights)
    airports_csv_path = out_dir / "convert_airports.csv"
    write_column_csv(airports_csv_path, "icao", airports)
    logger.info("Wrote %s distinct airport(s) -> %s", len(airports), airports_csv_path)

    return {
        "flights": flights_csv_out_path,
        "callsigns": callsigns_csv_path,
        "airports": airports_csv_path,
    }


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    run(args.flights_csv, args.output_dir)


if __name__ == "__main__":
    main()
