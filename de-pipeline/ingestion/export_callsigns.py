"""Rolls the previous run's callsigns.csv (last run's "new" delta) into callsigns_old.csv, then
extracts distinct callsigns from flights_raw.csv and overwrites callsigns.csv with only the ones
not already in that accumulated history.

Split out from ingest_opensky.py so a problem here (or in export_airports.py) doesn't require
re-fetching from OpenSky - re-run this on its own against the existing flights_raw.csv.
"""

import argparse
import csv
import logging
from pathlib import Path

import config
from transforms import append_column, distinct_callsigns, export_new_only

logger = logging.getLogger(__name__)


def _read_flights(flights_csv_path):
    with open(flights_csv_path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def run(flights_csv_path, output_dir):
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    flights = _read_flights(flights_csv_path)

    callsigns_path = out_dir / "callsigns.csv"
    callsigns_old_path = out_dir / "callsigns_old.csv"
    append_column(callsigns_path, callsigns_old_path, "callsign")

    new_callsigns = export_new_only(distinct_callsigns(flights), callsigns_old_path, callsigns_path, "callsign")

    logger.info("Wrote %s new callsign(s) -> %s", len(new_callsigns), callsigns_path)
    return {"callsigns": callsigns_path}


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--flights-csv",
        type=str,
        default=str(config.OUTPUT_DIR / "flights_raw.csv"),
        help="Path to flights_raw.csv, written by ingest_opensky.py (default: %(default)s)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(config.OUTPUT_DIR),
        help="Directory to read/write callsigns.csv/callsigns_old.csv (default: %(default)s)",
    )
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    run(args.flights_csv, args.output_dir)


if __name__ == "__main__":
    main()
