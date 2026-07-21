"""Entry point: pull a rolling trailing window of arrivals/departures for every curated
Southeast Asia airport, and write three local CSV files (flights_raw.csv, callsigns.csv,
airports.csv). This script never talks to Databricks - load_to_databricks.py uploads these
files to a Databricks Volume as a separate step."""

import argparse
import csv
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import config
from fetch_data import TokenManager, chunk_window, fetch_arrivals, fetch_departures
from transforms import dedupe, distinct_airports, distinct_callsigns, tag_record, utc_now_iso

logger = logging.getLogger(__name__)

REQUEST_PACING_SECONDS = 0.3


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--lookback-days",
        type=float,
        default=config.LOOKBACK_DAYS,
        help="Trailing window size in days (default: %(default)s)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(config.OUTPUT_DIR),
        help="Directory to write the output CSV files to (default: %(default)s)",
    )
    parser.add_argument(
        "--airports",
        type=str,
        default=None,
        help="Comma-separated ICAO codes to override the full curated list (e.g. WSSS,VTBS) "
        "- useful for a quick local test",
    )
    return parser.parse_args()


def _select_airports(airport_codes):
    if not airport_codes:
        return config.AIRPORTS
    wanted = {code.strip().upper() for code in airport_codes.split(",") if code.strip()}
    selected = [a for a in config.AIRPORTS if a["icao"] in wanted]
    missing = wanted - {a["icao"] for a in selected}
    if missing:
        raise ValueError(f"Unknown ICAO code(s) not in the curated list: {sorted(missing)}")
    return selected


def run(lookback_days, output_dir, airport_codes=None):
    airports = _select_airports(airport_codes)
    token_manager = TokenManager()

    now = datetime.now(timezone.utc)
    window_start = now
    window_end = now - timedelta(days=lookback_days)
    chunks = chunk_window(window_end.timestamp(), window_start.timestamp())

    logger.info(
        "Window: last %s day(s) -> %s chunk(s). %s airport(s) x 2 directions x %s chunk(s) = %s API calls",
        lookback_days,
        len(chunks),
        len(airports),
        len(chunks),
        len(airports) * 2 * len(chunks),
    )

    fetchers = (("departure", fetch_departures), ("arrival", fetch_arrivals))
    records = []
    for airport in airports:
        icao = airport["icao"]
        for movement_type, fetch_fn in fetchers:
            for begin, end in chunks:
                flights = fetch_fn(token_manager, icao, begin, end)
                fetched_at = utc_now_iso()
                records.extend(tag_record(f, icao, movement_type, fetched_at) for f in flights)
                logger.info(
                    "%-4s %-10s [%s .. %s): %s flight(s)",
                    icao,
                    movement_type,
                    int(begin),
                    int(end),
                    len(flights),
                )
                time.sleep(REQUEST_PACING_SECONDS)

    deduped = dedupe(records)
    logger.info("Fetched %s raw record(s), %s after de-dup", len(records), len(deduped))

    callsigns = distinct_callsigns(deduped)
    airport_codes_seen = distinct_airports(deduped)

    return _write_local_files(output_dir, deduped, callsigns, airport_codes_seen)


def _write_local_files(output_dir, flights, callsigns, airports):
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    flights_path = out_dir / "flights_raw.csv"
    _write_dict_csv(flights_path, config.FLIGHT_COLUMNS, flights)

    callsigns_path = out_dir / "callsigns.csv"
    _write_csv(callsigns_path, ["callsign"], callsigns)

    airports_path = out_dir / "airports.csv"
    _write_csv(airports_path, ["icao"], airports)

    logger.info(
        "Wrote %s flight record(s) -> %s | %s distinct callsign(s) -> %s | %s distinct airport(s) -> %s",
        len(flights),
        flights_path,
        len(callsigns),
        callsigns_path,
        len(airports),
        airports_path,
    )
    return {"flights": flights_path, "callsigns": callsigns_path, "airports": airports_path}


def _write_dict_csv(file_path, fieldnames, records):
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for record in records:
            writer.writerow(record)


def _write_csv(file_path, header, values):
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for value in values:
            writer.writerow([value])


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    run(args.lookback_days, args.output_dir, args.airports)


if __name__ == "__main__":
    main()
