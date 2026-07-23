"""Entry point: pull arrivals/departures for every curated Southeast Asia airport, for the
`lookback_days` full UTC calendar day(s) immediately before today - e.g. with the default of 1,
running today fetches all of yesterday (00:00:00 to 23:59:59 UTC), not a partial day.

Only fetches from OpenSky and overwrites flights_raw.csv with whatever was received - if the
API limit is hit partway through, whatever was already fetched is exported rather than
discarded. Extracting distinct callsigns/airports is a separate step, so a problem there
doesn't require re-fetching from OpenSky:
  - export_airports.py  - airports.csv / airports_old.csv
  - export_callsigns.py - callsigns.csv / callsigns_old.csv

This script never talks to Databricks/Snowflake either - load_to_databricks.py /
load_to_snowflake.py upload these files as a separate step.
"""

import argparse
import csv
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

import config
from fetch_data import TokenManager, chunk_window, fetch_arrivals, fetch_departures
from transforms import dedupe, tag_record, utc_now_iso

logger = logging.getLogger(__name__)

REQUEST_PACING_SECONDS = 0.3


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=config.LOOKBACK_DAYS,
        help="Number of full UTC calendar days before today to fetch (default: %(default)s)",
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


def _fetch_all(token_manager, airports, chunks):
    """Fetch every (airport, direction, chunk) combination. If a persistent error (e.g. the API
    limit) stops us partway through, whatever was already fetched is returned rather than lost."""
    fetchers = (("departure", fetch_departures), ("arrival", fetch_arrivals))
    records = []
    try:
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
    except (requests.RequestException, RuntimeError) as exc:
        logger.warning(
            "Stopping early after %s record(s) - %s: %s. Exporting what was received instead of "
            "discarding it.",
            len(records),
            type(exc).__name__,
            exc,
        )
        return records, True
    return records, False


def run(lookback_days, output_dir, airport_codes=None):
    airports = _select_airports(airport_codes)
    token_manager = TokenManager()

    now = datetime.now(timezone.utc)
    # Start of today (UTC) - the exclusive upper bound, so "today" itself is never included.
    window_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    window_end = window_start - timedelta(days=lookback_days)
    chunks = chunk_window(window_end.timestamp(), window_start.timestamp())

    logger.info("Window start (UTC): %s", window_end.isoformat())
    logger.info("Window end (UTC): %s", window_start.isoformat())
    logger.info(
        "Window: %s full day(s) ending yesterday -> %s chunk(s). %s airport(s) x 2 directions x %s chunk(s) = %s API calls",
        lookback_days,
        len(chunks),
        len(airports),
        len(chunks),
        len(airports) * 2 * len(chunks),
    )

    records, partial = _fetch_all(token_manager, airports, chunks)
    deduped = dedupe(records)
    logger.info(
        "Fetched %s raw record(s), %s after de-dup%s",
        len(records),
        len(deduped),
        " [PARTIAL RUN - API limit or error stopped fetching early]" if partial else "",
    )

    return _write_local_files(output_dir, deduped)


def _write_local_files(output_dir, flights):
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    flights_path = out_dir / "flights_raw.csv"
    _write_dict_csv(flights_path, config.FLIGHT_COLUMNS, flights)

    logger.info("Wrote %s flight record(s) -> %s", len(flights), flights_path)
    return {"flights": flights_path}


def _write_dict_csv(file_path, fieldnames, records):
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for record in records:
            writer.writerow(record)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    run(args.lookback_days, args.output_dir, args.airports)


if __name__ == "__main__":
    main()
