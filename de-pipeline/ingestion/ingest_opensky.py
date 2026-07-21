"""Entry point: pull arrivals/departures for every curated Southeast Asia airport, for the
`lookback_days` full UTC calendar day(s) immediately before today - e.g. with the default of 1,
running today fetches all of yesterday (00:00:00 to 23:59:59 UTC), not a partial day.

Step by step, each run:
1. Overwrites flights_raw.csv with whatever was fetched - if the API limit is hit partway
   through, whatever was already received is exported rather than discarded.
2. Rolls the previous run's airports.csv (last run's "new" delta) into airports_old.csv.
3. Rolls the previous run's callsigns.csv (last run's "new" delta) into callsigns_old.csv.
4. Extracts distinct airports from this run's flights, compares against airports_old.csv, and
   overwrites airports.csv with only the ones not already in that accumulated history.
5. Same as step 4, for callsigns.

This script never talks to Databricks - load_to_databricks.py uploads these files to a
Databricks Volume as a separate step.
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
from transforms import dedupe, distinct_airports, distinct_callsigns, tag_record, utc_now_iso

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

    # Step 1: overwrite flights_raw.csv with whatever was received.
    flights_path = out_dir / "flights_raw.csv"
    _write_dict_csv(flights_path, config.FLIGHT_COLUMNS, flights)

    # Steps 2-3: roll the previous run's "new" delta into the accumulated history, before
    # this run's comparison happens.
    airports_path = out_dir / "airports.csv"
    airports_old_path = out_dir / "airports_old.csv"
    _append_column(airports_path, airports_old_path, "icao")

    callsigns_path = out_dir / "callsigns.csv"
    callsigns_old_path = out_dir / "callsigns_old.csv"
    _append_column(callsigns_path, callsigns_old_path, "callsign")

    # Steps 4-5: this run's distinct values, filtered down to only what's not already known.
    new_airports = _write_new_only(distinct_airports(flights), airports_old_path, airports_path, "icao")
    new_callsigns = _write_new_only(distinct_callsigns(flights), callsigns_old_path, callsigns_path, "callsign")

    logger.info(
        "Wrote %s flight record(s) -> %s | %s new callsign(s) -> %s | %s new airport(s) -> %s",
        len(flights),
        flights_path,
        len(new_callsigns),
        callsigns_path,
        len(new_airports),
        airports_path,
    )
    return {"flights": flights_path, "callsigns": callsigns_path, "airports": airports_path}


def _read_column(path, column):
    path = Path(path)
    if not path.is_file():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return [row[column] for row in csv.DictReader(f) if row.get(column)]


def _append_column(source_path, dest_path, column):
    """Append source_path's values onto dest_path, creating dest_path (with header) if needed."""
    values = _read_column(source_path, column)
    if not values:
        return
    dest_path = Path(dest_path)
    write_header = not dest_path.is_file()
    with open(dest_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow([column])
        for value in values:
            writer.writerow([value])


def _write_new_only(current_values, old_path, out_path, column):
    """Compare current_values against everything already in old_path; overwrite out_path with
    only the ones not already known."""
    existing = set(_read_column(old_path, column))
    new_values = sorted({v for v in current_values if v} - existing)
    _write_csv(out_path, [column], new_values)
    return new_values


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
