"""Manual test utility: pull /flights/aircraft history for one aircraft and export raw JSON.

Not part of the scheduled ingestion path (Step 2 tracks curated airports, not individual
aircraft) - this is for ad hoc inspection of what OpenSky returns per aircraft, e.g. to
check an icao24 seen in a previous ingest_opensky.py output file.
"""

import argparse
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import config
from fetch_data import TokenManager, chunk_window, fetch_aircraft_flights
from transforms import utc_now_iso

logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--icao24", required=True, help="Aircraft transponder address, e.g. 7823bc")
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
        help="Directory to write the output JSON file to (default: %(default)s)",
    )
    return parser.parse_args()


def run(icao24, lookback_days, output_dir):
    icao24 = icao24.strip().lower()
    token_manager = TokenManager()

    now = datetime.now(timezone.utc)
    window_start = now
    window_end = now - timedelta(days=lookback_days)
    chunks = chunk_window(window_end.timestamp(), window_start.timestamp())

    logger.info("Pulling /flights/aircraft for %s across %s chunk(s)", icao24, len(chunks))

    seen_keys = set()
    records = []
    for begin, end in chunks:
        flights = fetch_aircraft_flights(token_manager, icao24, begin, end)
        fetched_at = utc_now_iso()
        for flight in flights:
            key = (flight.get("icao24"), flight.get("firstSeen"))
            if key in seen_keys:
                continue
            seen_keys.add(key)
            tagged = dict(flight)
            tagged["queried_icao24"] = icao24
            tagged["fetched_at"] = fetched_at
            records.append(tagged)
        logger.info("  [%s .. %s): %s flight(s)", int(begin), int(end), len(flights))

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    file_path = out_dir / f"aircraft_flights_{icao24}_{now.strftime('%Y%m%dT%H%M%SZ')}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)

    logger.info("Wrote %s record(s) to %s", len(records), file_path)
    return file_path


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    run(args.icao24, args.lookback_days, args.output_dir)


if __name__ == "__main__":
    main()
