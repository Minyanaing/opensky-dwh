"""Enrich the newest batch of callsigns/aircraft with airline/aircraft/route details from the
free, keyless adsbdb.com API - community-maintained, not authoritative. Reads the latest
discovery batch (MAX(_loaded_at)) from opensky_raw.bronze's callsigns/flights_raw tables, not
local files; writes local JSON output (adsbdb has no airport-lookup endpoint, so airports are
read but not queried)."""

import argparse
import json
import logging
import re
import time
from pathlib import Path

import requests

import config
from load_to_databricks import CATALOG, SCHEMA, get_connection
from transforms import utc_now_iso

logger = logging.getLogger(__name__)

ADSBDB_BASE_URL = "https://api.adsbdb.com/v0"
REQUEST_TIMEOUT_SECONDS = 30
REQUEST_PACING_SECONDS = 0.3
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# Safety cap for an unusually large batch, not the everyday throttle it used to be.
MAX_LOCAL_TEST_ITEMS = 20

# ICAO callsign convention: 3 letters + digits. Skips GA tail-number-style callsigns.
_AIRLINE_PREFIX_RE = re.compile(r"^([A-Z]{3})\d")


def _get(path):
    """GET one adsbdb endpoint, unwrapped from its {"response": ...} envelope.
    None on 404 (not found, not an error)."""
    url = f"{ADSBDB_BASE_URL}{path}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        except requests.RequestException as exc:
            logger.warning("Request error on %s (attempt %s/%s): %s", path, attempt, MAX_RETRIES, exc)
            time.sleep(RETRY_BACKOFF_SECONDS)
            continue

        if response.status_code == 404:
            return None

        if response.status_code in RETRYABLE_STATUS_CODES and attempt < MAX_RETRIES:
            wait_seconds = int(response.headers.get("Retry-After", RETRY_BACKOFF_SECONDS))
            logger.warning(
                "HTTP %s on %s, retrying in %ss (attempt %s/%s)",
                response.status_code,
                path,
                wait_seconds,
                attempt,
                MAX_RETRIES,
            )
            time.sleep(wait_seconds)
            continue

        if not response.ok:
            raise requests.HTTPError(
                f"{response.status_code} error calling {path}: {response.text[:500]}", response=response
            )
        return response.json().get("response")

    raise RuntimeError(f"Exhausted retries calling {path}")


def _latest_batch(cursor, table, column):
    """Distinct values from the table's most recent _loaded_at batch only."""
    cursor.execute(
        f"SELECT DISTINCT `{column}` FROM {CATALOG}.{SCHEMA}.{table} "
        f"WHERE `_loaded_at` = (SELECT MAX(`_loaded_at`) FROM {CATALOG}.{SCHEMA}.{table})"
    )
    return sorted({row[0] for row in cursor.fetchall() if row[0]})


def derive_airline_codes(callsigns):
    """Distinct ICAO airline designators from callsign prefixes."""
    codes = set()
    for callsign in callsigns:
        match = _AIRLINE_PREFIX_RE.match(callsign.strip().upper())
        if match:
            codes.add(match.group(1))
    return sorted(codes)


def fetch_callsign_routes(callsigns):
    results = []
    for callsign in callsigns:
        payload = _get(f"/callsign/{callsign}")
        flightroute = (payload or {}).get("flightroute") if payload else None
        results.append(
            {
                "callsign": callsign,
                "found": flightroute is not None,
                "flightroute": flightroute,
                "fetched_at": utc_now_iso(),
            }
        )
        logger.info("callsign %-10s -> %s", callsign, "found" if flightroute else "not found")
        time.sleep(REQUEST_PACING_SECONDS)
    return results


def fetch_aircraft_details(icao24_list):
    results = []
    for icao24 in icao24_list:
        payload = _get(f"/aircraft/{icao24}")
        aircraft = (payload or {}).get("aircraft") if payload else None
        results.append(
            {
                "icao24": icao24,
                "found": aircraft is not None,
                "aircraft": aircraft,
                "fetched_at": utc_now_iso(),
            }
        )
        logger.info("aircraft %-8s -> %s", icao24, "found" if aircraft else "not found")
        time.sleep(REQUEST_PACING_SECONDS)
    return results


def fetch_airlines(airline_codes):
    results = []
    for code in airline_codes:
        payload = _get(f"/airline/{code}")  # a list of airline objects, or None if unknown
        results.append(
            {
                "airline_code": code,
                "found": bool(payload),
                "airlines": payload or [],
                "fetched_at": utc_now_iso(),
            }
        )
        logger.info("airline %-6s -> %s", code, "found" if payload else "not found")
        time.sleep(REQUEST_PACING_SECONDS)
    return results


def _cap(items, label):
    if len(items) > MAX_LOCAL_TEST_ITEMS:
        logger.info(
            "%s %s in the latest batch, capping this run to the first %s",
            len(items),
            label,
            MAX_LOCAL_TEST_ITEMS,
        )
        return items[:MAX_LOCAL_TEST_ITEMS]
    return items


def _write_json(file_path, data):
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(config.OUTPUT_DIR),
        help="Directory to write the adsbdb_*.json files to (default: %(default)s)",
    )
    return parser.parse_args()


def run(output_dir):
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            callsigns = _cap(_latest_batch(cursor, "callsigns", "callsign"), "callsign(s)")
            icao24_list = _cap(_latest_batch(cursor, "flights_raw", "icao24"), "aircraft icao24 value(s)")
            airports_batch = _latest_batch(cursor, "airports", "icao")
    finally:
        connection.close()

    airline_codes = _cap(derive_airline_codes(callsigns), "derived airline code(s)")

    logger.info(
        "Latest batch: %s callsign(s), %s aircraft (icao24), %s airport(s) (not queried - "
        "adsbdb has no airport endpoint), %s derived airline code(s)",
        len(callsigns),
        len(icao24_list),
        len(airports_batch),
        len(airline_codes),
    )

    callsign_results = fetch_callsign_routes(callsigns)
    callsigns_path = out_dir / "adsbdb_callsigns.json"
    _write_json(callsigns_path, callsign_results)

    aircraft_results = fetch_aircraft_details(icao24_list)
    aircraft_path = out_dir / "adsbdb_aircraft.json"
    _write_json(aircraft_path, aircraft_results)

    airline_results = fetch_airlines(airline_codes)
    airlines_path = out_dir / "adsbdb_airlines.json"
    _write_json(airlines_path, airline_results)

    logger.info(
        "Wrote %s callsign route(s) -> %s | %s aircraft record(s) -> %s | %s airline record(s) -> %s",
        len(callsign_results),
        callsigns_path,
        len(aircraft_results),
        aircraft_path,
        len(airline_results),
        airlines_path,
    )
    return {"callsigns": callsigns_path, "aircraft": aircraft_path, "airlines": airlines_path}


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    run(args.output_dir)


if __name__ == "__main__":
    main()
