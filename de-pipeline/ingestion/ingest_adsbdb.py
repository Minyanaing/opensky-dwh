"""Local test: enrich exported OpenSky data with airline, aircraft, and callsign/route
details from the free, keyless adsbdb.com API (https://www.adsbdb.com/) - community-maintained,
not authoritative, same caveat as OpenSky's own `est*` fields.

Reads local files written by ingest_opensky.py as input:
- data/callsigns.csv    -> distinct callsigns -> GET /callsign/{callsign}  (route + embedded airline)
- data/flights_raw.json -> distinct icao24     -> GET /aircraft/{mode_s}   (aircraft details)
Airline codes are derived from callsign ICAO prefixes (not read from a file) and looked up
separately via GET /airline/{code} for a clean, deduped airline reference set.

airports.csv is not used here: adsbdb has no airport-lookup endpoint (confirmed against its
API docs) - airport enrichment is a separate concern (e.g. an OurAirports-based seed).

Local-only for now: writes JSON to disk. In Databricks, this will instead read from the Unity
Catalog tables built off the exported callsigns/airports CSVs (configured later), not local files.
"""

import argparse
import csv
import json
import logging
import re
import time
from pathlib import Path

import requests

import config
from transforms import utc_now_iso

logger = logging.getLogger(__name__)

ADSBDB_BASE_URL = "https://api.adsbdb.com/v0"
REQUEST_TIMEOUT_SECONDS = 30
REQUEST_PACING_SECONDS = 0.3
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# This script is for local testing only - cap each of the three inputs (callsigns, aircraft,
# derived airline codes) so a test run stays quick regardless of how much ingest_opensky.py pulled.
MAX_LOCAL_TEST_ITEMS = 20

# ICAO flight-callsign convention: 3-letter airline designator + a flight number/suffix.
# General-aviation callsigns (often just a tail number, e.g. "N8577FT") don't match this and
# are skipped rather than mined for a bogus "airline code".
_AIRLINE_PREFIX_RE = re.compile(r"^([A-Z]{3})\d")


def _get(path):
    """GET one adsbdb endpoint, unwrapped from its {"response": ...} envelope.

    Returns None on 404 (not in adsbdb's crowdsourced database - not an error).
    """
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


def _read_callsigns(csv_path):
    with open(csv_path, newline="", encoding="utf-8") as f:
        return [row["callsign"] for row in csv.DictReader(f) if row.get("callsign")]


def _read_distinct_icao24(flights_json_path):
    with open(flights_json_path, encoding="utf-8") as f:
        flights = json.load(f)
    return sorted({flight["icao24"] for flight in flights if flight.get("icao24")})


def derive_airline_codes(callsigns):
    """Extract distinct ICAO airline designators from callsign prefixes, skipping any
    callsign that doesn't look like a scheduled-flight callsign (e.g. GA tail numbers)."""
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
            "%s %s found, capping this local test run to the first %s",
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
        "--input-dir",
        type=str,
        default=str(config.OUTPUT_DIR),
        help="Directory holding ingest_opensky.py's callsigns.csv / flights_raw.json (default: %(default)s)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(config.OUTPUT_DIR),
        help="Directory to write the adsbdb_*.json files to (default: %(default)s)",
    )
    return parser.parse_args()


def run(input_dir, output_dir):
    in_dir = Path(input_dir)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    callsigns = _cap(_read_callsigns(in_dir / "callsigns.csv"), "callsign(s)")
    icao24_list = _cap(_read_distinct_icao24(in_dir / "flights_raw.json"), "aircraft icao24 value(s)")
    airline_codes = _cap(derive_airline_codes(callsigns), "derived airline code(s)")

    logger.info(
        "%s callsign(s), %s aircraft (icao24), %s derived airline code(s)",
        len(callsigns),
        len(icao24_list),
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
    run(args.input_dir, args.output_dir)


if __name__ == "__main__":
    main()
