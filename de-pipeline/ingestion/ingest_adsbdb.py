"""Enrich the exported callsigns/aircraft with airline/aircraft/route details from the free,
keyless adsbdb.com API - community-maintained, not authoritative.

Reads directly from local CSV files (the same ones ingest_opensky.py writes), not Databricks:
- data/callsigns.csv   -> GET /callsign/{callsign}, for every distinct callsign (no cap)
- data/flights_raw.csv -> distinct icao24           -> GET /aircraft/{mode_s}

adsbdb has no separate airline or airport lookup endpoint, so those aren't queried directly -
each callsign route response already embeds its airline and origin/destination airport. Airline
and airport reference CSVs are derived from that same callsign data (deduped by icao), not from
extra API calls.

Output is flat CSV: adsbdb_callsigns.csv / adsbdb_airlines.csv / adsbdb_airports.csv /
adsbdb_aircraft.csv - only the fields useful downstream, not the full nested API response.
"""

import argparse
import csv
import logging
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

CALLSIGN_COLUMNS = [
    "callsign",
    "found",
    "callsign_icao",
    "callsign_iata",
    "airline_icao",
    "airline_iata",
    "airline_name",
    "airline_country",
    "airline_callsign",
    "origin_icao",
    "origin_iata",
    "origin_name",
    "origin_country",
    "destination_icao",
    "destination_iata",
    "destination_name",
    "destination_country",
    "fetched_at",
]

AIRLINE_COLUMNS = ["icao", "iata", "name", "country", "callsign"]

AIRPORT_COLUMNS = ["icao", "iata", "name", "country"]

AIRCRAFT_COLUMNS = [
    "icao24",
    "found",
    "type",
    "icao_type",
    "manufacturer",
    "registration",
    "registered_owner",
    "registered_owner_country",
    "fetched_at",
]


def _get(path):
    """GET one adsbdb endpoint, unwrapped from its {"response": ...} envelope.
    None on 404 (not found) or 400 (invalid identifier, e.g. a non-ICAO callsign) - both are
    skippable, not errors."""
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

        if response.status_code == 400:
            logger.warning("Skipping %s - invalid identifier: %s", path, response.text[:200])
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


def _read_column(path, column):
    path = Path(path)
    if not path.is_file():
        logger.warning("%s not found, treating as empty", path)
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return sorted({row[column] for row in csv.DictReader(f) if row.get(column)})


def _flatten_callsign(callsign, flightroute, fetched_at):
    airline = (flightroute or {}).get("airline") or {}
    origin = (flightroute or {}).get("origin") or {}
    destination = (flightroute or {}).get("destination") or {}
    return {
        "callsign": callsign,
        "found": flightroute is not None,
        "callsign_icao": (flightroute or {}).get("callsign_icao", ""),
        "callsign_iata": (flightroute or {}).get("callsign_iata", ""),
        "airline_icao": airline.get("icao", ""),
        "airline_iata": airline.get("iata", ""),
        "airline_name": airline.get("name", ""),
        "airline_country": airline.get("country", ""),
        "airline_callsign": airline.get("callsign", ""),
        "origin_icao": origin.get("icao_code", ""),
        "origin_iata": origin.get("iata_code", ""),
        "origin_name": origin.get("name", ""),
        "origin_country": origin.get("country_name", ""),
        "destination_icao": destination.get("icao_code", ""),
        "destination_iata": destination.get("iata_code", ""),
        "destination_name": destination.get("name", ""),
        "destination_country": destination.get("country_name", ""),
        "fetched_at": fetched_at,
    }


def _flatten_aircraft(icao24, aircraft, fetched_at):
    aircraft = aircraft or {}
    return {
        "icao24": icao24,
        "found": bool(aircraft),
        "type": aircraft.get("type", ""),
        "icao_type": aircraft.get("icao_type", ""),
        "manufacturer": aircraft.get("manufacturer", ""),
        "registration": aircraft.get("registration", ""),
        "registered_owner": aircraft.get("registered_owner", ""),
        "registered_owner_country": aircraft.get("registered_owner_country_name", ""),
        "fetched_at": fetched_at,
    }


def _derive_airlines(callsign_rows):
    """Distinct airlines embedded in the callsign route results - no separate API call."""
    seen = {}
    for row in callsign_rows:
        icao = row.get("airline_icao")
        if not icao or icao in seen:
            continue
        seen[icao] = {
            "icao": icao,
            "iata": row.get("airline_iata", ""),
            "name": row.get("airline_name", ""),
            "country": row.get("airline_country", ""),
            "callsign": row.get("airline_callsign", ""),
        }
    return [seen[icao] for icao in sorted(seen)]


def _derive_airports(callsign_rows):
    """Distinct airports (both origin and destination) embedded in the callsign route results -
    adsbdb has no airport-lookup endpoint, so this is the only way to get airport details from it."""
    seen = {}
    for row in callsign_rows:
        for prefix in ("origin", "destination"):
            icao = row.get(f"{prefix}_icao")
            if not icao or icao in seen:
                continue
            seen[icao] = {
                "icao": icao,
                "iata": row.get(f"{prefix}_iata", ""),
                "name": row.get(f"{prefix}_name", ""),
                "country": row.get(f"{prefix}_country", ""),
            }
    return [seen[icao] for icao in sorted(seen)]


def fetch_callsign_routes(callsigns):
    results = []
    for callsign in callsigns:
        payload = _get(f"/callsign/{callsign}")
        flightroute = (payload or {}).get("flightroute") if payload else None
        results.append(_flatten_callsign(callsign, flightroute, utc_now_iso()))
        logger.info("callsign %-10s -> %s", callsign, "found" if flightroute else "not found")
        time.sleep(REQUEST_PACING_SECONDS)
    return results


def fetch_aircraft_details(icao24_list):
    results = []
    for icao24 in icao24_list:
        payload = _get(f"/aircraft/{icao24}")
        aircraft = (payload or {}).get("aircraft") if payload else None
        results.append(_flatten_aircraft(icao24, aircraft, utc_now_iso()))
        logger.info("aircraft %-8s -> %s", icao24, "found" if aircraft else "not found")
        time.sleep(REQUEST_PACING_SECONDS)
    return results


def _write_csv(file_path, fieldnames, rows):
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore", restval="")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def parse_args():
    default_dir = config.OUTPUT_DIR
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--callsigns-csv",
        type=str,
        default=str(default_dir / "callsigns.csv"),
        help="Path to the distinct-callsigns CSV (default: %(default)s)",
    )
    parser.add_argument(
        "--flights-csv",
        type=str,
        default=str(default_dir / "flights_raw.csv"),
        help="Path to the flights CSV, for distinct icao24 values (default: %(default)s)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(default_dir),
        help="Directory to write the adsbdb_*.csv files to (default: %(default)s)",
    )
    return parser.parse_args()


def run(callsigns_csv_path, flights_csv_path, output_dir):
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    callsigns = _read_column(callsigns_csv_path, "callsign")
    icao24_list = _read_column(flights_csv_path, "icao24")
    logger.info("%s callsign(s), %s aircraft (icao24)", len(callsigns), len(icao24_list))

    callsign_results = fetch_callsign_routes(callsigns)
    callsigns_path = out_dir / "adsbdb_callsigns.csv"
    _write_csv(callsigns_path, CALLSIGN_COLUMNS, callsign_results)

    airline_rows = _derive_airlines(callsign_results)
    airlines_path = out_dir / "adsbdb_airlines.csv"
    _write_csv(airlines_path, AIRLINE_COLUMNS, airline_rows)

    airport_rows = _derive_airports(callsign_results)
    airports_path = out_dir / "adsbdb_airports.csv"
    _write_csv(airports_path, AIRPORT_COLUMNS, airport_rows)

    aircraft_results = fetch_aircraft_details(icao24_list)
    aircraft_path = out_dir / "adsbdb_aircraft.csv"
    _write_csv(aircraft_path, AIRCRAFT_COLUMNS, aircraft_results)

    logger.info(
        "Wrote %s callsign route(s) -> %s | %s airline(s) -> %s | %s airport(s) -> %s | "
        "%s aircraft record(s) -> %s",
        len(callsign_results),
        callsigns_path,
        len(airline_rows),
        airlines_path,
        len(airport_rows),
        airports_path,
        len(aircraft_results),
        aircraft_path,
    )
    return {
        "callsigns": callsigns_path,
        "airlines": airlines_path,
        "airports": airports_path,
        "aircraft": aircraft_path,
    }


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    run(args.callsigns_csv, args.flights_csv, args.output_dir)


if __name__ == "__main__":
    main()
