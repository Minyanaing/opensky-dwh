"""Enrich data/airports.csv with metadata from the free, keyless airport-data.com API - one
GET per distinct ICAO code, keeping every field the API returns (no curation). Per the API's
documented status codes, 400 (invalid query params) and 404 (not found) both return a
well-formed {"status": ..., "error": "..."} body, same shape as a 200 - so neither is treated
as an error. A not-found/invalid airport still gets a full row - just with every airport-data
field explicitly null, not an error and not a skipped/missing row. Only 429 (rate limit) and
500 (server error) are retried.

If a persistent error (rate limit exhausted, network failure) stops the run partway through,
whatever was already fetched is still exported rather than discarded.

Writes data/airport_data.csv by default. Pass --json to also write data/airport_data.json
(same records, both formats) - CSV alone otherwise.
"""

import argparse
import csv
import json
import logging
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "common"))

import config
from transforms import read_column, utc_now_iso

logger = logging.getLogger(__name__)

API_BASE = "https://airport-data.com/api/ap_info.json"
REQUEST_TIMEOUT_SECONDS = 30
REQUEST_PACING_SECONDS = 0.3
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# The API's own success-response fields - every row gets these, explicitly null if not found.
AIRPORT_FIELDS = ["icao", "iata", "name", "location", "country", "country_code", "longitude", "latitude", "link"]


def _get(icao):
    """GET one airport's data. Returns the parsed JSON body for any of the API's documented
    outcomes - 200 (found), 400 (invalid query params, e.g. a malformed ICAO), or 404 (not
    found) - all three return a well-formed JSON body, so none of them are treated as errors.
    Only 429/5xx (rate limit / server error) are retried; anything else still raises."""
    url = f"{API_BASE}?icao={icao}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        except requests.RequestException as exc:
            logger.warning("Request error on %s (attempt %s/%s): %s", icao, attempt, MAX_RETRIES, exc)
            time.sleep(RETRY_BACKOFF_SECONDS)
            continue

        if response.status_code in RETRYABLE_STATUS_CODES and attempt < MAX_RETRIES:
            wait_seconds = int(response.headers.get("Retry-After", RETRY_BACKOFF_SECONDS))
            logger.warning(
                "HTTP %s on %s, retrying in %ss (attempt %s/%s)",
                response.status_code,
                icao,
                wait_seconds,
                attempt,
                MAX_RETRIES,
            )
            time.sleep(wait_seconds)
            continue

        if not response.ok and response.status_code not in (400, 404):
            raise requests.HTTPError(
                f"{response.status_code} error calling {url}: {response.text[:500]}", response=response
            )
        return response.json()

    raise RuntimeError(f"Exhausted retries calling {url}")


def _flatten(icao, payload):
    """Every row gets the same fields, in the same order - explicitly null (not just absent)
    for a not-found/invalid airport, so the output shape never varies row to row."""
    row = {"queried_icao": icao, "fetched_at": utc_now_iso()}
    for field in AIRPORT_FIELDS:
        row[field] = payload.get(field)
    row["status"] = payload.get("status")
    row["error"] = payload.get("error")
    return row


def fetch_airports(icao_list):
    """Fetch every ICAO code's data. If a persistent error (rate limit exhausted, network
    failure) stops us partway through, whatever was already fetched is returned rather than
    lost - the same partial-failure handling as ingest_opensky.py."""
    results = []
    try:
        for icao in icao_list:
            payload = _get(icao)
            results.append(_flatten(icao, payload))
            logger.info("airport %-6s -> %s", icao, "found" if payload.get("status") == 200 else "not found")
            time.sleep(REQUEST_PACING_SECONDS)
    except (requests.RequestException, RuntimeError) as exc:
        logger.warning(
            "Stopping early after %s record(s) - %s: %s. Exporting what was collected instead of "
            "discarding it.",
            len(results),
            type(exc).__name__,
            exc,
        )
    return results


def _write_csv(file_path, rows):
    fieldnames = ["queried_icao", "fetched_at"] + AIRPORT_FIELDS + ["status", "error"]
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, restval="")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_json(file_path, rows):
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)


def parse_args():
    default_dir = config.OUTPUT_DIR
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--airports-csv",
        type=str,
        default=str(default_dir / "airports.csv"),
        help="Path to the distinct-airports CSV (default: %(default)s)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(default_dir),
        help="Directory to write airport_data.csv (and optionally .json) to (default: %(default)s)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Also write airport_data.json alongside the CSV (default: CSV only)",
    )
    return parser.parse_args()


def run(airports_csv_path, output_dir, write_json=False):
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    icao_list = read_column(airports_csv_path, "icao")
    logger.info("%s airport(s) to look up", len(icao_list))

    rows = fetch_airports(icao_list)

    csv_path = out_dir / "airport_data.csv"
    _write_csv(csv_path, rows)
    result = {"csv": csv_path}
    log_msg = "Wrote %s airport record(s) -> %s"
    log_args = [len(rows), csv_path]

    if write_json:
        json_path = out_dir / "airport_data.json"
        _write_json(json_path, rows)
        result["json"] = json_path
        log_msg += ", %s"
        log_args.append(json_path)

    logger.info(log_msg, *log_args)
    return result


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    run(args.airports_csv, args.output_dir, write_json=args.json)


if __name__ == "__main__":
    main()
