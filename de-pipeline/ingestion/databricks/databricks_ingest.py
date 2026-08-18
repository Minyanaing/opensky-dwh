"""Orchestrates ingest_opensky -> export_airports -> export_callsigns -> ingest_adsbdb ->
ingest_airports (run_daily.bat's extraction/enrichment steps) as one importable entry point for
a Databricks notebook/job. load_to_databricks.py / load_to_snowflake.py are not part of this -
see land_to_volume() instead.

Local: same effect as run_daily.bat up to (not including) load_to_databricks.py.
Databricks: call run_all(land_volume=True) - lands each CSV into its Volume folder with a
timestamped name via a plain filesystem copy (Volumes are mounted paths on Databricks compute).
"""

import argparse
import logging
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
for _folder in ("common", "pipeline", "loaders"):
    sys.path.insert(0, str(_ROOT / _folder))

import config
import export_airports
import export_callsigns
import ingest_adsbdb
import ingest_airports
import ingest_opensky
from load_to_databricks import CATALOG, DATASETS, SCHEMA, VOLUME

logger = logging.getLogger(__name__)

# Matches run_daily.bat's load_to_databricks.py dataset list - aircrafts/airports_master aren't
# part of the daily sequence.
LANDING_DATASETS = ["flights_raw", "airlines", "airports", "callsigns", "airport_data"]


def run_ingestion(lookback_days=None, output_dir=None, airport_codes=None, adsbdb_limit=None, write_json=False):
    output_dir = str(output_dir or config.OUTPUT_DIR)
    lookback_days = config.LOOKBACK_DAYS if lookback_days is None else lookback_days

    flights = ingest_opensky.run(lookback_days, output_dir, airport_codes)
    airports = export_airports.run(str(flights["flights"]), output_dir)
    callsigns = export_callsigns.run(str(flights["flights"]), output_dir)
    ingest_adsbdb.run_callsigns_airlines_airports(str(callsigns["callsigns"]), output_dir, limit=adsbdb_limit)
    ingest_airports.run(str(airports["airports"]), output_dir, write_json=write_json)

    logger.info("Ingestion + enrichment sequence complete -> %s", output_dir)
    return output_dir


def land_to_volume(output_dir, datasets=LANDING_DATASETS, volume_root=None):
    """Copies each dataset's CSV into its Volume folder with a timestamped filename - same
    naming convention as load_to_databricks.py's upload_file(), just a plain copy since Volumes
    are mounted paths from inside Databricks compute."""
    volume_root = Path(volume_root or f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}")
    in_dir = Path(output_dir)
    landed = {}
    for dataset in datasets:
        filename, folder = DATASETS[dataset]
        local_path = in_dir / filename
        if not local_path.is_file():
            logger.warning("%s not found, skipping %s", local_path, dataset)
            continue
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        dest_dir = volume_root / folder
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = dest_dir / f"{local_path.stem}_{stamp}{local_path.suffix}"
        shutil.copy2(local_path, dest_path)
        logger.info("Landed %s -> %s", local_path, dest_path)
        landed[dataset] = dest_path
    logger.info("Landed %s file(s) to %s", len(landed), volume_root)
    return landed


def run_all(
    lookback_days=None,
    output_dir=None,
    airport_codes=None,
    adsbdb_limit=None,
    write_json=False,
    land_volume=False,
    volume_root=None,
):
    output_dir = str(output_dir or config.OUTPUT_DIR)
    run_ingestion(lookback_days, output_dir, airport_codes, adsbdb_limit, write_json)
    if land_volume:
        return land_to_volume(output_dir, volume_root=volume_root)
    return None


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--lookback-days", type=int, default=None, help="Overrides config.LOOKBACK_DAYS")
    parser.add_argument("--output-dir", type=str, default=None, help="Overrides config.OUTPUT_DIR")
    parser.add_argument(
        "--airports",
        type=str,
        default=None,
        help="Comma-separated ICAO codes, for a quick limited test (e.g. WSSS,VTBS)",
    )
    parser.add_argument(
        "--adsbdb-limit",
        type=int,
        default=None,
        help="Only process the first N callsigns via adsbdb, for a quick limited test",
    )
    parser.add_argument("--json", action="store_true", help="Also write airport_data.json")
    parser.add_argument(
        "--land-to-volume",
        action="store_true",
        help="After writing CSVs, also copy them into the Unity Catalog Volume landing folders "
        "(Databricks only - /Volumes/... doesn't exist locally)",
    )
    parser.add_argument(
        "--volume-root",
        type=str,
        default=None,
        help=f"Overrides the default /Volumes/{CATALOG}/{SCHEMA}/{VOLUME} root",
    )
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    run_all(
        lookback_days=args.lookback_days,
        output_dir=args.output_dir,
        airport_codes=args.airports,
        adsbdb_limit=args.adsbdb_limit,
        write_json=args.json,
        land_volume=args.land_to_volume,
        volume_root=args.volume_root,
    )


if __name__ == "__main__":
    main()
