"""Uploads local CSV exports to the opensky_raw.bronze.landing Unity Catalog Volume, one
sub-folder per dataset. Which dataset(s) to upload is required on the command line:

  python load_to_databricks.py flights_raw airlines airports callsigns aircrafts airport_data
  python load_to_databricks.py flights_raw callsigns   # just these two
  python load_to_databricks.py airports_master         # one-off/occasional master reload

Dataset name -> (local file, volume folder):
  flights_raw     -> flights_raw.csv       -> flights_raw/
  airlines        -> adsbdb_airlines.csv    -> airlines/
  airports        -> adsbdb_airports.csv    -> airports/
  callsigns       -> adsbdb_callsigns.csv   -> callsigns/
  aircrafts       -> adsbdb_aircraft.csv    -> aircrafts/
  airport_data    -> airport_data.csv       -> airport_data/
  airports_master -> master_airports.csv    -> airports_master/  (not part of the daily run - see
                                                                   run_daily.bat)

This only uploads files - it does not load them into any table. That's a separate step,
COPY INTO, run manually or on a schedule - see README.md.
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from databricks.sdk import WorkspaceClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "common"))

import config

logger = logging.getLogger(__name__)

CATALOG = "opensky_raw"
SCHEMA = "bronze"
VOLUME = "landing"

# dataset name -> (local filename, volume folder)
DATASETS = {
    "flights_raw": ("flights_raw.csv", "flights_raw"),
    "airlines": ("adsbdb_airlines.csv", "airlines"),
    "airports": ("adsbdb_airports.csv", "airports"),
    "callsigns": ("adsbdb_callsigns.csv", "callsigns"),
    "aircrafts": ("adsbdb_aircraft.csv", "aircrafts"),
    "airport_data": ("airport_data.csv", "airport_data"),
    "airports_master": ("master_airports.csv", "airports_master"),
}


def _require_env(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is not set")
    return value


def get_workspace_client():
    host = _require_env("DATABRICKS_HOST")

    token = os.environ.get("DATABRICKS_TOKEN")
    if token:
        logger.info("Authenticating with a personal access token (fallback)")
        return WorkspaceClient(host=f"https://{host}", token=token)

    client_id = os.environ.get("DATABRICKS_CLIENT_ID")
    client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET")
    if client_id and client_secret:
        logger.info("Authenticating as a service principal (OAuth M2M)")
        return WorkspaceClient(host=f"https://{host}", client_id=client_id, client_secret=client_secret)

    raise RuntimeError(
        "Set DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET (service principal, preferred) "
        "or DATABRICKS_TOKEN (personal access token fallback)."
    )


def upload_file(client, local_path, folder):
    """Upload one local file to /Volumes/opensky_raw/bronze/landing/<folder>/<timestamped-name>."""
    local_path = Path(local_path)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    remote_name = f"{local_path.stem}_{stamp}{local_path.suffix}"
    volume_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{folder}/{remote_name}"

    with open(local_path, "rb") as f:
        client.files.upload(volume_path, f, overwrite=True)

    logger.info("Uploaded %s -> %s", local_path, volume_path)
    return volume_path


def run(input_dir, datasets):
    in_dir = Path(input_dir)
    client = get_workspace_client()

    uploaded = {}
    for dataset in datasets:
        filename, folder = DATASETS[dataset]
        local_path = in_dir / filename
        if not local_path.is_file():
            logger.warning("%s not found, skipping %s", local_path, dataset)
            continue
        uploaded[dataset] = upload_file(client, local_path, folder)

    logger.info("Uploaded %s file(s) to the landing volume", len(uploaded))
    return uploaded


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "datasets",
        nargs="+",
        choices=list(DATASETS.keys()),
        help="Which dataset(s) to upload: " + ", ".join(DATASETS.keys()),
    )
    parser.add_argument(
        "--input-dir",
        type=str,
        default=str(config.OUTPUT_DIR),
        help="Directory holding the local CSV exports (default: %(default)s)",
    )
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("databricks").setLevel(logging.WARNING)
    args = parse_args()
    run(args.input_dir, args.datasets)


if __name__ == "__main__":
    main()
