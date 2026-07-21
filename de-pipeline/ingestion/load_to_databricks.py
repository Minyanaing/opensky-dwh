"""Uploads ingest_opensky.py's local CSV exports (flights_raw.csv, callsigns.csv, airports.csv)
to the opensky_raw.bronze.landing Unity Catalog Volume.

Replaces the previous row-by-row SQL INSERT approach: databricks-sql-connector's executemany()
turned out to issue one network round-trip per row internally (confirmed against
databricks/databricks-sql-python#196), making it far too slow for any real data volume. A file
upload to a Volume is one fast operation regardless of row count.

This only uploads files - it does not load them into the flights_raw/callsigns/airports tables.
That's a separate step, COPY INTO, run manually or on a schedule - see README.md.
"""

import argparse
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from databricks.sdk import WorkspaceClient

import config

logger = logging.getLogger(__name__)

CATALOG = "opensky_raw"
SCHEMA = "bronze"
VOLUME = "landing"

# (table name, local filename) - one file per table, matching ingest_opensky.py's output.
FILES = [
    ("flights_raw", "flights_raw.csv"),
    ("callsigns", "callsigns.csv"),
    ("airports", "airports.csv"),
]


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


def upload_file(client, local_path, table):
    """Upload one local file to /Volumes/opensky_raw/bronze/landing/<table>/<timestamped-name>."""
    local_path = Path(local_path)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    remote_name = f"{local_path.stem}_{stamp}{local_path.suffix}"
    volume_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{table}/{remote_name}"

    with open(local_path, "rb") as f:
        client.files.upload(volume_path, f, overwrite=True)

    logger.info("Uploaded %s -> %s", local_path, volume_path)
    return volume_path


def run(input_dir):
    in_dir = Path(input_dir)
    client = get_workspace_client()

    uploaded = {}
    for table, filename in FILES:
        local_path = in_dir / filename
        if not local_path.is_file():
            logger.warning("%s not found, skipping", local_path)
            continue
        uploaded[table] = upload_file(client, local_path, table)

    logger.info("Uploaded %s file(s) to the landing volume", len(uploaded))
    return uploaded


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-dir",
        type=str,
        default=str(config.OUTPUT_DIR),
        help="Directory holding ingest_opensky.py's flights_raw.csv/callsigns.csv/airports.csv "
        "(default: %(default)s)",
    )
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("databricks").setLevel(logging.WARNING)
    args = parse_args()
    run(args.input_dir)


if __name__ == "__main__":
    main()
