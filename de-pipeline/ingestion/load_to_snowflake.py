"""Uploads local CSV exports to the OPENSKY_DB.BRONZE.LANDING Snowflake internal stage, one
timestamped file per dataset - mirrors load_to_databricks.py exactly: this only uploads files,
it does not load them into any table. That's handled separately, either by AUTO_INGEST (if
active for the account - see snowflake/landing/landing.py) or a manual `ALTER PIPE ... REFRESH`.

  python load_to_snowflake.py flights_raw airlines airports callsigns aircrafts
  python load_to_snowflake.py flights_raw callsigns   # just these two

Dataset name -> (local file, stage folder):
  flights_raw -> flights_raw.csv       -> flights_raw/
  airlines    -> adsbdb_airlines.csv   -> airlines/
  airports    -> adsbdb_airports.csv   -> airports/
  callsigns   -> adsbdb_callsigns.csv  -> callsigns/
  aircrafts   -> adsbdb_aircraft.csv   -> aircrafts/
"""

import argparse
import logging
import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from dotenv import find_dotenv, load_dotenv

import config

load_dotenv()
_DOTENV_PATH = find_dotenv()

logger = logging.getLogger(__name__)

DATABASE = "OPENSKY_DB"
SCHEMA = "BRONZE"
STAGE = "LANDING"

# dataset name -> (local filename, stage folder)
DATASETS = {
    "flights_raw": ("flights_raw.csv", "flights_raw"),
    "airlines": ("adsbdb_airlines.csv", "airlines"),
    "airports": ("adsbdb_airports.csv", "airports"),
    "callsigns": ("adsbdb_callsigns.csv", "callsigns"),
    "aircrafts": ("adsbdb_aircraft.csv", "aircrafts"),
}


def _require_env(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is not set")
    return value


def _resolve_key_path(raw_path):
    """A relative path is resolved against the .env file's own directory, not the current
    working directory - so it still works regardless of which script/cwd loads it."""
    path = Path(raw_path)
    if path.is_absolute():
        return path
    anchor = Path(_DOTENV_PATH).parent if _DOTENV_PATH else Path.cwd()
    return (anchor / path).resolve()


def _load_private_key():
    raw = _require_env("SNOWFLAKE_DEV_SVC_PRIVATE_KEY").strip()
    if raw.startswith("-----BEGIN"):
        pem_data = raw.encode()
    else:
        key_path = _resolve_key_path(raw)
        logger.info("Reading private key from %s", key_path)
        pem_data = key_path.read_bytes()
    passphrase = os.environ.get("SNOWFLAKE_DEV_SVC_PRIVATE_KEY_PASSPHRASE")
    key = serialization.load_pem_private_key(
        pem_data,
        password=passphrase.encode() if passphrase else None,
        backend=default_backend(),
    )
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def get_connection():
    kwargs = {
        "account": _require_env("SNOWFLAKE_ACCOUNT"),
        "user": _require_env("SNOWFLAKE_DEV_SVC_USER"),
        "private_key": _load_private_key(),
    }
    warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE")
    if warehouse:
        kwargs["warehouse"] = warehouse
    role = os.environ.get("SNOWFLAKE_ROLE")
    if role:
        kwargs["role"] = role

    logger.info("Authenticating as %s (key pair)", kwargs["user"])
    return snowflake.connector.connect(**kwargs)


def _file_uri(local_path):
    """PUT needs a file:// URI - Windows paths (C:/...) need one leading slash added,
    POSIX paths (/tmp/...) already have it."""
    posix = Path(local_path).resolve().as_posix()
    if not posix.startswith("/"):
        posix = "/" + posix
    return f"file://{posix}"


def upload_file(cursor, local_path, folder):
    """Upload one local file to @OPENSKY_DB.BRONZE.LANDING/<folder>/<timestamped-name>. PUT
    always uses the source file's own basename, so the timestamped name comes from a temp copy."""
    local_path = Path(local_path)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    remote_name = f"{local_path.stem}_{stamp}{local_path.suffix}"
    stage_path = f"@{DATABASE}.{SCHEMA}.{STAGE}/{folder}/"

    with tempfile.TemporaryDirectory() as tmp_dir:
        renamed_path = Path(tmp_dir) / remote_name
        shutil.copy2(local_path, renamed_path)
        cursor.execute(f"PUT '{_file_uri(renamed_path)}' {stage_path} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")

    logger.info("Uploaded %s -> %s%s", local_path, stage_path, remote_name)


def run(input_dir, datasets):
    in_dir = Path(input_dir)
    connection = get_connection()

    uploaded = {}
    try:
        cursor = connection.cursor()
        for dataset in datasets:
            filename, folder = DATASETS[dataset]
            local_path = in_dir / filename
            if not local_path.is_file():
                logger.warning("%s not found, skipping %s", local_path, dataset)
                continue
            upload_file(cursor, local_path, folder)
            uploaded[dataset] = folder
    finally:
        connection.close()

    logger.info("Uploaded %s dataset(s)", len(uploaded))
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
    # snowflake-connector-python bundles boto3/botocore for internal-stage transfers; if the
    # machine has an unrelated, expired AWS SSO session cached, botocore logs a noisy
    # background refresh-failure warning that has nothing to do with Snowflake auth.
    logging.getLogger("botocore").setLevel(logging.ERROR)
    logging.getLogger("boto3").setLevel(logging.ERROR)
    args = parse_args()
    run(args.input_dir, args.datasets)


if __name__ == "__main__":
    main()
