"""Creates (or replaces) one Snowflake PIPE per Bronze table, from sql/<table>.sql - mirrors
de-pipeline/databricks/landing/landing.py's one-Job-per-table split, but as plain SQL: a
Snowflake PIPE is a SQL object (CREATE PIPE ... AS COPY INTO ...), no SDK/Job-API needed.

AUTO_INGEST is deliberately FALSE: Snowflake's automatic file-arrival ingestion for an INTERNAL
stage is only available on AWS-hosted accounts (and even then needs cloud-side event
notifications configured outside Snowflake) - not portable, and not something this script can
set up on its own. Instead, de-pipeline/ingestion/load_to_snowflake.py calls
`ALTER PIPE ... REFRESH` immediately after each upload, which achieves the same "load on
upload" outcome for any account/cloud, using only SQL over the same connection.

Run once (idempotent - CREATE OR REPLACE) to create all 5 pipes, and again any time a
sql/*.sql file or TABLES changes.
"""

import logging
import os
from pathlib import Path

import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from dotenv import find_dotenv, load_dotenv

load_dotenv()
_DOTENV_PATH = find_dotenv()

logger = logging.getLogger(__name__)

TABLES = ["flights_raw", "callsigns", "airlines", "airports", "aircrafts"]

SQL_DIR = Path(__file__).parent / "sql"


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


def _local_sql_path(table):
    return SQL_DIR / f"{table}.sql"


def create_pipe(cursor, table):
    sql_path = _local_sql_path(table)
    statement = sql_path.read_text(encoding="utf-8")
    cursor.execute(statement)
    logger.info("Created/replaced pipe for %s (from %s)", table, sql_path)


def run():
    connection = get_connection()
    try:
        cursor = connection.cursor()
        for table in TABLES:
            create_pipe(cursor, table)
    finally:
        connection.close()

    logger.info(
        "%s pipe(s) ready - each loads only its own table, AUTO_INGEST=FALSE (see module docstring)", len(TABLES)
    )


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    # snowflake-connector-python bundles boto3/botocore for internal-stage transfers; if the
    # machine has an unrelated, expired AWS SSO session cached, botocore logs a noisy
    # background refresh-failure warning that has nothing to do with Snowflake auth.
    logging.getLogger("botocore").setLevel(logging.ERROR)
    logging.getLogger("boto3").setLevel(logging.ERROR)
    run()


if __name__ == "__main__":
    main()
