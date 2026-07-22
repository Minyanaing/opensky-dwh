"""Applies one or more SQL files, in order, over a single Snowflake connection - key pair (JWT)
auth via SNOWFLAKE_ACCOUNT / SNOWFLAKE_DEV_SVC_USER / SNOWFLAKE_DEV_SVC_PRIVATE_KEY.

  python snowflake_setup.py setup.sql
  python snowflake_setup.py destroy.sql
  python snowflake_setup.py setup.sql some_future_step.sql   # any new SQL file just adds on

No fixed "setup" vs "destroy" mode - each file is just a positional arg, applied in the order
given, so a new SQL file never needs a code change to be runnable."""

import argparse
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


def _require_env(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is not set")
    return value


def _resolve_key_path(raw_path):
    """A relative path is resolved against the .env file's own directory, not the current
    working directory - so it still works after `cd setup` (or from any other script's cwd)."""
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


def _statements(sql_text):
    """Drop comment/blank lines, then split on ';' - comments must go first, or a semicolon
    inside one creates a spurious split."""
    code_lines = [line for line in sql_text.splitlines() if line.strip() and not line.strip().startswith("--")]
    code_only = "\n".join(code_lines)
    return [statement.strip() for statement in code_only.split(";") if statement.strip()]


def run_file(cursor, sql_file):
    statements = _statements(Path(sql_file).read_text(encoding="utf-8"))
    logger.info("=== %s (%s statement(s)) ===", sql_file, len(statements))
    for i, statement in enumerate(statements, start=1):
        logger.info("[%s/%s] %s", i, len(statements), statement.splitlines()[0][:100])
        cursor.execute(statement)
    logger.info("Finished %s", sql_file)


def run(sql_files):
    connection = get_connection()
    try:
        cursor = connection.cursor()
        for sql_file in sql_files:
            run_file(cursor, sql_file)
    finally:
        connection.close()


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "sql_files",
        nargs="+",
        help="One or more SQL files to apply, in order (e.g. setup.sql, destroy.sql, or any new file)",
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
    run(args.sql_files)


if __name__ == "__main__":
    main()
