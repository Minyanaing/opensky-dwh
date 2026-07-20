"""Creates the Unity Catalog landing (Bronze) objects for OpenSky ingestion.

Scope: the *landing* catalog only (`opensky_raw`, schema `bronze`, and its three tables -
flights_raw, callsigns, airports - matching what ingest_opensky.py exports). The dev/qa/prod
catalogs for the dbt Silver/Gold layers are a separate, later step.

Runs the idempotent DDL in setup.sql over a Databricks SQL Warehouse connection, authenticating
as a service principal (OAuth M2M) - or a personal access token as a fallback. This is also
what the infra-deploy.yml GitHub Actions workflow runs on merge to main. See README.md for the
one-time manual setup this depends on (creating the service principal, granting it CREATE
CATALOG on the metastore, granting it CAN_USE on the SQL warehouse).
"""

import logging
import os
from pathlib import Path

from databricks import sql
from databricks.sdk.core import Config, oauth_service_principal
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

SQL_FILE = Path(__file__).parent / "setup.sql"


def _require_env(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is not set")
    return value


def get_connection():
    server_hostname = _require_env("DATABRICKS_HOST")
    http_path = _require_env("DATABRICKS_HTTP_PATH")

    token = os.environ.get("DATABRICKS_TOKEN")
    if token:
        logger.info("Authenticating with a personal access token (fallback)")
        return sql.connect(server_hostname=server_hostname, http_path=http_path, access_token=token)

    client_id = os.environ.get("DATABRICKS_CLIENT_ID")
    client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET")
    if client_id and client_secret:
        logger.info("Authenticating as a service principal (OAuth M2M)")

        def credential_provider():
            config = Config(
                host=f"https://{server_hostname}",
                client_id=client_id,
                client_secret=client_secret,
            )
            return oauth_service_principal(config)

        return sql.connect(
            server_hostname=server_hostname, http_path=http_path, credentials_provider=credential_provider
        )

    raise RuntimeError(
        "Set DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET (service principal, preferred) "
        "or DATABRICKS_TOKEN (personal access token fallback)."
    )


def _statements(sql_text):
    """Drop comment/blank lines first, then split what's left on ';' into statements.

    Comments must be stripped *before* splitting on ';' - a semicolon inside a `--` comment
    (e.g. in prose explaining the SQL) would otherwise create a spurious statement boundary.
    """
    code_lines = [line for line in sql_text.splitlines() if line.strip() and not line.strip().startswith("--")]
    code_only = "\n".join(code_lines)
    return [statement.strip() for statement in code_only.split(";") if statement.strip()]


def run(sql_file=SQL_FILE):
    statements = _statements(sql_file.read_text(encoding="utf-8"))
    logger.info("Applying %s statement(s) from %s", len(statements), sql_file)

    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            for i, statement in enumerate(statements, start=1):
                logger.info("[%s/%s] %s", i, len(statements), statement.splitlines()[0][:100])
                cursor.execute(statement)
    finally:
        connection.close()

    logger.info("Databricks landing setup complete: opensky_raw.bronze (flights_raw, callsigns, airports)")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()


if __name__ == "__main__":
    main()
