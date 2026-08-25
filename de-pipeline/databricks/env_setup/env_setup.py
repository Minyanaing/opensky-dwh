"""Applies a given SQL file (e.g. setup.sql or tables.sql) over a Databricks SQL Warehouse
connection - service principal OAuth M2M, or PAT fallback. Mirrors databricks_setup.py, plus
{{CATALOG}} substitution: tables.sql is written once and stays identical across dev_catalog/
qa_catalog/prod_catalog, and --catalog picks which environment a given run applies it to."""

import argparse
import logging
import os
from pathlib import Path

from databricks import sql
from databricks.sdk.core import Config, oauth_service_principal
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

CATALOGS = ["dev_catalog", "qa_catalog", "prod_catalog"]


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
    """Drop comment/blank lines, then split on ';' - comments must go first, or a semicolon
    inside one creates a spurious split. A BEGIN...END compound statement (SQL scripting) must be
    submitted whole - splitting it on its internal ';'s would break it - so a file that starts
    with BEGIN is returned as a single statement instead."""
    code_lines = [line for line in sql_text.splitlines() if line.strip() and not line.strip().startswith("--")]
    code_only = "\n".join(code_lines).strip()
    if code_only.upper().startswith("BEGIN"):
        return [code_only]
    return [statement.strip() for statement in code_only.split(";") if statement.strip()]


def _apply_catalog(statements, catalog):
    """Substitute {{CATALOG}} with the given catalog name - lets tables.sql stay identical
    across environments; a no-op for files (like setup.sql) that don't use the placeholder."""
    if not catalog:
        return statements
    return [s.replace("{{CATALOG}}", catalog) for s in statements]


def _apply_admin_principal(statements):
    """Substitute {{ADMIN_PRINCIPAL}} from its env var, or drop the statement if unset."""
    admin_principal = os.environ.get("ADMIN_PRINCIPAL", "").strip()
    if admin_principal:
        return [s.replace("{{ADMIN_PRINCIPAL}}", f"`{admin_principal}`") for s in statements]

    kept, skipped = [], 0
    for statement in statements:
        if "{{ADMIN_PRINCIPAL}}" in statement:
            skipped += 1
        else:
            kept.append(statement)
    if skipped:
        logger.info("ADMIN_PRINCIPAL not set - skipping %s statement(s) that reference it", skipped)
    return kept


def run(sql_file, catalog=None):
    statements = _apply_admin_principal(_apply_catalog(_statements(sql_file.read_text(encoding="utf-8")), catalog))
    suffix = f" (catalog={catalog})" if catalog else ""
    logger.info("Applying %s statement(s) from %s%s", len(statements), sql_file, suffix)

    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            for i, statement in enumerate(statements, start=1):
                logger.info("[%s/%s] %s", i, len(statements), statement.splitlines()[0][:100])
                cursor.execute(statement)
    finally:
        connection.close()

    logger.info("Finished applying %s%s", sql_file.name, suffix)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sql-file",
        type=str,
        required=True,
        help="SQL file to apply, e.g. setup.sql or tables.sql",
    )
    parser.add_argument(
        "--catalog",
        type=str,
        default=None,
        choices=CATALOGS,
        help="Substitutes {{CATALOG}} in the SQL file - needed for tables.sql (run once per "
        "environment), not used by setup.sql (already names all three catalogs explicitly)",
    )
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    run(Path(args.sql_file), args.catalog)


if __name__ == "__main__":
    main()
