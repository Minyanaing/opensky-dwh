"""Deploys the gold transform Job: uploads gold/*.sql (per catalog) and schedules them as a
sequential task chain. Each catalog gets its own time (see CRON_SCHEDULE_BY_CATALOG), a 1-hour gap
after that same catalog's own silver run (databricks_silver.py). Reusable driver logic lives in
databricks_transform_common.py.

dim_callsign depends on dim_airline (resolves airline_sk via a point-in-time join), so dim_airline
must run first; the other two dims are independent. fct_flight_movement depends on all four (point-
in-time FK joins), so it must run last."""

import logging
from pathlib import Path

from dotenv import load_dotenv

from databricks_transform_common import deploy_layer, layer_deploy_cli, require_env

load_dotenv()

TASK_ORDER = ["dim_airline", "dim_aircraft", "dim_airport", "dim_callsign", "fct_flight_movement"]

SQL_DIR = Path(__file__).parent / "gold"
JOB_NAME_PREFIX = "opensky-gold-transform"
WORKSPACE_ROOT = "/Shared/opensky/gold"
CRON_SCHEDULE_BY_CATALOG = {
    "dev_catalog": "0 0 12 * * ?",  # 12:00:00 daily
    "qa_catalog": "0 15 12 * * ?",  # 12:15:00 daily
    "prod_catalog": "0 30 12 * * ?",  # 12:30:00 daily
}
TIMEZONE_ID = "Asia/Bangkok"


def run(http_path, catalog):
    return deploy_layer(
        http_path, catalog, TASK_ORDER, SQL_DIR, JOB_NAME_PREFIX, WORKSPACE_ROOT,
        CRON_SCHEDULE_BY_CATALOG[catalog], TIMEZONE_ID,
    )


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = layer_deploy_cli(__doc__)
    run(require_env("DATABRICKS_HTTP_PATH"), args.catalog)


if __name__ == "__main__":
    main()
