"""Deploys the gold transform Job: uploads gold/*.sql (per catalog) and schedules them as a
sequential task chain, 11:30 Asia/Bangkok daily (30 min after silver's 11:00 run - adjust once
silver's actual runtime is known). Reusable driver logic lives in databricks_transform_common.py.

Dimensions have no dependency on each other; fct_flight_movement depends on all four (it resolves
FKs by joining to each dim's is_current row), so it must run last."""

import logging
from pathlib import Path

from dotenv import load_dotenv

from databricks_transform_common import deploy_layer, layer_deploy_cli, require_env

load_dotenv()

TASK_ORDER = ["dim_airline", "dim_aircraft", "dim_airport", "dim_callsign", "fct_flight_movement"]

SQL_DIR = Path(__file__).parent / "gold"
JOB_NAME_PREFIX = "opensky-gold-transform"
WORKSPACE_ROOT = "/Shared/opensky/gold"
CRON_SCHEDULE = "0 30 11 * * ?"  # 11:30:00 daily
TIMEZONE_ID = "Asia/Bangkok"


def run(http_path, catalog):
    return deploy_layer(
        http_path, catalog, TASK_ORDER, SQL_DIR, JOB_NAME_PREFIX, WORKSPACE_ROOT, CRON_SCHEDULE, TIMEZONE_ID
    )


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = layer_deploy_cli(__doc__)
    run(require_env("DATABRICKS_HTTP_PATH"), args.catalog)


if __name__ == "__main__":
    main()
