"""Deploys the silver transform Job: uploads silver/*.sql (per catalog) and schedules them as a
sequential task chain, 11:00 Asia/Bangkok daily. Reusable driver logic lives in
databricks_transform_common.py."""

import logging
from pathlib import Path

from dotenv import load_dotenv

from databricks_transform_common import deploy_layer, layer_deploy_cli, require_env

load_dotenv()

TASK_ORDER = ["flights", "callsigns", "airlines", "airports", "aircrafts"]

SQL_DIR = Path(__file__).parent / "silver"
JOB_NAME_PREFIX = "opensky-silver-transform"
WORKSPACE_ROOT = "/Shared/opensky/silver"
CRON_SCHEDULE = "0 0 11 * * ?"  # 11:00:00 daily
TIMEZONE_ID = "Asia/Bangkok"


def run(http_path, catalog):
    return deploy_layer(
        http_path, catalog, TASK_ORDER, SQL_DIR, JOB_NAME_PREFIX, WORKSPACE_ROOT, CRON_SCHEDULE, TIMEZONE_ID,
        file_prefix="silver_",
    )


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = layer_deploy_cli(__doc__)
    run(require_env("DATABRICKS_HTTP_PATH"), args.catalog)


if __name__ == "__main__":
    main()
