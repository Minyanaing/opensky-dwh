"""Deploys the master transform Job: uploads master/*.sql (per catalog) and schedules them as a
sequential task chain, once a year (00:00 Asia/Bangkok, Jan 1) - deployed before 2027-01-01, so
that's naturally its first run. Reusable driver logic lives in databricks_transform_common.py."""

import logging
from pathlib import Path

from dotenv import load_dotenv

from databricks_transform_common import deploy_layer, layer_deploy_cli, require_env

load_dotenv()

TASK_ORDER = ["date", "time"]

SQL_DIR = Path(__file__).parent / "master"
JOB_NAME_PREFIX = "master-transform"
WORKSPACE_ROOT = "/Shared/master"
CRON_SCHEDULE = "0 0 0 1 1 ?"  # 00:00:00, January 1st, every year
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
