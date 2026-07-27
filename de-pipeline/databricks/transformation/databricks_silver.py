"""Deploys the silver transform Job: uploads silver/*.sql (per catalog) and schedules them as a
sequential task chain, 11:00 Asia/Bangkok daily. Reusable helpers live in
databricks_transform_common.py."""

import argparse
import logging
from pathlib import Path

from databricks.sdk.service import jobs
from dotenv import load_dotenv

from databricks_transform_common import (
    CATALOGS,
    build_sequential_tasks,
    create_or_update_job,
    get_workspace_client,
    require_env,
    upload_sql,
    warehouse_id_from_http_path,
)

load_dotenv()

logger = logging.getLogger(__name__)

TASK_ORDER = ["flights", "callsigns", "airlines", "airports", "aircrafts"]

SQL_DIR = Path(__file__).parent / "silver"
JOB_NAME_PREFIX = "opensky-silver-transform"
WORKSPACE_ROOT = "/Shared/opensky/silver"
CRON_SCHEDULE = "0 0 11 * * ?"  # 11:00:00 daily
TIMEZONE_ID = "Asia/Bangkok"


def run(http_path, catalog):
    client = get_workspace_client()
    warehouse_id = warehouse_id_from_http_path(http_path)

    sql_paths = {
        table: upload_sql(
            client,
            SQL_DIR / f"silver_{table}.sql",
            f"{WORKSPACE_ROOT}/{catalog}/silver_{table}.sql",
            catalog,
        )
        for table in TASK_ORDER
    }
    tasks = build_sequential_tasks(warehouse_id, TASK_ORDER, sql_paths)
    schedule = jobs.CronSchedule(quartz_cron_expression=CRON_SCHEDULE, timezone_id=TIMEZONE_ID)
    job_id = create_or_update_job(client, f"{JOB_NAME_PREFIX}-{catalog}", tasks, schedule)

    logger.info(
        "Job %s ready - runs %s daily at 11:00 %s (%s)",
        job_id,
        " -> ".join(TASK_ORDER),
        TIMEZONE_ID,
        catalog,
    )
    return job_id


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--catalog",
        type=str,
        required=True,
        choices=CATALOGS,
        help="Which environment catalog to deploy the silver job for",
    )
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    run(require_env("DATABRICKS_HTTP_PATH"), args.catalog)


if __name__ == "__main__":
    main()
