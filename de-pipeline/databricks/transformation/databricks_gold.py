"""Gold-layer transformation deploy. Reuses the auth/upload/job-orchestration helpers from
databricks_transform_common.py (get_workspace_client, warehouse_id_from_http_path, upload_sql,
build_sequential_tasks, create_or_update_job) rather than duplicating them - see that file for
what each does.

TASK_ORDER/SQL_DIR/JOB_NAME_PREFIX/schedule below are placeholders - the gold-layer SQL files and
their run order haven't been defined yet (transformation/gold/ doesn't exist). Fill these in once
that's decided; the reusable helpers already support whatever shape gold ends up needing."""

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

# TODO: fill in once the gold-layer tables and their run order are decided.
TASK_ORDER = []

SQL_DIR = Path(__file__).parent / "gold"
JOB_NAME_PREFIX = "opensky-gold-transform"
WORKSPACE_ROOT = "/Shared/opensky/gold"
CRON_SCHEDULE = None  # TODO: Quartz cron expression, e.g. "0 8 9 * * ?" for 09:08 daily
TIMEZONE_ID = "Asia/Bangkok"


def run(http_path, catalog):
    if not TASK_ORDER or not CRON_SCHEDULE:
        raise NotImplementedError(
            "Gold-layer TASK_ORDER/CRON_SCHEDULE not yet defined - see the TODOs in databricks_gold.py"
        )

    client = get_workspace_client()
    warehouse_id = warehouse_id_from_http_path(http_path)

    sql_paths = {
        table: upload_sql(
            client,
            SQL_DIR / f"gold_{table}.sql",
            f"{WORKSPACE_ROOT}/{catalog}/gold_{table}.sql",
            catalog,
        )
        for table in TASK_ORDER
    }
    tasks = build_sequential_tasks(warehouse_id, TASK_ORDER, sql_paths)
    schedule = jobs.CronSchedule(quartz_cron_expression=CRON_SCHEDULE, timezone_id=TIMEZONE_ID)
    job_id = create_or_update_job(client, f"{JOB_NAME_PREFIX}-{catalog}", tasks, schedule)

    logger.info(
        "Job %s ready - runs %s daily %s (%s)",
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
        help="Which environment catalog to deploy the gold job for",
    )
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    run(require_env("DATABRICKS_HTTP_PATH"), args.catalog)


if __name__ == "__main__":
    main()
