"""Silver-layer transformation deploy. Reuses the auth/upload/job-orchestration helpers from
databricks_transform_common.py rather than duplicating them.

Substitutes {{CATALOG}} in each transformation/silver/*.sql file and uploads it to the Databricks
workspace (/Shared/opensky/silver/<catalog>/), then creates/updates one Databricks Job that runs
them as sequential SQL tasks - flights -> callsigns -> airlines -> airports -> aircrafts, each
depending on the one before it so they run strictly one after another, not in parallel - on a
daily cron schedule (09:08 Asia/Bangkok).

--catalog is required (dev_catalog/qa_catalog/prod_catalog): this is one shared workspace across
all three environments (see env_setup.py), so both the uploaded path and the job name are scoped
per catalog - otherwise deploying qa_catalog would silently overwrite dev_catalog's job and SQL.

Unlike databricks/landing/landing.py (one Job per table, file-arrival triggered), this is one Job
with an ordered task chain, cron triggered - the silver layer runs on a schedule, not on upload."""

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

# Order matters - each task depends on the one before it, so they run strictly in sequence.
TASK_ORDER = ["flights", "callsigns", "airlines", "airports", "aircrafts"]

SQL_DIR = Path(__file__).parent / "silver"
JOB_NAME_PREFIX = "opensky-silver-transform"
WORKSPACE_ROOT = "/Shared/opensky/silver"
CRON_SCHEDULE = "0 8 9 * * ?"  # Quartz: sec min hour day month dow - 09:08:00 daily
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
        "Job %s ready - runs %s daily at 09:08 %s (%s)",
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
