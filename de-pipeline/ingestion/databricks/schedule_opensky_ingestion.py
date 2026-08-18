"""Creates (or updates) a Databricks Job that runs databricks_ingest.py daily at 10:00
Asia/Bangkok time, with --land-to-volume so the run lands CSVs in the Volume. Run
deploy_opensky_ingestion.py first - this only wires up the schedule, it doesn't upload code.

Untested assumption: a plain Python-file task needs no cluster/warehouse spec on Free Edition
serverless job compute, same as this project's existing SQL-warehouse jobs needed no cluster.
Verify the first run in the Databricks Jobs UI.
"""

import logging
import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

JOB_NAME = "opensky-ingestion-daily"
WORKSPACE_DIR = "/Shared/opensky/ingestion"
PYTHON_FILE = f"{WORKSPACE_DIR}/databricks/databricks_ingest.py"
CRON_SCHEDULE = "0 0 10 * * ?"
TIMEZONE_ID = "Asia/Bangkok"
TASK_PARAMETERS = ["--land-to-volume"]


def _require_env(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is not set")
    return value


def get_workspace_client():
    host = _require_env("DATABRICKS_HOST")

    token = os.environ.get("DATABRICKS_TOKEN")
    if token:
        logger.info("Authenticating with a personal access token (fallback)")
        return WorkspaceClient(host=f"https://{host}", token=token)

    client_id = os.environ.get("DATABRICKS_CLIENT_ID")
    client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET")
    if client_id and client_secret:
        logger.info("Authenticating as a service principal (OAuth M2M)")
        return WorkspaceClient(host=f"https://{host}", client_id=client_id, client_secret=client_secret)

    raise RuntimeError(
        "Set DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET (service principal, preferred) "
        "or DATABRICKS_TOKEN (personal access token fallback)."
    )


def _existing_job_id(client, job_name):
    for job in client.jobs.list(name=job_name):
        return job.job_id
    return None


def build_task():
    return jobs.Task(
        task_key="run_ingestion",
        spark_python_task=jobs.SparkPythonTask(
            python_file=PYTHON_FILE,
            source=jobs.Source.WORKSPACE,
            parameters=TASK_PARAMETERS,
        ),
    )


def create_or_update_job(client):
    task = build_task()
    schedule = jobs.CronSchedule(quartz_cron_expression=CRON_SCHEDULE, timezone_id=TIMEZONE_ID)

    existing_id = _existing_job_id(client, JOB_NAME)
    if existing_id:
        client.jobs.reset(
            job_id=existing_id,
            new_settings=jobs.JobSettings(name=JOB_NAME, tasks=[task], schedule=schedule),
        )
        logger.info("Updated existing job %s (id=%s)", JOB_NAME, existing_id)
        return existing_id

    response = client.jobs.create(name=JOB_NAME, tasks=[task], schedule=schedule)
    logger.info("Created job %s (id=%s)", JOB_NAME, response.job_id)
    return response.job_id


def run():
    client = get_workspace_client()
    return create_or_update_job(client)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()


if __name__ == "__main__":
    main()
