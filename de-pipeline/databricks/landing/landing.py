"""Creates (or updates) one Databricks Job per Bronze table, each with its own file-arrival
trigger scoped to that table's landing-volume folder - landing a file in one folder only runs
that folder's COPY INTO, never all of them. No Auto Loader/streaming needed: Databricks Jobs
support a file-arrival trigger directly on a Unity Catalog Volume (sub-)path.

Each table's COPY INTO lives in its own file under sql/<table>.sql, uploaded as-is to its own
workspace path. Run once (idempotent by job name) to create all 5 jobs, and again any time a
sql/*.sql file or TABLES changes - this re-uploads it and resets that table's job.
"""

import logging
import os
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
from databricks.sdk.service.workspace import ImportFormat
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# (table, volume folder) - matches load_to_databricks.py's DATASETS mapping. Each table's COPY
# INTO lives in sql/<table>.sql.
TABLES = [
    ("flights_raw", "flights_raw"),
    ("callsigns", "callsigns"),
    ("airlines", "airlines"),
    ("airports", "airports"),
    ("aircrafts", "aircrafts"),
]

SQL_DIR = Path(__file__).parent / "sql"
JOB_NAME_PREFIX = "opensky-landing-copy-into"
WORKSPACE_DIR = "/Shared/opensky"
VOLUME_ROOT = "/Volumes/opensky_raw/bronze/landing"


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


def warehouse_id_from_http_path(http_path):
    """DATABRICKS_HTTP_PATH is /sql/1.0/warehouses/<id> - reuse it instead of a separate env var."""
    return http_path.rstrip("/").rsplit("/", 1)[-1]


def _job_name(table):
    return f"{JOB_NAME_PREFIX}-{table}"


def _workspace_sql_path(table):
    return f"{WORKSPACE_DIR}/copy_into_{table}.sql"


def _local_sql_path(table):
    return SQL_DIR / f"{table}.sql"


def upload_sql(client, table):
    local_path = _local_sql_path(table)
    content = local_path.read_bytes()
    client.workspace.mkdirs(WORKSPACE_DIR)
    path = _workspace_sql_path(table)
    client.workspace.upload(path, content, format=ImportFormat.AUTO, overwrite=True)
    logger.info("Uploaded %s -> %s", local_path, path)
    return path


def _existing_job_id(client, job_name):
    for job in client.jobs.list(name=job_name):
        return job.job_id
    return None


def build_task(warehouse_id, sql_path):
    return jobs.Task(
        task_key="copy_into",
        sql_task=jobs.SqlTask(
            warehouse_id=warehouse_id,
            file=jobs.SqlTaskFile(path=sql_path, source=jobs.Source.WORKSPACE),
        ),
    )


def build_trigger(folder):
    return jobs.TriggerSettings(file_arrival=jobs.FileArrivalTriggerConfiguration(url=f"{VOLUME_ROOT}/{folder}/"))


def create_or_update_job(client, table, folder, warehouse_id, sql_path):
    job_name = _job_name(table)
    task = build_task(warehouse_id, sql_path)
    trigger = build_trigger(folder)

    existing_id = _existing_job_id(client, job_name)
    if existing_id:
        client.jobs.reset(
            job_id=existing_id,
            new_settings=jobs.JobSettings(name=job_name, tasks=[task], trigger=trigger),
        )
        logger.info("Updated existing job %s (id=%s)", job_name, existing_id)
        return existing_id

    response = client.jobs.create(name=job_name, tasks=[task], trigger=trigger)
    logger.info("Created job %s (id=%s)", job_name, response.job_id)
    return response.job_id


def run(http_path):
    client = get_workspace_client()
    warehouse_id = warehouse_id_from_http_path(http_path)

    job_ids = {}
    for table, folder in TABLES:
        sql_path = upload_sql(client, table)
        job_ids[table] = create_or_update_job(client, table, folder, warehouse_id, sql_path)

    logger.info("%s job(s) ready - each fires independently when its own folder gets a new file", len(job_ids))
    return job_ids


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run(_require_env("DATABRICKS_HTTP_PATH"))


if __name__ == "__main__":
    main()
