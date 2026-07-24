"""Reusable Databricks Jobs deploy helpers - auth, {{CATALOG}}-substituting upload, sequential-task
orchestration, idempotent create-or-update. Shared by databricks_silver.py and databricks_gold.py
so neither duplicates this boilerplate."""

import logging
import os
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
from databricks.sdk.service.workspace import ImportFormat

logger = logging.getLogger(__name__)

CATALOGS = ["dev_catalog", "qa_catalog", "prod_catalog"]


def require_env(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is not set")
    return value


def get_workspace_client():
    host = require_env("DATABRICKS_HOST")

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


def upload_sql(client, local_path, workspace_path, catalog):
    """Substitutes {{CATALOG}} in local_path's content and uploads it to workspace_path."""
    content = Path(local_path).read_text(encoding="utf-8").replace("{{CATALOG}}", catalog)
    client.workspace.mkdirs(Path(workspace_path).parent.as_posix())
    client.workspace.upload(workspace_path, content.encode("utf-8"), format=ImportFormat.AUTO, overwrite=True)
    logger.info("Uploaded %s -> %s", local_path, workspace_path)
    return workspace_path


def build_sequential_tasks(warehouse_id, task_order, sql_paths):
    """One SqlTask per entry in task_order, each depending on the one before it - runs strictly
    in sequence, not parallel. sql_paths maps each task_order entry to its workspace SQL path."""
    tasks = []
    previous_key = None
    for key in task_order:
        tasks.append(
            jobs.Task(
                task_key=key,
                sql_task=jobs.SqlTask(
                    warehouse_id=warehouse_id,
                    file=jobs.SqlTaskFile(path=sql_paths[key], source=jobs.Source.WORKSPACE),
                ),
                depends_on=[jobs.TaskDependency(task_key=previous_key)] if previous_key else None,
            )
        )
        previous_key = key
    return tasks


def existing_job_id(client, job_name):
    for job in client.jobs.list(name=job_name):
        return job.job_id
    return None


def create_or_update_job(client, job_name, tasks, schedule):
    existing_id = existing_job_id(client, job_name)
    if existing_id:
        client.jobs.reset(
            job_id=existing_id,
            new_settings=jobs.JobSettings(name=job_name, tasks=tasks, schedule=schedule),
        )
        logger.info("Updated existing job %s (id=%s)", job_name, existing_id)
        return existing_id

    response = client.jobs.create(name=job_name, tasks=tasks, schedule=schedule)
    logger.info("Created job %s (id=%s)", job_name, response.job_id)
    return response.job_id
