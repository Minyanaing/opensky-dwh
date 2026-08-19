"""Uploads only the files databricks_ingest.py needs (not the whole ingestion folder) to a
Databricks Workspace folder, preserving the common/pipeline/loaders/databricks layout so its
sys.path bootstrap resolves the same way it does locally.

Does not upload .env - that's not checked out by CI (see .gitignore) and is uploaded
separately/manually.
"""

import logging
import os
import posixpath
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parent.parent
WORKSPACE_DIR = "/Shared/opensky/ingestion"

REQUIRED_FILES = [
    "common/config.py",
    "common/transforms.py",
    "common/fetch_data.py",
    "pipeline/ingest_opensky.py",
    "pipeline/export_airports.py",
    "pipeline/export_callsigns.py",
    "pipeline/ingest_adsbdb.py",
    "pipeline/ingest_airports.py",
    "loaders/load_to_databricks.py",
    "databricks/databricks_ingest.py",
]

# Uploaded as an actual Notebook (ImportFormat.SOURCE), not a plain workspace file - the .py
# suffix is dropped from the workspace path, matching how Databricks stores notebook objects.
NOTEBOOK_FILE = "databricks/run_ingestion_notebook.py"
NOTEBOOK_WORKSPACE_PATH = "databricks/run_ingestion_notebook"


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


def upload_file(client, relative_path, root_dir=ROOT_DIR, workspace_dir=WORKSPACE_DIR):
    local_path = root_dir / relative_path
    remote_path = posixpath.join(workspace_dir, relative_path.replace("\\", "/"))
    client.workspace.mkdirs(posixpath.dirname(remote_path))
    client.workspace.upload(remote_path, local_path.read_bytes(), format=ImportFormat.AUTO, overwrite=True)
    logger.info("Uploaded %s -> %s", local_path, remote_path)
    return remote_path


def upload_notebook(client, root_dir=ROOT_DIR, workspace_dir=WORKSPACE_DIR):
    local_path = root_dir / NOTEBOOK_FILE
    remote_path = posixpath.join(workspace_dir, NOTEBOOK_WORKSPACE_PATH)
    client.workspace.mkdirs(posixpath.dirname(remote_path))
    client.workspace.upload(
        remote_path, local_path.read_bytes(), format=ImportFormat.SOURCE, language=Language.PYTHON, overwrite=True
    )
    logger.info("Uploaded %s -> %s (notebook)", local_path, remote_path)
    return remote_path


def run(files=REQUIRED_FILES, root_dir=ROOT_DIR, workspace_dir=WORKSPACE_DIR):
    client = get_workspace_client()
    uploaded = [upload_file(client, f, root_dir, workspace_dir) for f in files]
    uploaded.append(upload_notebook(client, root_dir, workspace_dir))
    logger.info("Uploaded %s file(s) -> %s", len(uploaded), workspace_dir)
    return uploaded


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()


if __name__ == "__main__":
    main()
