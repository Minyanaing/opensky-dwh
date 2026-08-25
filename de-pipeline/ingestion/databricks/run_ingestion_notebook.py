# Databricks notebook source
%pip install python-dotenv

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import logging
from pathlib import Path

LOG_PATH = Path("/Volumes/opensky_raw/bronze/landing/_logs/ingest_daily.log")
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(LOG_PATH), logging.StreamHandler()],
    force=True,
)

from databricks_ingest import run_all

# COMMAND ----------

result = run_all(land_volume=True)
print(result)
