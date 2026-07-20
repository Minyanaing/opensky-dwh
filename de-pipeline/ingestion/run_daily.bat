@echo off
REM Daily local ingestion - point Windows Task Scheduler at this file.
REM Runs ingest_opensky.py in INGEST_MODE=databricks, landing straight into opensky_raw.bronze.
cd /d "%~dp0"
call .venv\Scripts\activate.bat
set INGEST_MODE=databricks
python ingest_opensky.py >> ingest_daily.log 2>&1
