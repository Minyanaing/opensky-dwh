@echo off
REM Daily local ingestion - point Windows Task Scheduler at this file.
REM Step 1: fetch from OpenSky, write local CSVs. Step 2: upload those CSVs to the Databricks
REM landing volume. COPY INTO'ing them into the actual tables is a separate, manual step - see
REM README.md.
cd /d "%~dp0"

for /f "delims=" %%i in ('powershell -NoProfile -Command "Get-Date -Format \"yyyy-MM-dd HH:mm:ss\""') do set "NOW=%%i"
echo ==================== Start Time: %NOW% ==================== >> ingest_daily.log

call .venv\Scripts\activate.bat
python ingest_opensky.py >> ingest_daily.log 2>&1
python load_to_databricks.py >> ingest_daily.log 2>&1
