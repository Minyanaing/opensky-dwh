@echo off
REM Daily local ingestion - point Windows Task Scheduler at this file.
REM Runs in sequence: fetch from OpenSky -> extract new-only airports/callsigns -> enrich via
REM adsbdb -> enrich via airport-data.com -> upload CSVs to the Databricks landing volume. COPY
REM INTO'ing them into the actual tables is a separate, manual step - see README.md. Aircraft data
REM is loaded manually (--aircraft) when needed, not part of this run.
cd /d "%~dp0"

for /f "delims=" %%i in ('powershell -NoProfile -Command "Get-Date -Format \"yyyy-MM-dd HH:mm:ss\""') do set "NOW=%%i"

echo == >> ingest_daily.log
echo == >> ingest_daily.log
echo =========================================================== >> ingest_daily.log
echo ==================== Start Time: %NOW% ==================== >> ingest_daily.log

call .venv\Scripts\activate.bat

echo == >> ingest_daily.log
echo ---- ingest_opensky.py ---- >> ingest_daily.log
python ingest_opensky.py >> ingest_daily.log 2>&1

echo == >> ingest_daily.log
echo ---- export_airports.py ---- >> ingest_daily.log
python export_airports.py >> ingest_daily.log 2>&1

echo == >> ingest_daily.log
echo ---- export_callsigns.py ---- >> ingest_daily.log
python export_callsigns.py >> ingest_daily.log 2>&1

echo == >> ingest_daily.log
echo ---- ingest_adsbdb.py ---- >> ingest_daily.log
python ingest_adsbdb.py >> ingest_daily.log 2>&1

echo == >> ingest_daily.log
echo ---- ingest_airports.py ---- >> ingest_daily.log
python ingest_airports.py >> ingest_daily.log 2>&1

echo == >> ingest_daily.log
echo ---- load_to_databricks.py ---- >> ingest_daily.log
python load_to_databricks.py flights_raw airlines airports callsigns airport_data >> ingest_daily.log 2>&1

echo == >> ingest_daily.log
echo ---- load_to_snowflake.py ---- >> ingest_daily.log
python load_to_snowflake.py flights_raw airlines airports callsigns >> ingest_daily.log 2>&1

echo ==========###################################========== >> ingest_daily.log
