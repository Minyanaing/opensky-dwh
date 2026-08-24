-- {{CATALOG}} substituted per environment. Generates 2026-01-01 through Dec 31 of the current
-- year; re-running (once a year, via databricks_master.py) only inserts the newly-extended year -
-- prior years already match on date_sk and are left alone.
MERGE INTO {{CATALOG}}.master.date AS target
USING (
  SELECT
    CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_sk,
    d AS date,
    year(d) AS year,
    quarter(d) AS quarter,
    month(d) AS month,
    CAST(date_format(d, 'yyyyMM') AS INT) AS year_month,
    weekofyear(d) AS week_num,
    day(d) AS day,
    concat('Q', quarter(d)) AS quarter_name,
    date_format(d, 'MMMM') AS month_name,
    date_format(d, 'MMM') AS month_name_short,
    dayofweek(d) - 1 AS days_of_week_num,
    date_format(d, 'EEEE') AS days_of_weeks,
    date_format(d, 'EEE') AS days_of_weeks_short
  FROM (
    SELECT explode(sequence(
      to_date('2026-01-01'),
      make_date(year(current_date()), 12, 31),
      interval 1 day
    )) AS d
  )
) AS source
ON target.date_sk = source.date_sk
WHEN NOT MATCHED THEN INSERT *;
