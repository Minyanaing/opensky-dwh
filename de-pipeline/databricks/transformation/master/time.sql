-- {{CATALOG}} substituted per environment. Static (all 1440 minutes of a day) - MERGE is a no-op
-- after the first run.
MERGE INTO {{CATALOG}}.master.time AS target
USING (
  SELECT
    hour * 100 + minute AS time_sk,
    concat(lpad(hour, 2, '0'), ':', lpad(minute, 2, '0'), ':00') AS time,
    hour,
    minute,
    CASE WHEN hour < 12 THEN 'AM' ELSE 'PM' END AS am_pm,
    concat(hour, '-', hour + 1) AS hourly_range,
    concat(CAST(floor(hour / 2) * 2 AS INT), '-', CAST(floor(hour / 2) * 2 + 2 AS INT)) AS two_hour_range
  FROM (
    SELECT
      CAST(floor(m / 60) AS INT) AS hour,
      CAST(m % 60 AS INT) AS minute
    FROM (SELECT explode(sequence(0, 1439)) AS m)
  )
) AS source
ON target.time_sk = source.time_sk
WHEN NOT MATCHED THEN INSERT *;
