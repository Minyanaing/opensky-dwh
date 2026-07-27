-- {{CATALOG}} substituted per environment by env_setup.py --catalog. Master dimension tables for
-- the gold layer - populated by transformation/master/date.sql and time.sql.

CREATE TABLE IF NOT EXISTS {{CATALOG}}.master.date (
    `date_sk` INT,
    `date` DATE,
    `year` INT,
    `quarter` INT,
    `month` INT,
    `year_month` INT,
    `week_num` INT,
    `day` INT,
    `quarter_name` STRING,
    `month_name` STRING,
    `month_name_short` STRING,
    `days_of_week_num` INT,
    `days_of_weeks` STRING,
    `days_of_weeks_short` STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS {{CATALOG}}.master.time (
    `time_sk` INT,
    `time` STRING,
    `hour` INT,
    `hour_12` INT,
    `minute` INT,
    `am_pm` STRING,
    `hourly_range` STRING,
    `two_hour_range` STRING,
    `two_hour_range_idx` INT
) USING DELTA;
