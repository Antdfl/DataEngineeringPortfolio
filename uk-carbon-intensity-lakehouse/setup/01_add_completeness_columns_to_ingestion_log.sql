-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 01 Add completeness columns to the ingestion log
-- MAGIC
-- MAGIC **Only for ingestion logs created before 2026-11-11.** Newer logs already have these columns,
-- MAGIC because the ingestion notebook creates them; running this script on them fails.
-- MAGIC
-- MAGIC * `expected_half_hours`: half hours the call should return (minutes of the window / 30 + 1).
-- MAGIC * `is_complete`: true when the half hours received match the expected ones.

-- COMMAND ----------

ALTER TABLE portfolio.uk_carbon.ingestion_log ADD COLUMNS (
  expected_half_hours INT     COMMENT 'Half hours the call should return: minutes of the window / 30 + 1',
  is_complete         BOOLEAN COMMENT 'true when n_half_hours equals expected_half_hours'
);

-- COMMAND ----------

-- Expected half hours from the requested window: minutes / 30 + 1 (the API adds the half hour ending at the start)
UPDATE portfolio.uk_carbon.ingestion_log
SET expected_half_hours = CAST(
      (unix_timestamp(requested_to_utc,   "yyyy-MM-dd'T'HH:mmX")
     - unix_timestamp(requested_from_utc, "yyyy-MM-dd'T'HH:mmX")) / 1800 + 1 AS INT)
WHERE expected_half_hours IS NULL;

-- COMMAND ----------

-- Completeness: half hours received equal to the expected ones (failed calls, with no half hours, count as incomplete)
UPDATE portfolio.uk_carbon.ingestion_log
SET is_complete = CASE WHEN n_half_hours = expected_half_hours THEN true ELSE false END
WHERE is_complete IS NULL;

-- COMMAND ----------

-- Check: expected today 122 rows, 0 still null, 116 complete, 6 incomplete
SELECT COUNT(*)                              AS rows_total,
       COUNT_IF(expected_half_hours IS NULL) AS still_null,
       COUNT_IF(is_complete)                 AS complete,
       COUNT_IF(NOT is_complete)             AS incomplete
FROM portfolio.uk_carbon.ingestion_log;

-- COMMAND ----------

SELECT requested_from_utc, requested_to_utc, n_half_hours, expected_half_hours
FROM portfolio.uk_carbon.ingestion_log
WHERE NOT is_complete
ORDER BY requested_from_utc;
