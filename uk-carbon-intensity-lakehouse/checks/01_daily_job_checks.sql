-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Daily checks after the refresh Job
-- MAGIC
-- MAGIC Runs after the `uk_carbon_refresh` Job and returns one row per check, `OK` or `KO`, plus a summary row (id 99) with the number of checks in `KO`.
-- MAGIC Every check is a relation that holds every day (for example "gold rows = silver rows"), not a number written by hand, so the notebook needs no edit when new data arrives.
-- MAGIC
-- MAGIC | Layer | Checks |
-- MAGIC |---|---|
-- MAGIC | Ingestion | 1 to 3 |
-- MAGIC | Bronze | 4 to 7 |
-- MAGIC | Silver | 8 to 16 |
-- MAGIC | Gold | 17 to 24 |
-- MAGIC | Metric views | 25 and 26 |
-- MAGIC | Summary | 99 |
-- MAGIC
-- MAGIC Run it after the Job has finished: check 3 fails if the last log window ended more than 26 hours ago.

-- COMMAND ----------

-- Daily checks after the uk_carbon_refresh Job: one row per check, OK or KO.
-- Every check is a relation that holds every day, not a number written by hand.
WITH last_log AS (
  SELECT http_status,
         is_complete,
         requested_to_utc,
         to_timestamp(requested_to_utc, "yyyy-MM-dd'T'HH:mmX") AS window_end
  FROM portfolio.uk_carbon.ingestion_log
  ORDER BY requested_to_utc DESC, file_name DESC NULLS LAST
  LIMIT 1
),
bronze AS (
  SELECT (SELECT COUNT(*) FROM portfolio.uk_carbon_bronze.regional_raw)                                 AS bronze_rows,
         (SELECT COUNT(*) FROM portfolio.uk_carbon.ingestion_log WHERE file_name IS NOT NULL)           AS log_files,
         (SELECT COUNT(*) FROM portfolio.uk_carbon_bronze.regional_raw WHERE _rescued_data IS NOT NULL) AS rescued_rows
),
bronze_vs_log AS (
  SELECT COUNT(*)                                                                    AS files_compared,
         SUM(CASE WHEN json_array_length(b.data) = l.n_half_hours THEN 1 ELSE 0 END) AS files_matching,
         SUM(json_array_length(b.data))                                              AS bronze_half_hours,
         SUM(l.n_half_hours)                                                         AS log_half_hours
  FROM portfolio.uk_carbon_bronze.regional_raw b
  JOIN portfolio.uk_carbon.ingestion_log l
    ON element_at(split(b.source_file, '/'), -1) = l.file_name
),
intensity AS (
  SELECT COUNT(*)                                AS rows_total,
         COUNT(DISTINCT region_id, period_start) AS distinct_keys,
         COUNT(DISTINCT region_id)               AS regions,
         COUNT_IF(forecast_gco2_kwh IS NULL)     AS null_forecast,
         MAX(period_start)                       AS last_half_hour
  FROM portfolio.uk_carbon_silver.intensity
),
mix AS (
  SELECT COUNT(*)                                      AS rows_total,
         COUNT(DISTINCT region_id, period_start, fuel) AS distinct_keys,
         COUNT(DISTINCT region_id, period_start)       AS region_half_hours,
         COUNT(DISTINCT fuel)                          AS fuels,
         COUNT_IF(percentage IS NULL)                  AS null_percentage
  FROM portfolio.uk_carbon_silver.generation_mix
),
regions AS (
  SELECT COUNT(*)                    AS region_rows,
         COUNT(DISTINCT source_file) AS distinct_files
  FROM portfolio.uk_carbon_silver.regions
),
gold_intensity AS (
  SELECT COUNT(*)                                    AS rows_total,
         COUNT(DISTINCT region_id, period_start_utc) AS distinct_keys,
         COUNT_IF(region_name IS NULL)               AS null_names,
         typeof(MAX(period_start_uk))                AS uk_type,
         MAX(period_start_utc)                       AS last_half_hour
  FROM portfolio.uk_carbon_gold.mv_intensity_half_hour
),
gold_mix AS (
  SELECT COUNT(*)                                          AS rows_total,
         COUNT(DISTINCT region_id, period_start_utc, fuel) AS distinct_keys,
         COUNT(DISTINCT region_id, period_start_utc)       AS region_half_hours,
         COUNT_IF(region_name IS NULL)                     AS null_names,
         COUNT_IF(fuel_group IS NULL)                      AS null_groups,
         COUNT_IF(fuel_group = 'Renewable')                AS renewable_rows,
         COUNT_IF(fuel_group = 'Fossil')                   AS fossil_rows,
         COUNT_IF(fuel_group = 'Nuclear and other')        AS nuclear_other_rows
  FROM portfolio.uk_carbon_gold.mv_fuel_mix_half_hour
),
gold_mix_totals AS (
  SELECT COUNT_IF(abs(total_pct - 100) > 0.5) AS outside_tolerance
  FROM (
    SELECT region_id, period_start_utc, SUM(percentage) AS total_pct
    FROM portfolio.uk_carbon_gold.mv_fuel_mix_half_hour
    GROUP BY ALL
  )
),
metric_view AS (
  SELECT MEASURE(half_hours) AS half_hours
  FROM portfolio.uk_carbon_gold.metrics_carbon_intensity
),
metric_view_mix AS (
  SELECT MEASURE(region_half_hours) AS region_half_hours
  FROM portfolio.uk_carbon_gold.metrics_generation_mix
),
checks AS (
  SELECT 1 AS id, 'Ingestion' AS layer, 'Last call: HTTP 200' AS check_name,
         CAST(http_status AS STRING) AS actual, '200' AS expected, http_status = 200 AS passed
  FROM last_log
  UNION ALL
  SELECT 2, 'Ingestion', 'Last call: complete',
         CAST(is_complete AS STRING), 'true', is_complete
  FROM last_log
  UNION ALL
  SELECT 3, 'Ingestion', 'Last call: window ends less than 26 hours ago',
         requested_to_utc, 'after ' || CAST(current_timestamp() - INTERVAL 26 HOURS AS STRING),
         window_end > current_timestamp() - INTERVAL 26 HOURS
  FROM last_log
  UNION ALL
  SELECT 4, 'Bronze', 'Bronze rows = files in the log',
         CAST(bronze_rows AS STRING), CAST(log_files AS STRING), bronze_rows = log_files
  FROM bronze
  UNION ALL
  SELECT 5, 'Bronze', 'No rescued data',
         CAST(rescued_rows AS STRING), '0', rescued_rows = 0
  FROM bronze
  UNION ALL
  SELECT 6, 'Bronze', 'Every file has the half hours of the log',
         CAST(files_matching AS STRING), CAST(files_compared AS STRING), files_matching = files_compared
  FROM bronze_vs_log
  UNION ALL
  SELECT 7, 'Bronze', 'Files compared = bronze rows',
         CAST(v.files_compared AS STRING), CAST(b.bronze_rows AS STRING), v.files_compared = b.bronze_rows
  FROM bronze_vs_log v CROSS JOIN bronze b
  UNION ALL
  SELECT 8, 'Silver', 'intensity: one row per key',
         CAST(rows_total AS STRING), CAST(distinct_keys AS STRING), rows_total = distinct_keys
  FROM intensity
  UNION ALL
  SELECT 9, 'Silver', 'intensity: 14 regions',
         CAST(regions AS STRING), '14', regions = 14
  FROM intensity
  UNION ALL
  SELECT 10, 'Silver', 'intensity: no null forecast',
         CAST(null_forecast AS STRING), '0', null_forecast = 0
  FROM intensity
  UNION ALL
  SELECT 11, 'Silver', 'intensity: last half hour = end of the log window - 30 min',
         CAST(i.last_half_hour AS STRING), CAST(l.window_end - INTERVAL 30 MINUTES AS STRING),
         i.last_half_hour = l.window_end - INTERVAL 30 MINUTES
  FROM intensity i CROSS JOIN last_log l
  UNION ALL
  SELECT 12, 'Silver', 'generation_mix: one row per key',
         CAST(rows_total AS STRING), CAST(distinct_keys AS STRING), rows_total = distinct_keys
  FROM mix
  UNION ALL
  SELECT 13, 'Silver', 'generation_mix: 9 fuels',
         CAST(fuels AS STRING), '9', fuels = 9
  FROM mix
  UNION ALL
  SELECT 14, 'Silver', 'generation_mix: rows = 9 x intensity rows',
         CAST(m.rows_total AS STRING), CAST(9 * i.rows_total AS STRING),
         m.rows_total = 9 * i.rows_total AND m.region_half_hours = i.rows_total
  FROM mix m CROSS JOIN intensity i
  UNION ALL
  SELECT 15, 'Silver', 'generation_mix: no null percentage',
         CAST(null_percentage AS STRING), '0', null_percentage = 0
  FROM mix
  UNION ALL
  SELECT 16, 'Silver', 'regions: 14 rows from one file',
         CAST(region_rows AS STRING) || ' rows, ' || CAST(distinct_files AS STRING) || ' file',
         '14 rows, 1 file', region_rows = 14 AND distinct_files = 1
  FROM regions
  UNION ALL
  SELECT 17, 'Gold', 'mv_intensity_half_hour: rows = silver rows, one per key',
         CAST(g.rows_total AS STRING), CAST(i.rows_total AS STRING),
         g.rows_total = i.rows_total AND g.distinct_keys = g.rows_total
  FROM gold_intensity g CROSS JOIN intensity i
  UNION ALL
  SELECT 18, 'Gold', 'mv_intensity_half_hour: no null region names',
         CAST(null_names AS STRING), '0', null_names = 0
  FROM gold_intensity
  UNION ALL
  SELECT 19, 'Gold', 'mv_intensity_half_hour: London time is timestamp_ntz',
         uk_type, 'timestamp_ntz', uk_type = 'timestamp_ntz'
  FROM gold_intensity
  UNION ALL
  SELECT 20, 'Gold', 'mv_intensity_half_hour: same last half hour as silver',
         CAST(g.last_half_hour AS STRING), CAST(i.last_half_hour AS STRING),
         g.last_half_hour = i.last_half_hour
  FROM gold_intensity g CROSS JOIN intensity i
  UNION ALL
  SELECT 21, 'Gold', 'mv_fuel_mix_half_hour: rows = silver rows, one per key',
         CAST(g.rows_total AS STRING), CAST(m.rows_total AS STRING),
         g.rows_total = m.rows_total AND g.distinct_keys = g.rows_total
  FROM gold_mix g CROSS JOIN mix m
  UNION ALL
  SELECT 22, 'Gold', 'mv_fuel_mix_half_hour: no null names or groups',
         CAST(null_names AS STRING) || ' names, ' || CAST(null_groups AS STRING) || ' groups',
         '0 names, 0 groups', null_names = 0 AND null_groups = 0
  FROM gold_mix
  UNION ALL
  SELECT 23, 'Gold', 'mv_fuel_mix_half_hour: group rows = region half hours x 4 / 2 / 3',
         CAST(renewable_rows AS STRING) || ' / ' || CAST(fossil_rows AS STRING) || ' / ' || CAST(nuclear_other_rows AS STRING),
         CAST(4 * region_half_hours AS STRING) || ' / ' || CAST(2 * region_half_hours AS STRING) || ' / ' || CAST(3 * region_half_hours AS STRING),
         renewable_rows = 4 * region_half_hours AND fossil_rows = 2 * region_half_hours
           AND nuclear_other_rows = 3 * region_half_hours
  FROM gold_mix
  UNION ALL
  SELECT 24, 'Gold', 'mv_fuel_mix_half_hour: fuels add up to 100 +/- 0.5',
         CAST(outside_tolerance AS STRING), '0', outside_tolerance = 0
  FROM gold_mix_totals
  UNION ALL
  SELECT 25, 'Metric view', 'metrics_carbon_intensity: half_hours = gold rows',
         CAST(v.half_hours AS STRING), CAST(g.rows_total AS STRING), v.half_hours = g.rows_total
  FROM metric_view v CROSS JOIN gold_intensity g
  UNION ALL
  SELECT 26, 'Metric view', 'metrics_generation_mix: region_half_hours = gold intensity rows',
         CAST(v.region_half_hours AS STRING), CAST(g.rows_total AS STRING), v.region_half_hours = g.rows_total
  FROM metric_view_mix v CROSS JOIN gold_intensity g
)
SELECT id, layer, check_name, actual, expected,
       CASE WHEN passed THEN 'OK' ELSE 'KO' END AS status
FROM checks
UNION ALL
SELECT 99, 'Summary', 'Checks KO',
       CAST(COUNT_IF(passed IS NOT TRUE) AS STRING), '0',
       CASE WHEN COUNT_IF(passed IS NOT TRUE) = 0 THEN 'OK' ELSE 'KO' END
FROM checks
ORDER BY id;