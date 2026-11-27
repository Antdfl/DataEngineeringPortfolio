-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Setup: metric views on the gold layer
-- MAGIC
-- MAGIC Creates the metric views that hold the measures of the dashboard, Genie and the App, so that every tool
-- MAGIC uses the same formula. A metric view stores no data: it reads the gold materialized views at query time,
-- MAGIC so it is created once here and is not part of the pipeline. Every statement is idempotent
-- MAGIC (`CREATE OR REPLACE`), so the notebook can be run again safely.
-- MAGIC Run it after the pipeline `uk_carbon_pipeline` has created the gold layer.
-- MAGIC
-- MAGIC | Metric view | Source | Grain of the source |
-- MAGIC |---|---|---|
-- MAGIC | `portfolio.uk_carbon_gold.metrics_carbon_intensity` | `mv_intensity_half_hour` | Region and half hour |
-- MAGIC | `portfolio.uk_carbon_gold.metrics_generation_mix` | `mv_fuel_mix_half_hour` | Region, half hour and fuel |
-- MAGIC
-- MAGIC Averages are always a sum divided by a count, computed at query time: never an average of averages.
-- MAGIC Shares of a fuel group are the sum of the percentages divided by the number of region and half hour pairs, never an average of fuel percentages.
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE VIEW portfolio.uk_carbon_gold.metrics_carbon_intensity
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
source: portfolio.uk_carbon_gold.mv_intensity_half_hour
comment: Carbon intensity measures by region and London clock time, one formula for dashboard, Genie and App
dimensions:
  - name: region
    expr: region_name
    display_name: 'Region'
  - name: date_uk
    expr: date_uk
    display_name: 'Date (London)'
  - name: hour_uk
    expr: hour_uk
    display_name: 'Hour (London)'
  - name: half_hour_utc
    expr: period_start_utc
    comment: Start of the half hour in UTC, unique for each region. Use it to pick one half hour, because London clock time repeats one hour in October
    display_name: 'Half hour start (UTC)'
  - name: half_hour_uk
    expr: period_start_uk
    display_name: 'Half hour start (London)'
  - name: day_of_week
    expr: day_of_week_uk
    comment: ISO day of week, Monday 1 to Sunday 7, used to sort day_name
    display_name: 'Day of week number (ISO)'
  - name: day_name
    expr: day_name_uk
    display_name: 'Day of week'
  - name: intensity_index
    expr: intensity_index
    display_name: 'Intensity index'
measures:
  - name: half_hours
    expr: COUNT(1)
    display_name: 'Half hours'
  - name: total_intensity
    expr: SUM(forecast_gco2_kwh)
    display_name: 'Total intensity (gCO2/kWh)'
  - name: avg_intensity
    expr: MEASURE(total_intensity) / MEASURE(half_hours)
    comment: Weighted by half hours, never an average of averages
    display_name: 'Average intensity (gCO2/kWh)'
  - name: low_emission_half_hours
    expr: COUNT(1) FILTER (WHERE intensity_index IN ('very low', 'low'))
    display_name: 'Low emission half hours'
  - name: low_emission_share_pct
    expr: MEASURE(low_emission_half_hours) / MEASURE(half_hours) * 100
    display_name: 'Low emission share (%)'
$$;

-- COMMAND ----------

CREATE OR REPLACE VIEW portfolio.uk_carbon_gold.metrics_generation_mix
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
source: portfolio.uk_carbon_gold.mv_fuel_mix_half_hour
comment: Generation mix shares by region, London clock time and fuel group, one formula for dashboard, Genie and App
dimensions:
  - name: region
    expr: region_name
    display_name: 'Region'
  - name: date_uk
    expr: date_uk
    display_name: 'Date (London)'
  - name: hour_uk
    expr: hour_uk
    display_name: 'Hour (London)'
  - name: day_of_week
    expr: day_of_week_uk
    comment: ISO day of week, Monday 1 to Sunday 7, used to sort day_name
    display_name: 'Day of week number (ISO)'
  - name: day_name
    expr: day_name_uk
    display_name: 'Day of week'
  - name: fuel_group
    expr: fuel_group
    display_name: 'Fuel group'
  - name: fuel
    expr: fuel
    display_name: 'Fuel'
measures:
  - name: region_half_hours
    expr: COUNT(DISTINCT region_id, period_start_utc)
    comment: Number of region and half hour pairs, the denominator of every share
    display_name: 'Region half hours'
  - name: total_percentage
    expr: SUM(percentage)
    comment: Sum of the fuel percentages of the rows in scope
    display_name: 'Total percentage'
  - name: avg_share_pct
    expr: MEASURE(total_percentage) / MEASURE(region_half_hours)
    comment: Average share of the rows in scope per region half hour, never an average of fuel percentages
    display_name: 'Average share (%)'
$$;

-- COMMAND ----------

-- Check: one row for the whole gold; half_hours must equal the rows of mv_intensity_half_hour
SELECT MEASURE(half_hours)             AS half_hours,
       (SELECT COUNT(*) FROM portfolio.uk_carbon_gold.mv_intensity_half_hour) AS gold_rows,
       MEASURE(avg_intensity)          AS avg_intensity,
       MEASURE(low_emission_share_pct) AS low_emission_share_pct
FROM portfolio.uk_carbon_gold.metrics_carbon_intensity;

-- COMMAND ----------

-- Check: the region half hours must equal the rows of mv_intensity_half_hour, and the shares of all fuels add up to about 100
SELECT MEASURE(region_half_hours) AS region_half_hours,
       (SELECT COUNT(*) FROM portfolio.uk_carbon_gold.mv_intensity_half_hour) AS intensity_rows,
       MEASURE(avg_share_pct)     AS share_sum_pct
FROM portfolio.uk_carbon_gold.metrics_generation_mix;