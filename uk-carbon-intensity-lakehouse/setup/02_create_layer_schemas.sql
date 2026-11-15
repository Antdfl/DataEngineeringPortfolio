-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Setup: one schema per pipeline layer
-- MAGIC
-- MAGIC Creates the schemas where the pipeline `uk_carbon_pipeline` publishes its tables, one per
-- MAGIC medallion layer, and updates the comment of `portfolio.uk_carbon`, which keeps only the
-- MAGIC ingestion objects. Every statement is idempotent, so the notebook can be run again safely.
-- MAGIC Run `00_create_catalog_schema_volume` first: it creates the `portfolio` catalog.
-- MAGIC
-- MAGIC | Schema | Layer | Content |
-- MAGIC |---|---|---|
-- MAGIC | `portfolio.uk_carbon` | Ingestion | Landing volume and ingestion log (created by `00`) |
-- MAGIC | `portfolio.uk_carbon_bronze` | Bronze | Raw API responses read by Auto Loader, all fields as strings |
-- MAGIC | `portfolio.uk_carbon_silver` | Silver | Typed, filtered and deduplicated tables |
-- MAGIC | `portfolio.uk_carbon_gold` | Gold | Materialized views and metric view for the dashboard |

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS portfolio.uk_carbon_bronze
COMMENT 'UK Carbon Intensity bronze layer: raw API responses read by Auto Loader';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS portfolio.uk_carbon_silver
COMMENT 'UK Carbon Intensity silver layer: typed, filtered and deduplicated tables';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS portfolio.uk_carbon_gold
COMMENT 'UK Carbon Intensity gold layer: materialized views and metric view for the dashboard';

-- COMMAND ----------

-- The schema created by 00 keeps only the ingestion objects: update its comment
COMMENT ON SCHEMA portfolio.uk_carbon IS 'UK Carbon Intensity: ingestion area (landing volume and ingestion log)';

-- COMMAND ----------

-- Check: the four uk_carbon schemas exist
SHOW SCHEMAS IN portfolio LIKE 'uk_carbon*';

-- COMMAND ----------

-- Check: the comment of the ingestion schema
DESCRIBE SCHEMA portfolio.uk_carbon;