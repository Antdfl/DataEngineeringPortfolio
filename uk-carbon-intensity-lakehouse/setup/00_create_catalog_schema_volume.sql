-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Setup: catalog, schema and landing volume
-- MAGIC
-- MAGIC Creates the Unity Catalog objects used by the project. Every statement is idempotent
-- MAGIC (`IF NOT EXISTS`), so the notebook can be run again safely.
-- MAGIC
-- MAGIC | Object | Name | Purpose |
-- MAGIC |---|---|---|
-- MAGIC | Catalog | `portfolio` | Container for the portfolio projects |
-- MAGIC | Schema | `portfolio.uk_carbon` | Tables of the UK Carbon Intensity lakehouse (bronze, silver, gold) |
-- MAGIC | Volume | `portfolio.uk_carbon.landing` | Raw JSON files written by the ingestion task, read by the pipeline |
-- MAGIC
-- MAGIC If `CREATE CATALOG` is not allowed in your workspace, use the default workspace catalog
-- MAGIC instead and keep the same schema and volume names.

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS portfolio
COMMENT 'Data engineering portfolio projects';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS portfolio.uk_carbon
COMMENT 'UK Carbon Intensity lakehouse: bronze, silver and gold tables';

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS portfolio.uk_carbon.landing
COMMENT 'Raw API responses (JSON), one file per call, written by the ingestion task';

-- COMMAND ----------

-- Check: the three objects exist
SHOW SCHEMAS IN portfolio;

-- COMMAND ----------

SHOW VOLUMES IN portfolio.uk_carbon;
