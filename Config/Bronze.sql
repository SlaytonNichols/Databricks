-- Databricks notebook source
-- DBTITLE 1,Create Bronze Schema
CREATE SCHEMA IF NOT EXISTS bronze
COMMENT "Bronze Schema"
LOCATION "/mnt/bronze"

-- COMMAND ----------

describe schema bronze

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS bronze.heartrates_raw USING DELTA LOCATION '/mnt/bronze/heartrates_raw'
