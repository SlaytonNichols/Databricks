-- Databricks notebook source
-- DBTITLE 1,Create Silver Schema
CREATE SCHEMA IF NOT EXISTS silver
COMMENT "Silver Schema"
LOCATION "/mnt/silver"

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS silver.heartrates_cleaned USING DELTA LOCATION '/mnt/bronze/heartrates_cleaned'

-- COMMAND ----------


