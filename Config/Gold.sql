-- Databricks notebook source
-- DBTITLE 1,Create Gold Schema
CREATE SCHEMA IF NOT EXISTS gold
COMMENT "Gold Schema"
LOCATION "/mnt/gold"

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS gold.heartrates_curated USING DELTA LOCATION '/mnt/bronze/heartrates_curated'

-- COMMAND ----------


