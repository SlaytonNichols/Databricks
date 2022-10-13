-- Databricks notebook source
-- DBTITLE 1,Create Gold Schema
CREATE SCHEMA IF NOT EXISTS gold
COMMENT "Gold Schema"
LOCATION "/mnt/gold"

-- COMMAND ----------

describe schema gold
