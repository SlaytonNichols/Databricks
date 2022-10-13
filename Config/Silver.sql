-- Databricks notebook source
-- DBTITLE 1,Create Silver Schema
CREATE SCHEMA IF NOT EXISTS silver
COMMENT "Silver Schema"
LOCATION "/mnt/silver"

-- COMMAND ----------

describe schema silver
