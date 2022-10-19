-- Databricks notebook source
-- DBTITLE 1,Create Gold Schema
CREATE SCHEMA IF NOT EXISTS gold
COMMENT "Gold Schema"
LOCATION "/mnt/gold"

-- COMMAND ----------

USE gold;

-- COMMAND ----------

-- DBTITLE 1,Create Gold HeartRatesCurated Table
CREATE TABLE IF NOT EXISTS gold.heartrates_curated USING DELTA LOCATION '/mnt/gold/heartrates_curated'

-- COMMAND ----------

CREATE VIEW IF NOT EXISTS avg_heartrate_by_am_pm
AS
SELECT avg(h.bpm) as avg_bpm, h.date, h.AmPmString
  FROM gold.heartrates_curated h
GROUP BY h.date, h.AmPmString
ORDER BY h.date DESC

-- COMMAND ----------

CREATE VIEW IF NOT EXISTS avg_heartrate_by_is_weekday
AS
SELECT avg(h.bpm) as avg_bpm, h.date, h.isWeekDay
  FROM gold.heartrates_curated h
GROUP BY h.date, h.isWeekDay
ORDER BY h.date DESC

-- COMMAND ----------


