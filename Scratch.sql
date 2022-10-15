-- Databricks notebook source
select *
from ouraring.heartrates_raw

-- COMMAND ----------

select *
from ouraring.heartrates_cleaned

-- COMMAND ----------

-- DBTITLE 1,Draft Gold Table Scratch Pad
select *
from ouraring.heartrates_cleaned h
JOIN silver.date d on d.dateKey = h.dateKey
JOIN bronze.time t on t.TimeKey = h.timeKey

-- COMMAND ----------


