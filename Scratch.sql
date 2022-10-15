-- Databricks notebook source
select *
from ouraring.heartrates_raw

-- COMMAND ----------

select *
from ouraring.heartrates_cleaned

-- COMMAND ----------

select *
from ouraring.heartrates_curated

-- COMMAND ----------

SHOW TABLES FROM silver

-- COMMAND ----------

SELECT 
  h.bpm,
  h.source,  
  d.dateKey,  
  t.TimeKey
FROM ouraring.heartrates_cleaned h
JOIN silver.date d on to_date(h.timestamp) = d.date
JOIN bronze.time t on date_format(h.timestamp, "HH:mm:ss") = t.FullTime
