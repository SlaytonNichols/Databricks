-- Databricks notebook source
select *
from ouraring.heartrates_raw

-- COMMAND ----------

select *
from ouraring.heartrates_cleaned

-- COMMAND ----------

-- DBTITLE 1,Draft Gold Table Scratch Pad
CREATE TEMPORARY VIEW avg_heartrate_by_is_weekday
AS
SELECT avg(h.bpm) as avg_bpm, h.date, h.isWeekDay
  FROM ouraring.heartrates_curated h
GROUP BY h.date, h.isWeekDay

-- COMMAND ----------

CREATE TEMPORARY VIEW avg_heartrate_by_am_pm
AS
SELECT avg(h.bpm) as avg_bpm, h.date, h.AmPmString
  FROM ouraring.heartrates_curated h
GROUP BY h.date, h.AmPmString
