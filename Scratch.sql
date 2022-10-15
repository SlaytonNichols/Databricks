-- Databricks notebook source
select *
from ouraring.heartrates_raw

-- COMMAND ----------

select *
from ouraring.heartrates_cleaned

-- COMMAND ----------

-- DBTITLE 1,Draft Gold Table Scratch Pad
select avg(h.bpm), h.date, h.isWeekDay
from ouraring.heartrates_curated h
group by h.date, h.isWeekDay

-- COMMAND ----------


