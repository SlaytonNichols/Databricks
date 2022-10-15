-- Databricks notebook source
-- DBTITLE 1,Create Bronze Heart Rates Table
CREATE OR REFRESH STREAMING LIVE TABLE heartrates_raw
LOCATION "/mnt/bronze/heartrates_raw"
AS SELECT *
  FROM cloud_files(
    "/mnt/landing/ouraring/heartrates",
    "json",
    map("schema", "bpm INT, source STRING, timestamp STRING")
  );

-- COMMAND ----------

-- DBTITLE 1,Create Silver Heart Rates Table
CREATE OR REFRESH LIVE TABLE heartrates_cleaned
LOCATION "/mnt/silver/heartrates_cleaned"
AS
SELECT 
  h.bpm,
  h.source,  
  to_date(h.timestamp) as heartrateDate,
  date_format(h.timestamp, "HH:mm:ss") as heartrateTime
FROM LIVE.heartrates_raw h
JOIN silver.date d on to_date(h.timestamp) = d.date
JOIN bronze.time t on date_format(h.timestamp, "HH:mm:ss") = t.FullTime

-- COMMAND ----------

-- DBTITLE 1,Create Gold Heart Rates Table
CREATE OR REFRESH LIVE TABLE heartrates_curated
LOCATION "/mnt/gold/heartrates_curated"
AS
SELECT *
  FROM LIVE.heartrates_cleaned h
JOIN silver.date d on h.heartrateDate = d.date
JOIN bronze.time t on h.heartrateTime = t.FullTime

-- COMMAND ----------

-- DBTITLE 1,Create Gold Heart Rates Table
CREATE OR REFRESH LIVE TABLE avg_heartrate_by_isweekday
LOCATION "/mnt/gold/avg_heartrate_by_isweekday"
AS
SELECT avg(h.bpm) as avg_bpm, h.date, h.isWeekDay
  FROM LIVE.heartrates_curated h
GROUP BY h.date, h.isWeekDay

-- COMMAND ----------


