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
  d.dateKey,  
  t.TimeKey as timeKey
FROM LIVE.heartrates_raw h
JOIN silver.date d on to_date(h.timestamp) = d.date
JOIN bronze.time t on date_format(h.timestamp, "HH:mm:ss") = t.FullTime

-- COMMAND ----------

-- DBTITLE 1,Create Gold Heart Rates Table
-- CREATE OR REFRESH LIVE TABLE heartrates_curated
-- LOCATION "/mnt/gold/heartrates_curated"
-- AS
--   SELECT 
--     h.bpm,
--     h.source,
--     h.dateKey,
--     h.timeKey
--   FROM LIVE.heartrates_cleaned h
