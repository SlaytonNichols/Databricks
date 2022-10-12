-- Databricks notebook source
-- DBTITLE 1,Create Bronze Heart Rates Table
CREATE OR REFRESH STREAMING LIVE TABLE heartrates_raw
LOCATION "/mnt/bronze/tables/heartrates_raw"
AS SELECT *
  FROM cloud_files(
    "/mnt/landing/ouraring/heartrates",
    "json",
    map("schema", "bpm INT, source STRING, timestamp STRING")
  );

-- COMMAND ----------

-- DBTITLE 1,Create Silver Heart Rates Table
CREATE OR REFRESH LIVE TABLE heartrates_cleaned
LOCATION "/mnt/silver/tables/heartrates_cleaned"
AS
  SELECT 
    h.bpm,
    h.source,
    to_timestamp(h.timestamp) as timestamp
  FROM LIVE.heartrates_raw h

-- COMMAND ----------


