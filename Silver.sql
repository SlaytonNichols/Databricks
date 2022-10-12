-- Databricks notebook source
CREATE OR REFRESH LIVE TABLE heartrates
AS
  SELECT 
    h.bpm,
    h.source,
    to_timestamp(h.timestamp) as timestamp
  FROM bronze.heartrates h

-- COMMAND ----------


