-- Databricks notebook source
-- DBTITLE 1,Create Bronze Heart Rates Table
CREATE OR REFRESH STREAMING LIVE TABLE heartrates
AS SELECT *
  FROM cloud_files(
    "/mnt/heartrates",
    "json",
    map("schema", "bpm INT, source STRING, timestamp STRING")
  );
