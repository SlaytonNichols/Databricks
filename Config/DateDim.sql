-- Databricks notebook source
-- MAGIC %python
-- MAGIC jsonDf = spark.read.option("multiline", "true").json("/mnt/config/dbo_DimDate.json")
-- MAGIC display(jsonDf)
-- MAGIC jsonDf.write.mode("overwrite").format("delta").saveAsTable("bronze.date")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select *
-- MAGIC from bronze.date
