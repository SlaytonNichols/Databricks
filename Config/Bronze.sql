-- Databricks notebook source
-- DBTITLE 1,Create Bronze Schema
CREATE SCHEMA IF NOT EXISTS bronze
COMMENT "Bronze Schema"
LOCATION "/mnt/bronze"

-- COMMAND ----------

-- DBTITLE 1,Create Bronze HeartRatesRaw Table
CREATE TABLE IF NOT EXISTS bronze.heartrates_raw USING DELTA LOCATION '/mnt/bronze/heartrates_raw'

-- COMMAND ----------

-- DBTITLE 1,Create Bronze Date
-- MAGIC %python
-- MAGIC if not spark._jsparkSession.catalog().tableExists("bronze.date"):
-- MAGIC     jsonDf = spark.read.option("multiline", "true").json("/mnt/config/dbo_DimDate.json")
-- MAGIC     jsonDf.write.mode("overwrite").format("delta").saveAsTable("bronze.date")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC if not spark._jsparkSession.catalog().tableExists("bronze.time"):
-- MAGIC     jsonDf = spark.read.option("multiline", "true").json("/mnt/config/blog_dbo_DimTime.json")
-- MAGIC     jsonDf.write.mode("overwrite").format("delta").saveAsTable("bronze.time")

-- COMMAND ----------

select *
from bronze.time

-- COMMAND ----------


