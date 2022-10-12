# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": "",
       "fs.azure.account.oauth2.client.secret": "",
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com//oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

dbutils.fs.mount(
source = "abfss://ouraring@snadls.dfs.core.windows.net",
mount_point = "/mnt",
extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls('/mnt/heartrates'))

# COMMAND ----------



# COMMAND ----------


