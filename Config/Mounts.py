# Databricks notebook source
client_secret = dbutils.secrets.get(scope = "sn-databricks", key = "client-secret")
configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": "500d0466-520d-4629-91c0-613c77e06a12",
       "fs.azure.account.oauth2.client.secret": client_secret,
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/0c772262-30c5-44a5-a3de-0f1a95207e30/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}
if not any(mount.mountPoint == '/mnt' for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
    source = "abfss://databricks@snadls.dfs.core.windows.net",
    mount_point = "/mnt",
    extra_configs = configs)

# COMMAND ----------


