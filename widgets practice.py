# Databricks notebook source
def parametrs():
    dbutils.widgets.text("input_widget", "default_value", "Enter your input")
    input = dbutils.widgets.get("input_widget")
    print(f'the value entered is:{input}')
    

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.help("cp")

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /

# COMMAND ----------

# when we create a workspace in databrick by default it will create a storage account for every workspace that is DBFS file system. Generally ?#we don't use this for storing data instead we use it to store logs. To access it below command

%fs ls dbfs:/


# COMMAND ----------

# Apart from dbfs fs we can access the driver machine file system with below command

%fs ls file:/

# COMMAND ----------

# To see all mounted storages in databricks use below
%fs mounts

# i have command to mount in my mobile or i can go and check that in the databricks documentation
# Generally mounting a storage account is not recommended because of two isses they are 
# 1.we are exposing all credentials of service principal in the npotebook that is not good for this we can use azure key vault and databricks secret scope
# 2. As mounting storage location will happened under the root directory of DBFS file system who ever has access to that workspace they can access the mounted storage location without restrictions. So it is allways recommended to use UNITY CATALOG to secure access and data governance.