# Databricks notebook source
==> generally when we create databricks workspace in azure environment , DB will create many resources in azure clould in the backend which include virtual machines,storage accounts,networks etc.. 
==> we have two types of storages in the beginning they are DBFS storage , another is driver node storage we can access these storages using magic commands %fs 
==> 

# COMMAND ----------

# DBFS storage
%fs ls DBFS:/

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/COVID

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/COVID/USAFacts

# COMMAND ----------

# gives all the directories mounted on top of root directory
%fs mounts

# COMMAND ----------

# driver node file system
%fs ls file:/