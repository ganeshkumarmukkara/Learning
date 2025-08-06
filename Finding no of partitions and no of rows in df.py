# Databricks notebook source

"""
1. total number of partitions
2. count of rows in each partitions
"""

df=spark.range(1,1000000)
df.show()

# COMMAND ----------

from pyspark.sql.functions import col,spark_partition_id,count
df_final=df.withColumn("spark_partition_id",spark_partition_id())
df_final=df_final.groupBy(col("spark_partition_id")).agg(count("*").alias("total_rows"))
df_final.show()
