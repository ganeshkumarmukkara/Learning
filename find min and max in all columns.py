# Databricks notebook source
from pyspark.sql.functions import *

# Sample DataFrame
data = [(1, 2, 3, 4),
        (5, 6, 7, 8),
        (9, 10, 11, 12),
        (13, 14, 15, 16),
        (17, 18, 19, 20)]

df = spark.createDataFrame(data, ["col1", "col2", "col3", "col4"])

# Calculate min and max for each column
final_df = df.select([min(c).alias(f"{c}_min") for c in df.columns] + [max(c).alias(f"{c}_max") for c in df.columns])
# Show the result
final_df.show()


# COMMAND ----------

display(df.describe())