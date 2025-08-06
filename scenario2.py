# Databricks notebook source

df = spark.read.format('csv').option('header','True').load('dbfs:/FileStore/bank_prospects.csv')
df.show()


# COMMAND ----------

df.write.format('csv').save('dbfs:/ganesh/bank')

# COMMAND ----------

from pyspark.sql.functions import *
df2 = spark.read.format('csv').option('header','True').load('dbfs:/FileStore/bank_prospects.csv')
df3 = df2.filter(col('Gender') == 'Male')
display(df3)

# COMMAND ----------

df3.write.format('csv').mode('overwrite').save('dbfs:/ganesh/bank')

# COMMAND ----------

df4 = spark.read.format('csv').option('header','True').load('dbfs:/ganesh/bank')
display(df4)

# COMMAND ----------

# MAGIC %fs ls /FileStore

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import Row
df_rdd = df.rdd.zipWithIndex()
# for i in df_rdd.collect():
#     print(i)
df1 = df_rdd.map(lambda x:tuple(x[0])+(x[1],)).toDF(df.columns+['index'])
# df1.show()
df2 = df1.withColumn('new_indes',monotonically_increasing_id())
df2.show()

# COMMAND ----------

# MAGIC %fs ls