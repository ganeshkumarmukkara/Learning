# Databricks notebook source
# MAGIC %md
# MAGIC scenarios
# MAGIC 1. read excel data in different sheets
# MAGIC 2.read a csv file in which data is seperated with different delimeters
# MAGIC 3.how to make 4th row as header from csv file
# MAGIC 4. pivot and unpivot in both sql and pyspark
# MAGIC 5. window functions in both sql and pyspark
# MAGIC 6. how to read differnt file formats data
# MAGIC

# COMMAND ----------

data = [(1,'ganesh','mobile'),(1,'ganesh','cycle'),(2,'sai','glass'),(2,'sai','hero')]
schema = ['id','name','product']

df = spark.createDataFrame(data,schema)
df.show(truncate=True)

# COMMAND ----------

from pyspark.sql.functions import *
df2 = df.groupby('id','name').agg(collect_set('product'))
df2.show(truncate=True)
df2.rdd.glom().map(len).collect()


# COMMAND ----------

print(df2.rdd.getNumPartitions())
df3 = df2.repartition(2)
print(df3.rdd.getNumPartitions())
df3.rdd.glom().map(len).collect()

# COMMAND ----------

def partsize(rddd):
    return [sys.getsizeof(partition) for partition in rddd]

partsize = df3.rdd.mapPartitions(partsize).collect()
print(partsize) #it is in bytes for each partition 

# COMMAND ----------

l = [1,2,3,4,5]
rdd = spark.sparkContext.parallelize(l)
rdd.count()

# COMMAND ----------

l1 = ['a','b,','c']
l2 = [1,2,3]
rdd = spark.sparkContext.parallelize(list(zip(l1,l2)))
rdd.collect()