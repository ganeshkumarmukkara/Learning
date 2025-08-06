# Databricks notebook source
data = [(1,[{"amount":45},{"amount":60}]),(2,[{"amount":55},{"amount":10}]),(3,[{"amount":35},{"amount":30}]),(4,[{"amount":45},{"amount":40}])]
schema = ["id","cash"]
df = spark.createDataFrame(data=data,schema=schema)
df.show(truncate=False)

# COMMAND ----------

df.createTempView('emp')
df2 = spark.sql('select * from emp where id >2')
df2.show()

# COMMAND ----------

display(df_sel)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, ArrayType, StructType, StructField, IntegerType


# Sample data
data = [
    (1, [{"amount": 45}, {"amount": 60}]),
    (2, [{"amount": 55}, {"amount": 10}]),
    (3, [{"amount": 35}, {"amount": 30}]),
    (4, [{"amount": 45}, {"amount": 40}])
]

# schema = StructType([
#     StructField("id", IntegerType(), True),
#     StructField("cash", ArrayType(MapType(StringType(), IntegerType())), True)
# ])
schema = ['id','cash']

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Define UDF to calculate the sum of amounts
@udf(IntegerType())
def sum_amounts(cash):
    total = 0
    for item in cash:
        total += item['amount']
    return total
        
    

# Add a new column with the sum of amounts
df_with_sum = df.withColumn("total_amount", sum_amounts(df["cash"]))

# Show the result
df_with_sum.show(truncate=False)
