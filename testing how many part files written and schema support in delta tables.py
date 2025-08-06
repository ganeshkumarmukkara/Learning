# Databricks notebook source
data = [(1,'ganesh',27,70000),(2,'sai',20,60000),(3,'yallu',30,20000),(4,'chinna',52,80000)]

schema = "id int,name string,age int,salary int"

df = spark.createDataFrame(data,schema)
display(df)

# COMMAND ----------

df.write.format('delta').mode('append').saveAsTable('frd_data')


# COMMAND ----------

data = [(5,'ganesh','mukkara',27,70000,'bangalore'),(6,'sai','pujari',20,60000,'hyderabad'),(7,'yallu',' ',30,20000,'delhi'),(8,'chinna','mukkara',52,80000,' ')]

# schema = ['id','name','surname','age','salary','city']

schema = "id int,name string, surname string,age int,salary int, city string"
 
df1 = spark.createDataFrame(data,schema)
display(df1)

# COMMAND ----------

# Append df1 with schema evolution enabled
df1.write.format('delta') \
    .mode('append') \
    .option('mergeSchema', 'true') \
    .saveAsTable('frd_data')


# COMMAND ----------

df3 = spark.read.table('frd_data')
display(df3)

# COMMAND ----------

df.printSchema()


# COMMAND ----------

df1.printSchema()