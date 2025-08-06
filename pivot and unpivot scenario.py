# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC   create table emp_compensation (
# MAGIC     emp_id int,
# MAGIC     salary_type varchar(20),
# MAGIC     val int
# MAGIC   )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into emp_compensation
# MAGIC values (1,'salary',10000),(1,'bonus',5000),(1,'hike_percent',10)
# MAGIC , (2,'salary',15000),(2,'bonus',7000),(2,'hike_percent',8)
# MAGIC , (3,'salary',12000),(3,'bonus',6000),(3,'hike_percent',7);

# COMMAND ----------

df = spark.read.table('emp_compensation')
df.show()

# COMMAND ----------

df = spark.read.format('delta').load('dbfs:/user/hive/warehouse/emp_compensation')
display(df)

# COMMAND ----------

#pivot using pyspark

df2 = df.groupBy('emp_id').pivot('salary_type').sum('val').orderBy('emp_id')
display(df2)

# COMMAND ----------

#unpivot using pyspark

unpivot_df = df2.selectExpr("emp_id","stack(3,'bonus',bonus,'hike_percent',hike_percent,'salary',salary) as (salry_type, val)")
unpivot_df.show()


# COMMAND ----------

# Use spark.sql to run the SQL query and assign the result to a DataFrame
# pivot table and save
pivot_df = spark.sql("""
    SELECT
        emp_id,
        SUM(CASE WHEN salary_type = 'salary' THEN val END) AS salary,
        SUM(CASE WHEN salary_type = 'bonus' THEN val END) AS bonus,
        SUM(CASE WHEN salary_type = 'hike_percent' THEN val END) AS hike_percent
    FROM
        emp_compensation
    GROUP BY
        emp_id
""")

pivot_df.write.mode('overwrite').saveAsTable('emp_compensation_pivot')

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended emp_compensation_pivot  

# COMMAND ----------

# MAGIC %sql
# MAGIC -- unpivot in sql 
# MAGIC --select * from emp_compensation
# MAGIC
# MAGIC select emp_id, 'salary' as salary_type, salary as val from emp_compensation
# MAGIC union all
# MAGIC select emp_id, 'bonus' as salary_type, bonus as val from emp_compensation
# MAGIC union all
# MAGIC select emp_id, 'hike_percent' as salary_type, hike_percent as val from emp_compensation
# MAGIC