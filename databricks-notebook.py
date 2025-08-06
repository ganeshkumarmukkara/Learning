# Databricks notebook source
# MAGIC %md
# MAGIC Load the Data from CSV file into the Data Frame

# COMMAND ----------


df = spark.read.csv("/FileStore/tables/Order.csv", header=True, inferSchema=True, sep =',')
df.printSchema()
df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Display the Data using the Databricks Specific Function

# COMMAND ----------

display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC #Data Type in the pySpark

# COMMAND ----------

#Data type                             Value assigned in Python                   API to instantiate
#ByteType                                 	int                                     DataTypes.ByteType
#ShortType                            		int                                     DataTypes.ShortType
#IntegerType                         		int                   					DataTypes.IntegerType
#LongType                           		int                 					DataTypes.LongType
#FloatType                           		float              						DataTypes.FloatType
#DoubleType                          		float                  					DataTypes.DoubleType
#StringType                          		str             						DataTypes.StringType
#BooleanType                          		bool                					DataTypes.BooleanType
#DecimalType                          		decimal.Decimal         				DecimalType
#BinaryType 								bytearray 								BinaryType()
#TimestampType 								datetime.datetime 						TimestampType()
#DateType 									datetime.date 							DateType()
#ArrayType 									List, tuple, or array 					ArrayType(dataType, [nullable])
#MapType 									dict 									MapType(keyType, valueType, [nullable])
#StructType 								List or tuple 							StructType([fields])
#StructField 								 										StructField(name, dataType, [nullable])


# COMMAND ----------

# MAGIC %md
# MAGIC #Define schema progarmatically

# COMMAND ----------

#Define schema progarmatically
from pyspark.sql.types import *
orderSchema = StructType([StructField("Region", StringType() ,True)
,StructField("Country", StringType() ,True)
,StructField("ItemType", StringType() ,True)
,StructField("SalesChannel", StringType() ,True)
,StructField("OrderPriority", StringType() ,True)
,StructField("OrderID", IntegerType() ,True)
,StructField("UnitsSold", IntegerType() ,True)
,StructField("UnitPrice", DoubleType() ,True)
,StructField("UnitCost", DoubleType() ,True)
,StructField("TotalRevenue", DoubleType() ,True)
,StructField("TotalCost", DoubleType() ,True)
,StructField("TotalProfit", DoubleType() ,True)
])


df = spark.read.load("/FileStore/tables/Order.csv",format="csv", header=True, schema=orderSchema)
df.printSchema()
df.schema

# COMMAND ----------

# MAGIC %md
# MAGIC #Define schema Declaratively

# COMMAND ----------


orderSchema = 'Region String ,Country String ,ItemType String ,SalesChannel String ,OrderPriority String ,OrderID Integer ,UnitsSold Integer ,UnitPrice Double ,UnitCost Double ,TotalRevenue Double ,TotalCost Double ,TotalProfit Double'
                          
df = spark.read.load("/FileStore/tables/Order.csv",format="csv", header=True, schema=orderSchema)
df.printSchema()
#df.schema
#display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC Row object

# COMMAND ----------

df.collect()

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Columns in Spark Dataframe 
# MAGIC  in Scala
# MAGIC $"myColumn"
# MAGIC 'myColumn
# MAGIC
# MAGIC Columns are just expressions.<br>
# MAGIC Columns and transformations of those columns compile to the same logical plan as
# MAGIC parsed expressions.

# COMMAND ----------

from pyspark.sql.functions import col, column
col("someColumnName")
column("someColumnName")


# COMMAND ----------

# To get list of columns

df.columns

# COMMAND ----------

#To get the first row
df.first()

# COMMAND ----------

# MAGIC %md
# MAGIC #Create Dataframe and Rows Manually

# COMMAND ----------

from pyspark.sql import Row
row1 = Row("Ram", None, 1, True)

# To access the specific attribute within the row use following:
#parallelizedRows = spark.sparkContext.parallelize(newRows)
row1[0]

# Create a dataframe
row1 = Row("Ram", None, 1, True)
myManualSchema = 'Name string, address string not null, id integer, exists string'
manDf = spark.createDataFrame([row1], myManualSchema)
manDf.show()


# COMMAND ----------

# MAGIC %md
# MAGIC # Select and SelectExpr (Select Expressions)

# COMMAND ----------

from pyspark.sql.functions import expr, col, column
df.select("Region", "Country").show(2)

#select using variety of ways
df.select(
expr("Country"),
col("Region"),
column("ItemType"),
df.OrderID)\
.show(2)

# Select using the alias
df.select(
expr("Country as NewCountry"),
col("Region").alias('New Region'),
column("ItemType"),
df.OrderID)\
.show(2)

#Use selectexpr

df.selectExpr("Country as NewCountry", "Region").show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC Converting to Spark Types (Literals)

# COMMAND ----------

# in Python
from pyspark.sql.functions import lit
df.select(expr("*"), lit(1).alias("NumberOne")).show(2)
df.select(expr("*"), lit(True).alias("MyResult")).show(2)
df.select(expr("*"), lit('Constant').alias("AddedColumn")).show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC #Add column to dataframe

# COMMAND ----------

from pyspark.sql.functions import lit
newdf = df.withColumn("NewColumn", lit(1)).show(2)
newdf = df.withColumn("withinCountry", expr("Country == 'India'"))\

newdf.columns

# COMMAND ----------

# MAGIC %md
# MAGIC #Renaming Columns

# COMMAND ----------

newdf = df.withColumnRenamed("Country", "NewCountry")
newdf.columns

# COMMAND ----------

# Use escape character when column name contains spaces (`)
newdf =df.withColumnRenamed("Country","New Column Name")
newdf.select("`New Column Name`").show()

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC  #Removing Columns
# MAGIC  
# MAGIC  Use comma to remove multiple columns

# COMMAND ----------

df.drop("Country").columns
df.drop("Country", "Region").columns

# COMMAND ----------

# MAGIC %md
# MAGIC Change Column Type

# COMMAND ----------

df.withColumn("UnitsSoldNew", col("UnitsSold").cast("double")).show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC #Filter Rows
# MAGIC To filter rows, we create an expression that evaluates to true or false. You then filter out the rows
# MAGIC with an expression that is equal to false.

# COMMAND ----------

df.filter(col("UnitsSold") < 1000).show(2)
df.where("UnitsSold < 1000").show(2)
df.filter(df.UnitsSold < 1000).show(2)


# COMMAND ----------

# MAGIC %md
# MAGIC # Unique Rows
# MAGIC extract the unique or distinct values in a DataFrame

# COMMAND ----------

df.select("Country", "Region").distinct().count()
df.select("Country").distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC #Union
# MAGIC DataFramess. To union two DataFrames, you must be sure that they have the same schema and
# MAGIC number of columns; otherwise, the union will fail.

# COMMAND ----------

df1 = df.filter("Country = 'Libya'")
df2 = df.filter("Country = 'Canada'")
df1.union(df2).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #Sorting
# MAGIC There are two equivalent operations to do this sort
# MAGIC and orderBy that work the exact same way. They accept both column expressions and strings as
# MAGIC well as multiple columns. The default is to sort in ascending order: <br>
# MAGIC You need to use the asc and desc functions if operating
# MAGIC on a column. These allow you to specify the order in which a given column should be sorted: <br>
# MAGIC
# MAGIC use asc_nulls_first, desc_nulls_first, asc_nulls_last, or
# MAGIC desc_nulls_last to specify where you would like your null values to appear in an ordered
# MAGIC DataFrame.

# COMMAND ----------

df.sort("Country").show(5)
df.orderBy("Country", "UnitsSold").show(5)
df.orderBy(col("ItemType"), col("UnitPrice")).show(5)

#asc and desc function
from pyspark.sql.functions import desc, asc
df.orderBy(expr("Country desc")).show(2)
df.orderBy(col("Country").desc(), col("UnitPrice").asc()).show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC #Limit
# MAGIC you
# MAGIC might want just the top ten of some DataFrame

# COMMAND ----------

df.limit(5).show()
df.orderBy(expr("Country desc")).limit(6).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #Repartition and Coalesce
# MAGIC
# MAGIC Repartition will incur a full shuffle of the data, regardless of whether one is necessary. This
# MAGIC means that you should typically only repartition when the future number of partitions is greater
# MAGIC than your current number of partitions or when you are looking to partition by a set of columns: <br> <br>
# MAGIC Coalesce, on the other hand, will not incur a full shuffle and will try to combine partitions. This
# MAGIC operation will shuffle your data into five partitions based on the destination country name, and
# MAGIC then coalesce them (without a full shuffle)

# COMMAND ----------

df.rdd.getNumPartitions()
df.repartition(5)

#If you know that you’re going to be filtering by a certain column often, it can be worth repartitioning based on that column:
df.repartition(5, col("Country"))


df.repartition(5, col("Country")).coalesce(2)


# COMMAND ----------

# MAGIC %md
# MAGIC Use collect carefully

# COMMAND ----------

collectDF = df.limit(10)
collectDF.take(5) # take works with an Integer count
collectDF.show() # this prints it out nicely
collectDF.show(5, False)
collectDF.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC Describe to get statistics

# COMMAND ----------

display(df.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC #String functions

# COMMAND ----------

from pyspark.sql.functions import initcap
from pyspark.sql.functions import lower, upper

#Translate the first letter of each word to upper case in the sentence.
df.select(initcap(col("Country"))).show()
df.select(lower(col("Country"))).show()
df.select(upper(col("Country"))).show()

#To concat 
display(df.select(concat(col("Region"), col("Country"))))

#To concat with separater
display(df.select(concat_ws('|',col("Region"), col("Country"))))


#instr(str, substr) - Returns the (1-based) index of the first occurrence of substr in str
display(df.select(instr(col("Region"),"Mi")))

#length(expr) - Returns the character length of string data or number of bytes of binary data
display(df.select(length(col("Region"))))

from pyspark.sql.functions import lit, ltrim, rtrim, rpad, lpad, trim
df.select(
ltrim(lit(" HELLO ")).alias("ltrim"),
rtrim(lit(" HELLO ")).alias("rtrim"),
trim(lit(" HELLO ")).alias("trim"),
lpad(lit("HELLO"), 3, " ").alias("lp"),
rpad(lit("HELLO"), 10, " ").alias("rp")).show(2)

# Regular Expression
#from pyspark.sql.functions import regexp_replace
#regex_string = "Hello|WHITE|RED|GREEN|BLUE"
#df.select(regexp_replace(col("Country"), regex_string, "COLOR").alias("color_clean"),col("Description")).show(2)


# COMMAND ----------

# MAGIC %md
# MAGIC #Date Handling in Spark
# MAGIC
# MAGIC Spark will not throw an error if it cannot parse the date; rather, it will just return null.

# COMMAND ----------

from pyspark.sql.functions import current_date, current_timestamp
dateDF = spark.range(10)\
.withColumn("today", current_date())\
.withColumn("now", current_timestamp())

display(dateDF)

# Add Subtract dates
from pyspark.sql.functions import date_add, date_sub
dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)

#Days and Month difference between dates
from pyspark.sql.functions import datediff, months_between, to_date
dateDF.withColumn("week_ago", date_sub(col("today"), 7))\
.select(datediff(col("week_ago"), col("today"))).show(1)

dateDF.select(
to_date(lit("2016-01-01")).alias("start"),
to_date(lit("2017-05-22")).alias("end"))\
.select(months_between(col("start"), col("end"))).show(1)

#incorrect date format
dateDF.select(to_date(lit("2016-20-12")),to_date(lit("2017-12-11"))).show(1)

#Give date format
from pyspark.sql.functions import to_date
dateFormat = "yyyy-dd-MM"
cleanDateDF = spark.range(1).select(
to_date(lit("2017-12-11"), dateFormat).alias("date"),
to_date(lit("2017-20-12"), dateFormat).alias("date2"))
cleanDateDF.show()

#Handle timestamp to date  format casting
from pyspark.sql.functions import to_timestamp, year, month, dayofmonth, hour, minute, second
cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()

#get year using the year function with date and timepstamp
cleanDateDF.select(year(to_timestamp(col("date"), dateFormat))).show()

#get month using the month function with date and timepstamp
cleanDateDF.select(month(to_timestamp(col("date"), dateFormat))).show()

#get dayofmonth using the dayofmonth function with date and timepstamp
cleanDateDF.select(dayofmonth(to_timestamp(col("date"), dateFormat))).show()


#get hour using the hour function with date and timepstamp
cleanDateDF.select(hour(to_timestamp(col("date"), dateFormat))).show()


#get minute using the minute function with date and timepstamp
cleanDateDF.select(minute(to_timestamp(col("date"), dateFormat))).show()


#get second using the second function with date and timepstamp
cleanDateDF.select(second(to_timestamp(col("date"), dateFormat))).show()


# COMMAND ----------

# MAGIC %md
# MAGIC #Coalesce
# MAGIC
# MAGIC Spark includes a function to allow you to select the first non-null value from a set of columns by
# MAGIC using the coalesce function.

# COMMAND ----------

from pyspark.sql.functions import coalesce
df.select(coalesce(col("Description"), col("CustomerId"))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #ifnull, nullIf, nvl, and nvl2
# MAGIC
# MAGIC There are several other SQL functions that you can use to achieve similar things. ifnull allows
# MAGIC you to select the second value if the first is null, and defaults to the first. Alternatively, you could
# MAGIC use nullif, which returns null if the two values are equal or else returns the second if they are
# MAGIC not. nvl returns the second value if the first is null, but defaults to the first. Finally, nvl2 returns
# MAGIC the second value if the first is not null; otherwise, it will return the last specified value

# COMMAND ----------

# MAGIC %sql
# MAGIC /*SELECT
# MAGIC ifnull(null, 'return_value'),
# MAGIC nullif('value', 'value'),
# MAGIC nvl(null, 'return_value'),
# MAGIC nvl2('not_null', 'return_value', "else_value")
# MAGIC FROM dfTable LIMIT 1*/

# COMMAND ----------

# MAGIC %md
# MAGIC #Drop
# MAGIC The simplest function is drop, which removes rows that contain nulls. The default is to drop any
# MAGIC row in which any value is null:

# COMMAND ----------

df.na.drop()

#Specifying "any" as an argument drops a row if any of the values are null. Using “all” drops the row only if all values are null or NaN for that row:
df.na.drop("any")
df.na.drop("all")

#We can also apply this to certain sets of columns by passing in an array of columns:
df.na.drop("all", subset=["Country", "Region"])


# COMMAND ----------

# MAGIC %md
# MAGIC #Fill 
# MAGIC
# MAGIC Using the fill function, you can fill one or more columns with a set of values. This can be done
# MAGIC by specifying a map—that is a particular value and a set of columns.

# COMMAND ----------

#For example, to fill all null values in columns of type String, you might specify the following:
df.na.fill("All Null values become this string")
df.na.fill("all", subset=["Country", "Region"])

fill_cols_vals = {"UnitsSold": 5, "Region" : "No Value"}
df.na.fill(fill_cols_vals)


# COMMAND ----------

# MAGIC %md
# MAGIC #Split column to array

# COMMAND ----------

from pyspark.sql.functions import split
df.select(split(col("Region"), " ")).show(2)

df.select(split(col("Region"), " ").alias("Region_Array"))\
.selectExpr("Region_Array[0]").show(2)


# COMMAND ----------

# MAGIC %md
# MAGIC Array Size

# COMMAND ----------

# in Python
from pyspark.sql.functions import size
df.select(size(split(col("Region"), " "))).show(2) # shows 5 and 3

#Array contains
from pyspark.sql.functions import array_contains
df.select(array_contains(split(col("Region"), " "), "North")).show(2)

# COMMAND ----------

#Where NotNul function
from pyspark.sql.functions import *
df2 = df.select("Region", "Country", "UnitsSold").where(df.Country == "Libya")
df2.show()
df2 = df.select("Region", "Country", df["UnitsSold"]+1).where(df.Country == "Libya")
df2.show()
df2.show(5, truncate=False)
df2 = df.select("Region", "Country", df["UnitsSold"].isNotNull()).where(df.Country == "Libya")



# COMMAND ----------

#df2= df.filter((df.Country  == "Libya") |  (df.Country  == "Japan"))
#df2.show()
df.createOrReplaceTempView("Order")
df2 = spark.sql ("select * from Order where Country in ('Libya', 'Japan')")
df2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #explode
# MAGIC
# MAGIC The explode function takes a column that consists of arrays and creates one row (with the rest of
# MAGIC the values duplicated) per value in the array

# COMMAND ----------

from pyspark.sql.functions import split, explode
df.withColumn("splitted", split(col("Region"), " "))\
.withColumn("exploded", explode(col("splitted")))\
.select("Region", "Country", "exploded").show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC Maps
# MAGIC Maps are created by using the map function and key-value pairs of columns. You then can select
# MAGIC them just like you might select from an array:

# COMMAND ----------

from pyspark.sql.functions import create_map
df.select(create_map(col("Region"), col("Country")).alias("complex_map"))\
.show(2)

#query map
#df.select(map(col("Region"), col("Country")).alias("complex_map"))\
#.selectExpr("complex_map['Middle']").show(2)


# COMMAND ----------

# MAGIC %md
# MAGIC #Aggregate Function

# COMMAND ----------

# in Python
from pyspark.sql.functions import count, countDistinct
df.select(count("Region")).show() 

#countDistinct
df.select(countDistinct("Region")).show()
#-- in SQL
#SELECT COUNT(DISTINCT *) FROM DFTABLE

#first and last
#You can get the first and last values from a DataFrame by using these two obviously named functions.

# in Python
from pyspark.sql.functions import first, last
df.select(first("Region"), last("Region")).show()

#min and max
#To extract the minimum and maximum values from a DataFrame, use the min and max functions:

from pyspark.sql.functions import min, max
df.select(min("UnitsSold"), max("UnitsSold")).show()

#sum
#Another simple task is to add all the values in a row using the sum function:
from pyspark.sql.functions import sum
df.select(sum("UnitsSold")).show()

#sumDistinct
#In addition to summing a total, you also can sum a distinct set of values by using the sumDistinct function:
from pyspark.sql.functions import sumDistinct
df.select(sumDistinct("UnitsSold")).show() 

#avg

from pyspark.sql.functions import avg
df.select(avg("UnitsSold")).show() 



#Variance and Standard Deviation for ppulation and sample
from pyspark.sql.functions import var_pop, stddev_pop
from pyspark.sql.functions import var_samp, stddev_samp
df.select(var_pop("Quantity"), var_samp("Quantity"),
stddev_pop("Quantity"), stddev_samp("Quantity")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #Grouping
# MAGIC
# MAGIC  this returns another DataFrame and is lazily performed. We do this grouping in two phases. First we specify the column(s) on which we would like to
# MAGIC group, and then we specify the aggregation(s). The first step returns a
# MAGIC RelationalGroupedDataset, and the second step returns a DataFrame.

# COMMAND ----------

df.groupBy("Region", "Country").count().show()

#-- in SQL
#SELECT count(*) FROM dfTable GROUP BY InvoiceNo, CustomerId

# in Python
#Rather than passing that function as an expression into a select statement, we specify it as within agg. This makes it possible for you to pass-in arbitrary expressions that just need to have some aggregation specified. You can even do things like alias a column after transforming it for later use in your data flow:

from pyspark.sql.functions import count
df.groupBy("Region").agg(
count("UnitsSold").alias("quan"),
expr("count(UnitsSold)")).show()


# COMMAND ----------

# MAGIC %md
# MAGIC #Joins

# COMMAND ----------

#Inner joins are the default join, so we just need to specify our left DataFrame and join the right in the JOIN expression:

person = spark.createDataFrame([
(0, "Bill Chambers", 0, [100]),
(1, "Matei Zaharia", 1, [500, 250, 100]),
(2, "Michael Armbrust", 1, [250, 100])])\
.toDF("id", "name", "graduate_program", "spark_status")
graduateProgram = spark.createDataFrame([
(0, "Masters", "School of Information", "UC Berkeley"),
(2, "Masters", "EECS", "UC Berkeley"),
(1, "Ph.D.", "EECS", "UC Berkeley")])\
.toDF("id", "degree", "department", "school")
sparkStatus = spark.createDataFrame([
(500, "Vice President"),
(250, "PMC Member"),
(100, "Contributor")])\
.toDF("id", "status")

joinExpression = person["graduate_program"] == graduateProgram['id']
person.join(graduateProgram, joinExpression).show()


# specify the join type
joinType = "inner"
person.join(graduateProgram, joinExpression, joinType).show()

#Outer Joins
#Outer joins evaluate the keys in both of the DataFrames or tables and includes (and joins together) the rows that evaluate to true or false. If there is no equivalent row in either the left or right DataFrame, Spark will insert null:

joinType = "outer"
person.join(graduateProgram, joinExpression, joinType).show()

#Left Outer Joins
#Left outer joins evaluate the keys in both of the DataFrames or tables and includes all rows from the left DataFrame as well as any rows in the right DataFrame that have a match in the left DataFrame. If there is no equivalent row in the right DataFrame, Spark will insert null:

joinType = "left_outer"
graduateProgram.join(person, joinExpression, joinType).show()


#Right Outer Joins
#Right outer joins evaluate the keys in both of the DataFrames or tables and includes all rows from the right DataFrame as well as any rows in the left DataFrame that have a match in the right DataFrame. If there is no equivalent row in the left DataFrame, Spark will insert null:

joinType = "right_outer"
person.join(graduateProgram, joinExpression, joinType).show()

#Natural Joins
#Natural joins make implicit guesses at the columns on which you would like to join. It finds matching columns and returns the results. Left, right, and outer natural joins are all supported.

#-- in SQL
#SELECT * FROM graduateProgram NATURAL JOIN person

#Cross (Cartesian) Joins
#The last of our joins are cross-joins or cartesian products. Cross-joins in simplest terms are inner joins that do not specify a predicate. Cross joins will join every single row in the left DataFrame to ever single row in the right DataFrame. This will cause an absolute explosion in the number of rows contained in the resulting DataFrame. If you have 1,000 rows in each DataFrame, the crossjoin of these will result in 1,000,000 (1,000 x 1,000) rows. For this reason, you must very explicitly state that you want a cross-join by using the cross join keyword:

joinType = "cross"
graduateProgram.join(person, joinExpression, joinType).show()

#-- in SQL
#SELECT * FROM graduateProgram CROSS JOIN person
#ON graduateProgram.id = person.graduate_program

#If you truly intend to have a cross-join, you can call that out explicitly:
person.crossJoin(graduateProgram).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #Write to dataframe <br>
# MAGIC
# MAGIC append::           Appends the output files to the list of files that already exist at that location<br>
# MAGIC overwrite::         Will completely overwrite any data that already exists there<br>
# MAGIC errorIfExists::     Throws an error and fails the write if data or files already exist at the specified location<br>
# MAGIC ignore::            If data or files exist at the location, do nothing with the current DataFrame<br>

# COMMAND ----------

dataframe.write.format("csv")
.option("mode", "OVERWRITE")
.option("dateFormat", "yyyy-MM-dd")
.option("path", "path/to/file(s)")
.save()




# COMMAND ----------

# MAGIC %md
# MAGIC Parquet Format
# MAGIC
# MAGIC Parquet Files
# MAGIC Parquet is an open source column-oriented data store that provides a variety of storage
# MAGIC optimizations, especially for analytics workloads. It provides columnar compression, which
# MAGIC saves storage space and allows for reading individual columns instead of entire files. It is a file
# MAGIC format that works exceptionally well with Apache Spark and is in fact the default file format. We
# MAGIC recommend writing data out to Parquet for long-term storage because reading from a Parquet file
# MAGIC will always be more efficient than JSON or CSV. Another advantage of Parquet is that it
# MAGIC supports complex types. This means that if your column is an array (which would fail with a
# MAGIC CSV file, for example), map, or struct, you’ll still be able to read and write that file without
# MAGIC issue. Here’s how to specify Parquet as the read format:

# COMMAND ----------

spark.read.format("parquet")\
.load("/data/flight-data/parquet/2010-summary.parquet").show(5)

csvFile.write.format("parquet").mode("overwrite")\
.save("/tmp/my-parquet-file.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC #Explain

# COMMAND ----------

dbDataFrame.explain()

# COMMAND ----------

#Filter Functions
df2= df.filter(df.Country != "Libya")
df2.show()

#Multiple Condition
df2= df.filter((df.Country  == "Libya") |  (df.Country  == "Japan"))
df2.show()

#List of values filter
li=["Libya","Japan"]
df.filter(df.Country.isin(li)).show()

#Like Filter
df.filter(df.Country.like("%J%")).show()

#Regular Expression Filter
df.filter(df.Country.rlike("(?i)^*L$")).show()

#Assuming that one of the column in dataframe is Array then You can run filter using the below code
#from pyspark.sql.functions import array_contains
#df.filter(array_contains(df.phoneNumbers,"123")).show()




# COMMAND ----------

df2 =df.withColumn("UnitSoldNew",df.UnitsSold+1)
df3 = df2.select("UnitSoldNew")
df4 = df3.drop()
df4.show()



# COMMAND ----------

#Add Columns
df2 =df.withColumn("UnitSoldNew",df.UnitsSold+1)
df2.show()

# Column Rename
df2 = df2.withColumnRenamed("UnitSoldNew","UnitSoldRenamed")
df2.show()

#Column Drop
df2 = df2.drop("UnitSoldRenamed")
#or
df2 = df2.drop(df2.UnitsSold)
df2.show()

#Change DataType of Column
df2 = df.withColumn("UnitsSold",df.UnitsSold.cast("Double")).show()
df2.printSchema()

# COMMAND ----------

#Show Function Variation
df.show(n=3,truncate=25,vertical=True)
df.show()
df.show(10)
df.show(truncate=False)

#Usually, collect() is used to retrieve the action output when you have very small result set and calling collect() on an RDD/DataFrame with a bigger result set causes out of memory as it returns the entire dataset (from all workers) to the driver hence we should avoid calling collect() on a larger dataset.


# COMMAND ----------

#String Functions

from pyspark.sql import functions as F

df2 =df.select("Region","Country",F.when(df.UnitsSold > 2000, 1).otherwise(0))
df2.show()

#check isin
df2 = df[df.Country.isin("Libiya" ,"Japan")]
df2.show()

#Like
df2 = df.select("Region","Country", df.Country.like("L" ))
df2.show()

#StartsWith
df2 = df.select("Region","Country", df.Country.startswith("L"))
df2.show()
 
df2 = df.select("Region","Country", df.Country.endswith("L"))
df2.show()



# COMMAND ----------

#Missing Data

#Replace null with given value for the list of columns
df2 = df.fillna(value=0,subset=["Country"])
df2.show()

#Another way to replace
df2= df.na.fill(value=0, subset=["Country"])
df2.show()

#Replace null for all columns
df2 = df.na.fill(0)
df2.show()


#drop all null rows
df2 = df.na.drop()
df2.show() 

#replace value
df2 = df.na.replace(10, 20)

 .show() 

# COMMAND ----------

#Distinct / Remove Duplicate
df2 = df.dropDuplicates()  
dropDisDF = df.dropDuplicates(["Country","UnitsSold"])
df2 =df.distinct()

# COMMAND ----------

#Sorting and Order By
df.sort("Country","Region").show(truncate=False)
df.sort(df.Country,df.Region).show(truncate=False)

#Ascending / descending
df.sort(df.Country.asc(),df.Region.desc()).show(truncate=False)

#Order bY function is also works same as sort
df.orderBy(df.Country.asc(),df.Region.desc()).show(truncate=False)


# COMMAND ----------

from pyspark.sql.functions import col,sum,avg,max
#group by
df.groupBy(df.Country).count().show()

#Multiple column grouping
df.groupBy(df.Country, df.Region).sum("UnitsSold").show()

#Multiple aggregation
df.groupBy(df.Country, df.Region).sum("UnitsSold","UnitCost").show()

#Multiple Different Type of aggregation
df.printSchema()
df.groupBy(df.Country, df.Region).sum("UnitsSold").where(col("sum(UnitsSold)")>1000).show()


# COMMAND ----------

# MAGIC %fs
# MAGIC ls 

# COMMAND ----------

# MAGIC %sql
# MAGIC --CREATE DATABASE sample
# MAGIC
# MAGIC select * from OrderTable

# COMMAND ----------


#CREATE DATABASE sample
#df.write.mode("overwrite").saveAsTable("OrderTable18Dec")
dbutils.fs("ls")

# COMMAND ----------

#df.write.mode("overwrite").saveAsTable("OrdeYabler")
# dataframe.write.mode("overwrite").option("path","<your-storage-path>").saveAsTable("<example-table>")   Unmanaged Overwrite
df.write.mode("overwrite").partitionBy("Country").saveAsTable("OrderPartition18Dec")

# COMMAND ----------

# MAGIC %fs
# MAGIC rm /user/hive/warehouse/ordeyabler/part-00000-dd01437f-1f50-4cc6-91dd-f7b1b194476e-c000.snappy.parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC --REFRESH TABLE OrdeYabler
# MAGIC --select * from OrdeYabler
# MAGIC --Drop table OrdeYabler
# MAGIC --CRUD
# MAGIC -- SELECT * FROM OrderPartition18Dec WHERE OrderID=686800706
# MAGIC -- DESCRIBE history OrderPartition18Dec
# MAGIC
# MAGIC SELECT * FROM OrderPartition18Dec VERSION AS OF 1 WHERE OrderID=686800706
# MAGIC UPDATE  VERSION AS OF 0 SET Country = 'Libya' WHERE  OrderID=686800706
# MAGIC
# MAGIC
# MAGIC  --SELECT * FROM order_delta WHERE OrderID=686800706
# MAGIC  -- DELETE FROM OrderPartition18Dec WHERE OrderID=686800706
# MAGIC -- Confirm the user's data was deleted
# MAGIC /*SELECT * FROM order_delta WHERE OrderID=686800706
# MAGIC
# MAGIC INSERT INTO order_delta
# MAGIC SELECT * FROM order_delta VERSION AS OF 0
# MAGIC WHERE OrderID=686800706
# MAGIC
# MAGIC UPDATE  VERSION AS OF 0 SET Country = 'Libya' WHERE  OrderID=686800706
# MAGIC
# MAGIC
# MAGIC -- Vacuum deletes all files no longer needed by the current version of the table.
# MAGIC VACUUM  VERSION AS OF 0
# MAGIC
# MAGIC CACHE SELECT * FROM  VERSION AS OF 0
# MAGIC OPTIMIZE  VERSION AS OF 0 ZORDER BY Country
# MAGIC

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("order_delta5")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from order_delta5;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE loans_delta2
# MAGIC USING delta
# MAGIC AS SELECT * FROM parquet.'/tmp/delta_demo/loans_parquet'

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Use CONVERT TO DELTA to convert Parquet files to Delta Lake format in place
# MAGIC CONVERT TO DELTA parquet. /tmp/delta_demo/loans_parquet`

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE HISTORY order_delta
# MAGIC --View the Delta Lake transaction log

# COMMAND ----------

#Use Schema Evolution to add new columns to schema
new_data.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("loans_delta")

# COMMAND ----------

#verison
#DESCRIBE HISTORY loans_delta
spark.sql("SELECT * FROM order_delta VERSION AS OF 0").show(3)
#spark.sql("SELECT COUNT(*) FROM loans_delta VERSION AS OF 0").show()

#Rollback
%sql 
#RESTORE loans_delta VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql
# MAGIC --CRUD
# MAGIC
# MAGIC -- describe history order_delta
# MAGIC SELECT * FROM order_delta VERSION AS OF 1  WHERE OrderID=686800706

# COMMAND ----------

# MAGIC %sql
# MAGIC --SELECT * FROM order_delta5 WHERE OrderID=686800706
# MAGIC --DELETE FROM order_delta5 WHERE OrderID=686800706
# MAGIC SELECT * FROM order_delta5 VERSION AS OF 3 WHERE OrderID=686800706
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC --CRUD
# MAGIC SELECT * FROM order_delta WHERE OrderID=686800706
# MAGIC DELETE FROM order_delta WHERE OrderID=6868007060
# MAGIC -- Confirm the user's data was deleted
# MAGIC SELECT * FROM order_delta WHERE OrderID=686800706
# MAGIC
# MAGIC INSERT INTO order_delta
# MAGIC SELECT * FROM order_delta VERSION AS OF 0
# MAGIC WHERE OrderID=686800706
# MAGIC
# MAGIC UPDATE  VERSION AS OF 0 SET Country = 'Libya' WHERE  OrderID=686800706
# MAGIC
# MAGIC
# MAGIC -- Vacuum deletes all files no longer needed by the current version of the table.
# MAGIC VACUUM  VERSION AS OF 0
# MAGIC
# MAGIC CACHE SELECT * FROM  VERSION AS OF 0
# MAGIC OPTIMIZE  VERSION AS OF 0 ZORDER BY Country

# COMMAND ----------

