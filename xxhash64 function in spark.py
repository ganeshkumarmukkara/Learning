# Databricks notebook source

# The xxhash64() function in PySpark is used to compute a 64-bit hash value for a given input. This function is based on the xxHash algorithm, which is a fast, non-cryptographic hashing algorithm. It's commonly used for generating hash values that are fast to compute and distribute well over a wide range of inputs.

# Key uses of xxhash64() in PySpark:
# Efficient Partitioning: When working with large datasets, you often need to distribute data evenly across partitions to avoid data skew. Using xxhash64() allows you to generate a consistent hash value that can be used to distribute records more evenly.

# Join Optimization: Hashing can be useful in optimizing joins between DataFrames by converting join keys to hash values, making comparison operations faster.

# Deduplication: You can use the hash value to identify duplicate records. If two records generate the same hash using xxhash64(), it indicates that they are likely duplicates.

# Fast Comparisons: It can be used for quickly comparing large strings or complex columns by converting them into fixed-size hash values and comparing the hashes instead of the original values.

# Performance:
# xxhash64() is particularly useful when you need a fast, low-overhead hash function, especially when working with big datasets in distributed systems like Spark. However, since it's non-cryptographic, it's not suitable for cases where cryptographic security is needed.

# COMMAND ----------

from pyspark.sql.functions import xxhash64

# Example DataFrame
df = spark.createDataFrame([
    ("Alice", 34),
    ("Bob", 45),
    ("Charlie", 23)
], ["name", "age"]) 

# Generate a hash value for each row based on the 'name' column
df_with_hash = df.withColumn("name_hash", xxhash64("name"))
df_with_hash.show()
