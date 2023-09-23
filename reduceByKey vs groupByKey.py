# Databricks notebook source
# MAGIC %md
# MAGIC #### reduceByKey()
# MAGIC
# MAGIC <p>reduceByKey() transformation is used to merge the values of each key using an associative reduce function on PySpark RDD. It is a wider transformation as it shuffles data across multiple partitions and It operates on pair RDD (key/value pair).<\p>
# MAGIC
# MAGIC
# MAGIC #### groupByKey()
# MAGIC
# MAGIC <p>PySpark groupByKey() is the most frequently used wide transformation operation that involves shuffling of data across the executors when data is not partitioned on the Key. It takes key-value pairs (K, V) as an input, groups the values based on the key(K), and generates a dataset of KeyValueGroupedDataset(K, Iterable) pairs as an output.<\p>

# COMMAND ----------

data = [
    ("Project", 1),
    ("Gutenberg’s", 1),
    ("Alice’s", 1),
    ("Adventures", 1),
    ("in", 1),
    ("Wonderland", 1),
    ("Project", 1),
    ("Gutenberg’s", 1),
    ("Adventures", 1),
    ("in", 1),
    ("Wonderland", 1),
    ("Project", 1),
    ("Gutenberg’s", 1),
]

rdd = sc.parallelize(data)

rdd.collect()

# COMMAND ----------

rdd.reduceByKey(lambda x, y: x + y).collect()

# COMMAND ----------



# COMMAND ----------

rdd.groupByKey().collect()

# COMMAND ----------

rdd.groupByKey().mapValues(len).collect()

# COMMAND ----------

rdd.groupByKey().mapValues(list).collect()

# COMMAND ----------


