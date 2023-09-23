# Databricks notebook source
# MAGIC %md
# MAGIC #### map()
# MAGIC <p>Spark map() transformation applies a function to each row in a DataFrame/Dataset and returns the new transformed Dataset.<\p>
# MAGIC
# MAGIC #### flatMap()
# MAGIC <p>Spark flatMap() transformation flattens the DataFrame/Dataset after applying the function on every element and returns a new transformed Dataset. The returned Dataset will return more rows than the current DataFrame. It is also referred to as a one-to-many transformation function<\p>

# COMMAND ----------

sc.parallelize([3, 5, 7, 9, 11, 13]).map(lambda x: range(1, x)).collect()

# COMMAND ----------

sc.parallelize([3, 5, 7, 9, 11, 13]).flatMap(lambda x: range(1, x)).collect()

# COMMAND ----------



# COMMAND ----------

data = [
    "Project Gutenberg’s",
    "Alice’s Adventures in Wonderland",
    "Project Gutenberg’s",
    "Adventures in Wonderland",
    "Project Gutenberg’s",
]

rdd = sc.parallelize(data)

rdd.collect()

# COMMAND ----------

mapRDD = rdd.map(lambda x: x.split(" "))

print(mapRDD.collect())

print(mapRDD.count())

# COMMAND ----------

flatMapRDD = rdd.flatMap(lambda x: x.split(" "))

print(flatMapRDD.collect())

print(flatMapRDD.count())

# COMMAND ----------


