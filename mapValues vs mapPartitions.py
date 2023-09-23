# Databricks notebook source
# MAGIC %md
# MAGIC #### mapValues()
# MAGIC
# MAGIC <p>mapValues() is a transformation operation that is available on a Pair RDD (i.e., an RDD of key-value pairs). It applies a transformation function to the values of each key-value pair in the RDD while keeping the key unchanged.<\p>
# MAGIC
# MAGIC #### mapPartitions()
# MAGIC
# MAGIC <p>mapPartitions() is a narrow transformation operation that applies a function to each partition of the RDD, if you have a DataFrame, you need to convert to RDD in order to use it. mapPartitions() is mainly used to initialize connections once for each partition instead of every row, this is the main difference between map() vs mapPartitions(). It is a narrow transformation as there will not be any data movement/shuffling between partitions to perform the function.<\p>

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

rdd.mapValues(lambda x: x + 2).collect()

# COMMAND ----------



# COMMAND ----------

data = [
    ("James", "Smith", "M", 3000),
    ("Anna", "Rose", "F", 4100),
    ("Robert", "Williams", "M", 6200),
]

columns = ["FirstName", "LastName", "Gender", "Salary"]

df = spark.createDataFrame(data=data, schema=columns)

df.display()

# COMMAND ----------

def reformat(partitionData):
    for row in partitionData:
        yield [row["LastName"] + ", " + row["FirstName"], row["Salary"] * 10 / 100]


df2 = df.rdd.mapPartitions(reformat).toDF(["name", "bonus"])

display(df2)

# COMMAND ----------


