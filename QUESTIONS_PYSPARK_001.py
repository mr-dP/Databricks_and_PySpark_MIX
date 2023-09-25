# Databricks notebook source
# MAGIC %md
# MAGIC #####Question 01
# MAGIC <p>How can I create a table in Apache Spark with a column named 'date' that contains all the dates from 1st January to 31st December?<\p>

# COMMAND ----------

from pyspark.sql.functions import col, date_add, to_date, lit

date_range_df = spark.range(0, 365).withColumn(
    "date", date_add(to_date(lit("2023-01-01")), col("id").cast("int"))
)

# COMMAND ----------

display(date_range_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   date_add('2023-01-01', sequence) AS date
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       explode(sequence(0, 364)) AS sequence
# MAGIC   );

# COMMAND ----------



# COMMAND ----------


