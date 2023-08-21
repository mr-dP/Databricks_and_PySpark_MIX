# Databricks notebook source
# MAGIC %md
# MAGIC <p>Function in Apache Spark that takes another function as an argument</p>

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

data_arr = [
    (1, ["java", "azure", "python", None, "spark"], [6, 8, 9, 7, 10]),
    (4, ["python", "aws", "scala", "spark", None], [3, 7, 5, 8, 0]),
]

df_arr = spark.createDataFrame(data_arr, ["id", "skills", "ratings"])

data_map = [
    (1, {"java": 6, "azure": 8, "python": 9, "spark": 10}),
    (3, {".net": 9, "gcp": 5, "hive": 8, "sql": 8}),
]

df_map = spark.createDataFrame(data_map, ["id", "skills_map"])

# COMMAND ----------

df_arr.printSchema()

df_map.printSchema()

# COMMAND ----------

display(df_arr)

display(df_map)

# COMMAND ----------

# MAGIC %md
# MAGIC #### transform()
# MAGIC
# MAGIC <p>The transform() function in PySpark is designed to apply a user-defined transformation on each element of an array column in a DataFrame</p>

# COMMAND ----------

help(F.transform)

# COMMAND ----------

display(
    df_arr.select(
        "skills", F.transform("skills", lambda x: F.length(x)).alias("string_length")
    )
)

# COMMAND ----------

display(df_arr.selectExpr("skills", "TRANSFORM(skills, x -> length(x)) AS string_len"))

# COMMAND ----------

display(
    df_arr.select(
        "skills",
        "ratings",
        F.transform("ratings", lambda x: x * 10).alias("out_of_100"),
    )
)

# COMMAND ----------

display(
    df_arr.selectExpr(
        "skills", "ratings", "TRANSFORM(ratings, x -> x * 10) AS out_of_100"
    )
)

# COMMAND ----------

def add_10(x):
    return x + 10

# COMMAND ----------

display(
    df_arr.select(
        "ratings", F.transform("ratings", lambda x: add_10(x)).alias("ratings_plus_10")
    )
)

# COMMAND ----------

display(
    df_arr.select("ratings", F.transform("ratings", add_10).alias("ratings_plus_10"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### filter()
# MAGIC <p>Filter the arrays based on certian conditions</p>

# COMMAND ----------

help(F.filter)

# COMMAND ----------

display(
    df_arr.select(
        "ratings", F.filter("ratings", lambda x: x % 2 != 0).alias("odd_ratings")
    )
)

# COMMAND ----------

display(df_arr.selectExpr("ratings", "FILTER(ratings, x -> x % 2 != 0) AS odd_ratings"))

# COMMAND ----------

def odd_ratings(d):
    return d % 2 != 0

# COMMAND ----------

display(
    df_arr.select(
        "ratings", F.filter("ratings", lambda x: odd_ratings(x)).alias("odd_ratings")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### exists()
# MAGIC
# MAGIC <p>Return <code>true</code> if the condition is fulfilled for at least one of the element in the array otherwise <code>false</code></p>

# COMMAND ----------

help(F.exists)

# COMMAND ----------

display(
    df_arr.select(
        "skills", F.exists("skills", lambda x: x.startswith("j")).alias("skills_with_j")
    )
)

# COMMAND ----------

display(
    df_arr.selectExpr(
        "skills", "EXISTS(skills, x -> startswith(x, 'j')) AS skills_with_j"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### forall()
# MAGIC
# MAGIC <p>Return <code>true</code> if all the elements fulfill the condition passed to it otherwise <code>false</code></p>

# COMMAND ----------

help(F.forall)

# COMMAND ----------

display(
    df_arr.select(
        "ratings",
        F.forall("ratings", lambda x: x < 5).alias(
            "all_ratings_less_than_5"
        ),  # all ratings are less than 5?
        F.forall("ratings", lambda x: x < 10).alias(
            "all_ratings_less_than_10"
        ),  # all ratings are less than 10?
    )
)

# COMMAND ----------

display(
    df_arr.selectExpr(
        "ratings",
        "FORALL(ratings, x -> x < 5) AS all_ratings_less_than_5",
        "FORALL(ratings, x -> x < 10) AS all_ratings_less_than_10",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### aggregate()

# COMMAND ----------

help(F.aggregate)

# COMMAND ----------

display(
    df_arr.select(
        "ratings",
        F.aggregate("ratings", F.lit(0.0), lambda x, y: x + y).alias("ratings_sum"),
    )
)

# COMMAND ----------

display(
    df_arr.select(
        "ratings",
        F.aggregate("ratings", F.lit(0.0), lambda acc, x: acc + x).alias("ratings_sum"),
    )
)

# COMMAND ----------

def add(m, n):
    return m + n

# COMMAND ----------

display(
    df_arr.select(
        "ratings",
        F.aggregate("ratings", F.lit(0.0), add).alias("ratings_sum"),
    )
)

# COMMAND ----------

display(
    df_arr.selectExpr(
        "ratings", "AGGREGATE(ratings, 0, (x, y) -> INT(x + y)) AS ratings_total"
    )
)

# COMMAND ----------

display(
    df_arr.select(
        "ratings",
        F.aggregate("ratings", F.lit(0.0), add, lambda x: x + 100).alias(
            "sum_ratings_plus_100"
        ),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### zip_with()
# MAGIC
# MAGIC <p>Merge 2 columns together based on provided logic</p>

# COMMAND ----------

help(F.zip_with)

# COMMAND ----------

display(
    df_arr.select(
        "skills",
        "ratings",
        F.zip_with("ratings", "skills", lambda x, y: F.concat_ws(" --> ", x, y)).alias(
            "skills-ratings"
        ),
    )
)

# COMMAND ----------

display(
    df_arr.withColumn(
        "ratings_plus_10", F.transform("ratings", lambda x: x + 10)
    ).select(
        "*",
        F.zip_with("ratings", "ratings_plus_10", lambda x, y: x + y).alias(
            "ratings_sum"
        ),
    )
)

# COMMAND ----------

display(
    df_arr.withColumn(
        "ratings_plus_10", F.transform("ratings", lambda x: x + 10)
    ).selectExpr(
        "*", "ZIP_WITH(ratings, ratings_plus_10, (x, y) -> x + y) AS ratings_sum"
    )
)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### map_filter()
# MAGIC
# MAGIC <p>filter the map based on some condition</p>

# COMMAND ----------

help(F.map_filter)

# COMMAND ----------

display(df_map)

# COMMAND ----------

display(
    df_map.select(
        "*", F.map_filter("skills_map", lambda x, y: y > 8).alias("rating_gt_8")
    )
)

# COMMAND ----------

display(
    df_map.selectExpr(
        "*", "MAP_FILTER(skills_map, (key, value) -> value > 8) AS rating_gt_8"
    )
)

# COMMAND ----------

display(
    df_map.select(
        "skills_map",
        F.map_filter("skills_map", lambda key, val: key.contains("e")).alias(
            "skills_containing_e"
        ),
    )
)

# COMMAND ----------

display(df_map.selectExpr("*", "MAP_FILTER(skills_map, (x, y) -> x LIKE '%e%')"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### map_zip_with()
# MAGIC
# MAGIC <p>Merge the values of the 2 maps based on the keys with the help of merge function</p>

# COMMAND ----------

help(F.map_zip_with)

# COMMAND ----------

display(
    df_map.withColumn("skills_map_dup", F.col("skills_map")).select(
        "*",
        F.map_zip_with(
            "skills_map", "skills_map_dup", lambda key, val1, val2: val1 + val2
        ).alias("new_zipped_map"),
    )
)

# COMMAND ----------

display(
    df_map.selectExpr(
        "*",
        "skills_map AS skills_map_dup",
        "MAP_ZIP_WITH(skills_map, skills_map_dup, (k, v1, v2) -> v1 + v2) AS new_zipped_map",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### transform_keys()
# MAGIC
# MAGIC <p>Transform the keys in map</p>

# COMMAND ----------

help(F.transform_keys)

# COMMAND ----------

display(df_map.select("*", F.transform_keys("skills_map", lambda x, y: F.upper(x))))

# COMMAND ----------

display(
    df_map.selectExpr(
        "*", "TRANSFORM_KEYS(skills_map, (key, value) -> upper(key)) AS keys_upper"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### transform_values()
# MAGIC
# MAGIC <p>Transform the values in map</p>

# COMMAND ----------

help(F.transform_values)

# COMMAND ----------

display(
    df_map.select(
        "*", F.transform_values("skills_map", lambda x, y: y * 100).alias("num_100")
    )
)

# COMMAND ----------

display(
    df_map.selectExpr(
        "*", "TRANSFORM_VALUES(skills_map, (x, y) -> y * 1000) AS num_1000"
    )
)

# COMMAND ----------


