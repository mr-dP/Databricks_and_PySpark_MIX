# Databricks notebook source
# MAGIC %sql
# MAGIC -- TYPE 1         APPEND
# MAGIC -- TYPE 2         UPSERT/MERGE       Only latest data                       No history
# MAGIC -- TYPE 3         UPSERT/MERGE       Historical data (N number of times)    History in new row
# MAGIC -- TYPE 4         UPSERT/MERGE       Only one time history                  History in another column

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### SCD Type 0 - append / insert only
# MAGIC
# MAGIC ###### If source is going to send full data or new data only then we can chose this operation

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/user/hive/warehouse/dept

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dept;
# MAGIC
# MAGIC CREATE TABLE dept (
# MAGIC   deptno DECIMAL(2),
# MAGIC   dname VARCHAR(15),
# MAGIC   loc VARCHAR(13)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO dept
# MAGIC
# MAGIC SELECT 10 AS deptno,
# MAGIC 	'ACCOUNTING' AS dname,
# MAGIC 	'NEW YORK' AS loc
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 20 AS deptno,
# MAGIC 	'RESEARCH' AS dname,
# MAGIC 	'DALLAS' AS loc
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 30 AS deptno,
# MAGIC 	'SALES' AS dname,
# MAGIC 	'CHICAGO' AS loc
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 40 AS deptno,
# MAGIC 	'OPERATIONS' AS dname,
# MAGIC 	'BOSTON' AS loc;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   dept;
# MAGIC
# MAGIC -- SCD Type 0 - APPEND or INSERT only

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/user/hive/warehouse/dept_source

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dept_source;
# MAGIC
# MAGIC CREATE TABLE dept_source (
# MAGIC   deptno DECIMAL(2),
# MAGIC   dname VARCHAR(15),
# MAGIC   loc VARCHAR(13)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO dept_source
# MAGIC
# MAGIC SELECT 10 AS deptno,
# MAGIC 	'ACCOUNTING' AS dname,
# MAGIC 	'NEW YORK' AS loc
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 40 AS deptno,
# MAGIC 	'OPERATIONS' AS dname,
# MAGIC 	'BOSTON' AS loc
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 50 AS deptno,
# MAGIC 	'IT' AS dname,
# MAGIC 	'BENGALURU' AS loc
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 60 AS deptno,
# MAGIC 	'HRMS' AS dname,
# MAGIC 	'HYDERABAD' AS loc;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   dept_source;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- deptno "10" and "40" are already available in the target table
# MAGIC -- If we do INSERT statement, then 10 and 40 will be inserted again
# MAGIC -- But we need to insert only which is not there in the target
# MAGIC
# MAGIC -- ANTI-JOIN can be performed in this case or we can use INSERT...SELECT query like following:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   dept_source
# MAGIC WHERE
# MAGIC   deptno NOT IN (
# MAGIC     SELECT
# MAGIC       deptno
# MAGIC     FROM
# MAGIC       dept
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM dept_source src
# MAGIC   ANTI JOIN dept tgt ON src.deptno = tgt.deptno;

# COMMAND ----------

# MAGIC %md
# MAGIC ###### INSERT only un-matched data which is not available in target

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO
# MAGIC   dept
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   dept_source
# MAGIC WHERE
# MAGIC   deptno NOT IN (
# MAGIC     SELECT
# MAGIC       deptno
# MAGIC     FROM
# MAGIC       dept
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   dept;

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### SCD Type 1 - overwrite
# MAGIC
# MAGIC ###### The new overwrites the exsiting data. Thus the existing data is lost as it is not stored anywhere else. This is the default type of dimension you create. You do not need to specify any additional information to create a Type 1 SCD
# MAGIC ###### In SCD TYPE 1, normally we do not use DELETE

# COMMAND ----------

# MAGIC %md
# MAGIC ##### MERGE INTO
# MAGIC ###### Merges a set of updates, insertions, and deletions based on a source table into a target Delta table.

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/user/hive/warehouse/customer_target

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS customer_target;
# MAGIC
# MAGIC CREATE TABLE customer_target (
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   location STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO
# MAGIC   customer_target
# MAGIC VALUES
# MAGIC   (1, 'Ram', 'Chennai'),
# MAGIC   (2, 'Reshwanth', 'Hyderabad'),
# MAGIC   (3, 'Vikranth', 'Bengaluru');

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/user/hive/warehouse/customer_source

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS customer_source;
# MAGIC
# MAGIC CREATE TABLE customer_source (
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   location STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO
# MAGIC   customer_source
# MAGIC VALUES
# MAGIC   (1, 'Ram', 'Mumbai'),
# MAGIC   (4, 'Raj', 'Hyderabad'),
# MAGIC   (5, 'Prasad', 'Pune');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   customer_target;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   customer_source;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO customer_target tgt USING customer_source src ON tgt.id = src.id
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE
# MAGIC SET
# MAGIC   *
# MAGIC   WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC   *;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   customer_source;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   customer_target;

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/user/hive/warehouse/events

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS events;
# MAGIC
# MAGIC CREATE TABLE events (
# MAGIC   event_id INT,
# MAGIC   event_date DATE,
# MAGIC   data STRING,
# MAGIC   delete BOOLEAN
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO
# MAGIC   events
# MAGIC VALUES
# MAGIC   (1, '2021-01-01', 'sample event', 0),
# MAGIC   (2, '2021-02-02', 'sample event', 0),
# MAGIC   (3, '2021-03-03', 'sample event', 0),
# MAGIC   (4, '2021-04-04', 'sample event', 0);

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/user/hive/warehouse/updates

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS updates;
# MAGIC
# MAGIC CREATE TABLE updates (
# MAGIC   event_id INT,
# MAGIC   event_date DATE,
# MAGIC   data STRING,
# MAGIC   delete BOOLEAN
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO
# MAGIC   updates
# MAGIC VALUES
# MAGIC   (5, '2021-01-01', '5th event', 0),
# MAGIC   (6, '2021-02-02', '6th event', 0),
# MAGIC   (7, '2021-03-03', '7th event', 0),
# MAGIC   (2, '2021-03-03', '7th event', 1),
# MAGIC   (1, '2021-04-04', '1st event', 0);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   events;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   updates;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO events tgt USING updates src ON tgt.event_id = src.event_id
# MAGIC WHEN MATCHED
# MAGIC AND src.DELETE = true THEN DELETE
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE
# MAGIC SET
# MAGIC   tgt.event_date = src.event_date,
# MAGIC   tgt.data = src.data
# MAGIC   WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC   (
# MAGIC     event_id,
# MAGIC     event_date,
# MAGIC     data,
# MAGIC     DELETE
# MAGIC   )
# MAGIC VALUES
# MAGIC   (
# MAGIC     event_id,
# MAGIC     event_date,
# MAGIC     data,
# MAGIC     DELETE
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   updates;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   events;

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### SCD Type 2

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/user/hive/warehouse/customer_dim

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS customer_dim;
# MAGIC
# MAGIC CREATE TABLE customer_dim (
# MAGIC   customer_id INT,
# MAGIC   name STRING,
# MAGIC   address STRING,
# MAGIC   current BOOLEAN,
# MAGIC   effectiveDate TIMESTAMP,
# MAGIC   endDate TIMESTAMP
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO
# MAGIC   customer_dim
# MAGIC VALUES
# MAGIC   (
# MAGIC     1,
# MAGIC     'Mahesh',
# MAGIC     'Bengaluru',
# MAGIC     1,
# MAGIC     current_timestamp(),
# MAGIC     '9999-12-31'
# MAGIC   ),
# MAGIC   (
# MAGIC     2,
# MAGIC     'Ram',
# MAGIC     'Hyderabad',
# MAGIC     1,
# MAGIC     current_timestamp(),
# MAGIC     '9999-12-31'
# MAGIC   ),
# MAGIC   (
# MAGIC     3,
# MAGIC     'Ravi',
# MAGIC     'Chennai',
# MAGIC     1,
# MAGIC     current_timestamp(),
# MAGIC     '9999-12-31'
# MAGIC   ),
# MAGIC   (
# MAGIC     4,
# MAGIC     'Raj',
# MAGIC     'Pune',
# MAGIC     1,
# MAGIC     current_timestamp(),
# MAGIC     '9999-12-31'
# MAGIC   );

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/user/hive/warehouse/updates_source

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS updates_source;
# MAGIC
# MAGIC CREATE TABLE updates_source (
# MAGIC   customer_id INT,
# MAGIC   name STRING,
# MAGIC   address STRING,
# MAGIC   effectiveDate TIMESTAMP
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO
# MAGIC   updates_source
# MAGIC VALUES
# MAGIC   (
# MAGIC     5,
# MAGIC     'Sridhar',
# MAGIC     'Delhi',
# MAGIC     current_timestamp()
# MAGIC   ),
# MAGIC   (
# MAGIC     6,
# MAGIC     'Prasad',
# MAGIC     'Mumbai',
# MAGIC     current_timestamp()
# MAGIC   ),
# MAGIC   (
# MAGIC     2,
# MAGIC     'Ram',
# MAGIC     'Bengaluru',
# MAGIC     current_timestamp()
# MAGIC   ),
# MAGIC   (
# MAGIC     1,
# MAGIC     'Mahesh',
# MAGIC     'Hyderabad',
# MAGIC     current_timestamp()
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   customer_dim;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   updates_source;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- We select data from source UNION ALL seelect data from target as well which is having active status and different address
# MAGIC SELECT
# MAGIC   t.customer_id AS merge_key,
# MAGIC   t.*
# MAGIC FROM
# MAGIC   updates_source t
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   null AS merge_key,
# MAGIC   t1.*
# MAGIC FROM
# MAGIC   updates_source t1
# MAGIC   JOIN customer_dim t2 ON t1.customer_id = t2.customer_id
# MAGIC WHERE
# MAGIC   t2.current = 1
# MAGIC   AND t2.address <> t1.address;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO customer_dim tgt USING (
# MAGIC   SELECT
# MAGIC     t.customer_id AS merge_key,
# MAGIC     t.*
# MAGIC   FROM
# MAGIC     updates_source t
# MAGIC   UNION ALL
# MAGIC   SELECT
# MAGIC     null AS merge_key,
# MAGIC     t1.*
# MAGIC   FROM
# MAGIC     updates_source t1
# MAGIC     JOIN customer_dim t2 ON t1.customer_id = t2.customer_id
# MAGIC   WHERE
# MAGIC     t2.current = 1
# MAGIC     AND t2.address <> t1.address
# MAGIC ) staged_updates ON staged_updates.merge_key = tgt.customer_id
# MAGIC WHEN MATCHED
# MAGIC AND tgt.current = 1
# MAGIC and staged_updates.address <> tgt.address THEN
# MAGIC UPDATE
# MAGIC SET
# MAGIC   current = 0,
# MAGIC   endDate = staged_updates.effectiveDate
# MAGIC   WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC   (
# MAGIC     customer_id,
# MAGIC     name,
# MAGIC     address,
# MAGIC     current,
# MAGIC     effectiveDate,
# MAGIC     endDate
# MAGIC   )
# MAGIC VALUES
# MAGIC   (
# MAGIC     staged_updates.customer_id,
# MAGIC     staged_updates.name,
# MAGIC     staged_updates.address,
# MAGIC     1,
# MAGIC     staged_updates.effectiveDate,
# MAGIC     '9999-12-31'
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   customer_dim
# MAGIC ORDER BY
# MAGIC   customer_id,
# MAGIC   current;

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### SCD Type 3
# MAGIC
# MAGIC ###### Stores 2 versions of values for certain selected level attributes. Each record stores the previous value and the current value of the selected attribute. When the value of any of the selected attributes changes, the current value is stored as the old value and the new value becomes the current value

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/user/hive/warehouse/customer_type3

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS customer_type3;
# MAGIC
# MAGIC CREATE TABLE customer_type3 (
# MAGIC   customer_id INT,
# MAGIC   name STRING,
# MAGIC   current_loc STRING,
# MAGIC   previous_loc STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO customer_type3 (
# MAGIC 	customer_id,
# MAGIC 	name,
# MAGIC 	current_loc,
# MAGIC 	previous_loc
# MAGIC 	)
# MAGIC SELECT 1,
# MAGIC 	'Ravi',
# MAGIC 	'Bengaluru',
# MAGIC 	NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 2,
# MAGIC 	'Ram',
# MAGIC 	'Chennai',
# MAGIC 	NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 3,
# MAGIC 	'Prasad',
# MAGIC 	'Hyderabad',
# MAGIC 	NULL;

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/user/hive/warehouse/customer_src3

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS customer_src3;
# MAGIC
# MAGIC CREATE TABLE customer_src3 (
# MAGIC   customer_id INT,
# MAGIC   name STRING,
# MAGIC   location STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO
# MAGIC   customer_src3
# MAGIC VALUES
# MAGIC   (1, 'Ravi', 'Chennai'),
# MAGIC   (2, 'Ram', 'Hyderabad'),
# MAGIC   (4, 'Mahesh', 'Bengaluru'),
# MAGIC   (5, 'Sridhar', 'Hyderabad');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   customer_type3;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   customer_src3;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###### Approach 01 - without joining with target table

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO customer_type3 tgt USING customer_src3 src ON tgt.customer_id = src.customer_id
# MAGIC WHEN MATCHED
# MAGIC AND lower(src.location) <> lower(tgt.current_loc) THEN
# MAGIC UPDATE
# MAGIC SET
# MAGIC   tgt.previous_loc = tgt.current_loc,
# MAGIC   tgt.current_loc = src.location
# MAGIC   WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC   (customer_id, name, current_loc)
# MAGIC VALUES(src.customer_id, src.name, src.location);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   customer_type3;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   customer_src3;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###### Approach 02 - joining with target table get the comparison

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/user/hive/warehouse/customer_type3

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS customer_type3;
# MAGIC
# MAGIC CREATE TABLE customer_type3 (
# MAGIC   customer_id INT,
# MAGIC   name STRING,
# MAGIC   current_loc STRING,
# MAGIC   previous_loc STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO customer_type3 (
# MAGIC 	customer_id,
# MAGIC 	name,
# MAGIC 	current_loc,
# MAGIC 	previous_loc
# MAGIC 	)
# MAGIC SELECT 1,
# MAGIC 	'Ravi',
# MAGIC 	'Bengaluru',
# MAGIC 	NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 2,
# MAGIC 	'Ram',
# MAGIC 	'Chennai',
# MAGIC 	NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 3,
# MAGIC 	'Prasad',
# MAGIC 	'Hyderabad',
# MAGIC 	NULL;

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/user/hive/warehouse/customer_src3

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS customer_src3;
# MAGIC
# MAGIC CREATE TABLE customer_src3 (
# MAGIC   customer_id INT,
# MAGIC   name STRING,
# MAGIC   location STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO
# MAGIC   customer_src3
# MAGIC VALUES
# MAGIC   (1, 'Ravi', 'Chennai'),
# MAGIC   (2, 'Ram', 'Hyderabad'),
# MAGIC   (4, 'Mahesh', 'Bengaluru'),
# MAGIC   (5, 'Sridhar', 'Hyderabad');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   src.customer_id,
# MAGIC   src.name,
# MAGIC   src.location AS current_loc,
# MAGIC   CASE
# MAGIC     WHEN src.location <> tgt.current_loc THEN tgt.current_loc
# MAGIC     ELSE tgt.previous_loc
# MAGIC   END AS previous_loc
# MAGIC FROM
# MAGIC   customer_src3 src
# MAGIC   LEFT JOIN customer_type3 tgt on src.customer_id = tgt.customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO customer_type3 tgt USING (
# MAGIC   SELECT
# MAGIC     src.customer_id,
# MAGIC     src.name,
# MAGIC     src.location AS current_loc,
# MAGIC     CASE
# MAGIC       WHEN src.location <> tgt.current_loc THEN tgt.current_loc
# MAGIC       ELSE tgt.previous_loc
# MAGIC     END AS previous_loc
# MAGIC   FROM
# MAGIC     customer_src3 src
# MAGIC     LEFT JOIN customer_type3 tgt on src.customer_id = tgt.customer_id
# MAGIC ) src ON tgt.customer_id = src.customer_id
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE
# MAGIC SET
# MAGIC   tgt.customer_id = src.customer_id,
# MAGIC   tgt.name = src.name,
# MAGIC   tgt.current_loc = src.current_loc,
# MAGIC   tgt.previous_loc = src.previous_loc
# MAGIC   WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC   (customer_id, name, current_loc, previous_loc)
# MAGIC VALUES
# MAGIC   (
# MAGIC     src.customer_id,
# MAGIC     src.name,
# MAGIC     src.current_loc,
# MAGIC     src.previous_loc
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   customer_type3;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   customer_src3;

# COMMAND ----------


