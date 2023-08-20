-- Databricks notebook source
-- TYPE 1         APPEND
-- TYPE 2         UPSERT/MERGE       Only latest data                       No history
-- TYPE 3         UPSERT/MERGE       Historical data (N number of times)    History in new row
-- TYPE 4         UPSERT/MERGE       Only one time history                  History in another column

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### SCD Type 0 - append / insert only
-- MAGIC
-- MAGIC ###### If source is going to send full data or new data only then we can chose this operation

-- COMMAND ----------

-- MAGIC %fs rm -r dbfs:/user/hive/warehouse/dept

-- COMMAND ----------

DROP TABLE IF EXISTS dept;

CREATE TABLE dept (
  deptno DECIMAL(2),
  dname VARCHAR(15),
  loc VARCHAR(13)
);

-- COMMAND ----------

INSERT INTO dept

SELECT 10 AS deptno,
	'ACCOUNTING' AS dname,
	'NEW YORK' AS loc

UNION ALL

SELECT 20 AS deptno,
	'RESEARCH' AS dname,
	'DALLAS' AS loc

UNION ALL

SELECT 30 AS deptno,
	'SALES' AS dname,
	'CHICAGO' AS loc

UNION ALL

SELECT 40 AS deptno,
	'OPERATIONS' AS dname,
	'BOSTON' AS loc;


-- COMMAND ----------

SELECT
  *
FROM
  dept;

-- SCD Type 0 - APPEND or INSERT only

-- COMMAND ----------

-- MAGIC %fs rm -r dbfs:/user/hive/warehouse/dept_source

-- COMMAND ----------

DROP TABLE IF EXISTS dept_source;

CREATE TABLE dept_source (
  deptno DECIMAL(2),
  dname VARCHAR(15),
  loc VARCHAR(13)
);

-- COMMAND ----------

INSERT INTO dept_source

SELECT 10 AS deptno,
	'ACCOUNTING' AS dname,
	'NEW YORK' AS loc

UNION ALL

SELECT 40 AS deptno,
	'OPERATIONS' AS dname,
	'BOSTON' AS loc

UNION ALL

SELECT 50 AS deptno,
	'IT' AS dname,
	'BENGALURU' AS loc

UNION ALL

SELECT 60 AS deptno,
	'HRMS' AS dname,
	'HYDERABAD' AS loc;


-- COMMAND ----------

SELECT
  *
FROM
  dept_source;

-- COMMAND ----------

-- deptno "10" and "40" are already available in the target table
-- If we do INSERT statement, then 10 and 40 will be inserted again
-- But we need to insert only which is not there in the target

-- ANTI-JOIN can be performed in this case or we can use INSERT...SELECT query like following:

-- COMMAND ----------

SELECT
  *
FROM
  dept_source
WHERE
  deptno NOT IN (
    SELECT
      deptno
    FROM
      dept
  );

-- COMMAND ----------

SELECT *
FROM dept_source src
  ANTI JOIN dept tgt ON src.deptno = tgt.deptno;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### INSERT only un-matched data which is not available in target

-- COMMAND ----------

INSERT INTO
  dept
SELECT
  *
FROM
  dept_source
WHERE
  deptno NOT IN (
    SELECT
      deptno
    FROM
      dept
  );

-- COMMAND ----------

SELECT
  *
FROM
  dept;

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### SCD Type 1 - overwrite
-- MAGIC
-- MAGIC ###### The new overwrites the exsiting data. Thus the existing data is lost as it is not stored anywhere else. This is the default type of dimension you create. You do not need to specify any additional information to create a Type 1 SCD
-- MAGIC ###### In SCD TYPE 1, normally we do not use DELETE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### MERGE INTO
-- MAGIC ###### Merges a set of updates, insertions, and deletions based on a source table into a target Delta table.

-- COMMAND ----------

-- MAGIC %fs rm -r dbfs:/user/hive/warehouse/customer_target

-- COMMAND ----------

DROP TABLE IF EXISTS customer_target;

CREATE TABLE customer_target (
  id INT,
  name STRING,
  location STRING
);

-- COMMAND ----------

INSERT INTO
  customer_target
VALUES
  (1, 'Ram', 'Chennai'),
  (2, 'Reshwanth', 'Hyderabad'),
  (3, 'Vikranth', 'Bengaluru');

-- COMMAND ----------

-- MAGIC %fs rm -r dbfs:/user/hive/warehouse/customer_source

-- COMMAND ----------

DROP TABLE IF EXISTS customer_source;

CREATE TABLE customer_source (
  id INT,
  name STRING,
  location STRING
);

-- COMMAND ----------

INSERT INTO
  customer_source
VALUES
  (1, 'Ram', 'Mumbai'),
  (4, 'Raj', 'Hyderabad'),
  (5, 'Prasad', 'Pune');

-- COMMAND ----------

SELECT
  *
FROM
  customer_target;

-- COMMAND ----------

SELECT
  *
FROM
  customer_source;

-- COMMAND ----------

MERGE INTO customer_target tgt USING customer_source src ON tgt.id = src.id
WHEN MATCHED THEN
UPDATE
SET
  *
  WHEN NOT MATCHED THEN
INSERT
  *;

-- COMMAND ----------

SELECT
  *
FROM
  customer_source;

-- COMMAND ----------

SELECT
  *
FROM
  customer_target;

-- COMMAND ----------

-- MAGIC %fs rm -r dbfs:/user/hive/warehouse/events

-- COMMAND ----------

DROP TABLE IF EXISTS events;

CREATE TABLE events (
  event_id INT,
  event_date DATE,
  data STRING,
  delete BOOLEAN
);

-- COMMAND ----------

INSERT INTO
  events
VALUES
  (1, '2021-01-01', 'sample event', 0),
  (2, '2021-02-02', 'sample event', 0),
  (3, '2021-03-03', 'sample event', 0),
  (4, '2021-04-04', 'sample event', 0);

-- COMMAND ----------

-- MAGIC %fs rm -r dbfs:/user/hive/warehouse/updates

-- COMMAND ----------

DROP TABLE IF EXISTS updates;

CREATE TABLE updates (
  event_id INT,
  event_date DATE,
  data STRING,
  delete BOOLEAN
);

-- COMMAND ----------

INSERT INTO
  updates
VALUES
  (5, '2021-01-01', '5th event', 0),
  (6, '2021-02-02', '6th event', 0),
  (7, '2021-03-03', '7th event', 0),
  (2, '2021-03-03', '7th event', 1),
  (1, '2021-04-04', '1st event', 0);

-- COMMAND ----------

SELECT
  *
FROM
  events;

-- COMMAND ----------

SELECT
  *
FROM
  updates;

-- COMMAND ----------

MERGE INTO events tgt USING updates src ON tgt.event_id = src.event_id
WHEN MATCHED
AND src.DELETE = true THEN DELETE
WHEN MATCHED THEN
UPDATE
SET
  tgt.event_date = src.event_date,
  tgt.data = src.data
  WHEN NOT MATCHED THEN
INSERT
  (
    event_id,
    event_date,
    data,
    DELETE
  )
VALUES
  (
    event_id,
    event_date,
    data,
    DELETE
  );

-- COMMAND ----------

SELECT
  *
FROM
  updates;

-- COMMAND ----------

SELECT
  *
FROM
  events;

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### SCD Type 2

-- COMMAND ----------

-- MAGIC %fs rm -r dbfs:/user/hive/warehouse/customer_dim

-- COMMAND ----------

DROP TABLE IF EXISTS customer_dim;

CREATE TABLE customer_dim (
  customer_id INT,
  name STRING,
  address STRING,
  current BOOLEAN,
  effectiveDate TIMESTAMP,
  endDate TIMESTAMP
);

-- COMMAND ----------

INSERT INTO
  customer_dim
VALUES
  (
    1,
    'Mahesh',
    'Bengaluru',
    1,
    current_timestamp(),
    '9999-12-31'
  ),
  (
    2,
    'Ram',
    'Hyderabad',
    1,
    current_timestamp(),
    '9999-12-31'
  ),
  (
    3,
    'Ravi',
    'Chennai',
    1,
    current_timestamp(),
    '9999-12-31'
  ),
  (
    4,
    'Raj',
    'Pune',
    1,
    current_timestamp(),
    '9999-12-31'
  );

-- COMMAND ----------

-- MAGIC %fs rm -r dbfs:/user/hive/warehouse/updates_source

-- COMMAND ----------

DROP TABLE IF EXISTS updates_source;

CREATE TABLE updates_source (
  customer_id INT,
  name STRING,
  address STRING,
  effectiveDate TIMESTAMP
);

-- COMMAND ----------

INSERT INTO
  updates_source
VALUES
  (
    5,
    'Sridhar',
    'Delhi',
    current_timestamp()
  ),
  (
    6,
    'Prasad',
    'Mumbai',
    current_timestamp()
  ),
  (
    2,
    'Ram',
    'Bengaluru',
    current_timestamp()
  ),
  (
    1,
    'Mahesh',
    'Hyderabad',
    current_timestamp()
  );

-- COMMAND ----------

SELECT
  *
FROM
  customer_dim;

-- COMMAND ----------

SELECT
  *
FROM
  updates_source;

-- COMMAND ----------

-- We select data from source UNION ALL seelect data from target as well which is having active status and different address
SELECT
  t.customer_id AS merge_key,
  t.*
FROM
  updates_source t
UNION ALL
SELECT
  null AS merge_key,
  t1.*
FROM
  updates_source t1
  JOIN customer_dim t2 ON t1.customer_id = t2.customer_id
WHERE
  t2.current = 1
  AND t2.address <> t1.address;

-- COMMAND ----------

MERGE INTO customer_dim tgt USING (
  SELECT
    t.customer_id AS merge_key,
    t.*
  FROM
    updates_source t
  UNION ALL
  SELECT
    null AS merge_key,
    t1.*
  FROM
    updates_source t1
    JOIN customer_dim t2 ON t1.customer_id = t2.customer_id
  WHERE
    t2.current = 1
    AND t2.address <> t1.address
) staged_updates ON staged_updates.merge_key = tgt.customer_id
WHEN MATCHED
AND tgt.current = 1
and staged_updates.address <> tgt.address THEN
UPDATE
SET
  current = 0,
  endDate = staged_updates.effectiveDate
  WHEN NOT MATCHED THEN
INSERT
  (
    customer_id,
    name,
    address,
    current,
    effectiveDate,
    endDate
  )
VALUES
  (
    staged_updates.customer_id,
    staged_updates.name,
    staged_updates.address,
    1,
    staged_updates.effectiveDate,
    '9999-12-31'
  );

-- COMMAND ----------

SELECT
  *
FROM
  customer_dim
ORDER BY
  customer_id,
  current;

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### SCD Type 3
-- MAGIC
-- MAGIC ###### Stores 2 versions of values for certain selected level attributes. Each record stores the previous value and the current value of the selected attribute. When the value of any of the selected attributes changes, the current value is stored as the old value and the new value becomes the current value

-- COMMAND ----------

-- MAGIC %fs rm -r dbfs:/user/hive/warehouse/customer_type3

-- COMMAND ----------

DROP TABLE IF EXISTS customer_type3;

CREATE TABLE customer_type3 (
  customer_id INT,
  name STRING,
  current_loc STRING,
  previous_loc STRING
);

-- COMMAND ----------

INSERT INTO customer_type3 (
	customer_id,
	name,
	current_loc,
	previous_loc
	)
SELECT 1,
	'Ravi',
	'Bengaluru',
	NULL

UNION ALL

SELECT 2,
	'Ram',
	'Chennai',
	NULL

UNION ALL

SELECT 3,
	'Prasad',
	'Hyderabad',
	NULL;

-- COMMAND ----------

-- MAGIC %fs rm -r dbfs:/user/hive/warehouse/customer_src3

-- COMMAND ----------

DROP TABLE IF EXISTS customer_src3;

CREATE TABLE customer_src3 (
  customer_id INT,
  name STRING,
  location STRING
);

-- COMMAND ----------

INSERT INTO
  customer_src3
VALUES
  (1, 'Ravi', 'Chennai'),
  (2, 'Ram', 'Hyderabad'),
  (4, 'Mahesh', 'Bengaluru'),
  (5, 'Sridhar', 'Hyderabad');

-- COMMAND ----------

SELECT
  *
FROM
  customer_type3;

-- COMMAND ----------

SELECT
  *
FROM
  customer_src3;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ###### Approach 01 - without joining with target table

-- COMMAND ----------

MERGE INTO customer_type3 tgt USING customer_src3 src ON tgt.customer_id = src.customer_id
WHEN MATCHED
AND lower(src.location) <> lower(tgt.current_loc) THEN
UPDATE
SET
  tgt.previous_loc = tgt.current_loc,
  tgt.current_loc = src.location
  WHEN NOT MATCHED THEN
INSERT
  (customer_id, name, current_loc)
VALUES(src.customer_id, src.name, src.location);

-- COMMAND ----------

SELECT
  *
FROM
  customer_type3;

-- COMMAND ----------

SELECT
  *
FROM
  customer_src3;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ###### Approach 02 - joining with target table get the comparison

-- COMMAND ----------

-- MAGIC %fs rm -r dbfs:/user/hive/warehouse/customer_type3

-- COMMAND ----------

DROP TABLE IF EXISTS customer_type3;

CREATE TABLE customer_type3 (
  customer_id INT,
  name STRING,
  current_loc STRING,
  previous_loc STRING
);

-- COMMAND ----------

INSERT INTO customer_type3 (
	customer_id,
	name,
	current_loc,
	previous_loc
	)
SELECT 1,
	'Ravi',
	'Bengaluru',
	NULL

UNION ALL

SELECT 2,
	'Ram',
	'Chennai',
	NULL

UNION ALL

SELECT 3,
	'Prasad',
	'Hyderabad',
	NULL;

-- COMMAND ----------

-- MAGIC %fs rm -r dbfs:/user/hive/warehouse/customer_src3

-- COMMAND ----------

DROP TABLE IF EXISTS customer_src3;

CREATE TABLE customer_src3 (
  customer_id INT,
  name STRING,
  location STRING
);

-- COMMAND ----------

INSERT INTO
  customer_src3
VALUES
  (1, 'Ravi', 'Chennai'),
  (2, 'Ram', 'Hyderabad'),
  (4, 'Mahesh', 'Bengaluru'),
  (5, 'Sridhar', 'Hyderabad');

-- COMMAND ----------

SELECT
  src.customer_id,
  src.name,
  src.location AS current_loc,
  CASE
    WHEN src.location <> tgt.current_loc THEN tgt.current_loc
    ELSE tgt.previous_loc
  END AS previous_loc
FROM
  customer_src3 src
  LEFT JOIN customer_type3 tgt on src.customer_id = tgt.customer_id;

-- COMMAND ----------

MERGE INTO customer_type3 tgt USING (
  SELECT
    src.customer_id,
    src.name,
    src.location AS current_loc,
    CASE
      WHEN src.location <> tgt.current_loc THEN tgt.current_loc
      ELSE tgt.previous_loc
    END AS previous_loc
  FROM
    customer_src3 src
    LEFT JOIN customer_type3 tgt on src.customer_id = tgt.customer_id
) src ON tgt.customer_id = src.customer_id
WHEN MATCHED THEN
UPDATE
SET
  tgt.customer_id = src.customer_id,
  tgt.name = src.name,
  tgt.current_loc = src.current_loc,
  tgt.previous_loc = src.previous_loc
  WHEN NOT MATCHED THEN
INSERT
  (customer_id, name, current_loc, previous_loc)
VALUES
  (
    src.customer_id,
    src.name,
    src.current_loc,
    src.previous_loc
  );

-- COMMAND ----------

SELECT
  *
FROM
  customer_type3;

-- COMMAND ----------

SELECT
  *
FROM
  customer_src3;

-- COMMAND ----------


