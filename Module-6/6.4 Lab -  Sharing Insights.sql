-- Databricks notebook source
-- MAGIC 
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Lab 3 - Sharing Insights
-- MAGIC ## Module 6 Assignment
-- MAGIC 
-- MAGIC In this lab, we will explore a small mock data set from a group of data centers. You'll see that is is similar to the data you have been working with, but it contains a few new columns and it is structured slightly differently to test your skills with hierarchical data manipulation. 
-- MAGIC 
-- MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this assignment you will: </br>
-- MAGIC 
-- MAGIC * Apply higher-order functions to array data
-- MAGIC * Apply advanced aggregation and summary techniques to process data
-- MAGIC * Present data in an interactive dashboard or static file 
-- MAGIC 
-- MAGIC As you work through the following tasks, you will be prompted to enter selected answers in Coursera. Find the quiz associated with this lab to enter your answers. 
-- MAGIC 
-- MAGIC Run the cell below to prepare this workspace for the lab. 

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 1: Create a table
-- MAGIC 
-- MAGIC **Summary:** Create a table. 
-- MAGIC 
-- MAGIC Use this path to access the data: `/mnt/training/iot-devices/data-centers/energy.json`
-- MAGIC 
-- MAGIC Steps to complete: 
-- MAGIC * Write a `CREATE TABLE` statement for the data located at the endpoint listed above
-- MAGIC * Use json as the file format

-- COMMAND ----------

--TODO
CREATE TABLE IF NOT EXISTS EnergyTable
  USING json
  OPTIONS (
    path "/mnt/training/iot-devices/data-centers/energy.json",
    inferSchema "true"
  )


-- COMMAND ----------

describe EnergyTable;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 2: Sample the table
-- MAGIC 
-- MAGIC **Summary:** Sample the table to get a closer look at a few rows
-- MAGIC 
-- MAGIC Steps to complete: 
-- MAGIC * Write a query that allows you to see a few rows of the data

-- COMMAND ----------

--TODO
Select *from EnergyTable TableSample(5 rows);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 3: Create view
-- MAGIC 
-- MAGIC **Summary:** Create a temporary view that displays the timestamp column as a timestamp. 
-- MAGIC 
-- MAGIC Steps to complete: 
-- MAGIC * Create a temporary view named `DCDevices`
-- MAGIC * Convert the `timestamp` column to a timestamp type. Refer to the [Datetime patterns](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html#) documentation for the formatting information. 
-- MAGIC * (Optional) Rename columns to use camelCase

-- COMMAND ----------

--TODO
CREATE OR REPLACE TEMPORARY VIEW DCDevices
AS
  SELECT
    battery_level as Battery_level,
    co2_level as Co2_Level,
    device_id as Device_Id,
    device_type as Device_Type,
    signal as Signal,
    temps as Temperature,
    to_timestamp(timestamp, 'yyyy/MM/dd HH:mm:ss')as Time 
  FROM EnergyTable;  
  


-- COMMAND ----------

Select *from DcDevices tablesample(5 rows); 

-- COMMAND ----------

describe EnergyTable;

-- COMMAND ----------





-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 4: Flag records with defective batteries
-- MAGIC 
-- MAGIC **Summary:** When a battery is malfunctioning, it can report negative battery levels. Create a new boolean column `needService` that shows whether a device needs service.  
-- MAGIC 
-- MAGIC Steps to complete: 
-- MAGIC * Write a query that shows which devices have malfunctioning batteries
-- MAGIC * Include columns `batteryLevel`, `deviceId`, and `needService`
-- MAGIC * Order the results by `deviceId`, and then `batteryLevel`
-- MAGIC * **Answer the corresponding question in Coursera**

-- COMMAND ----------

--TODO Flag Records with defective batteries 

SELECT
  battery_level as batteryLevel,
  device_id as deviceId,
  EXISTS (battery_level, b -> b <0) As needService
FROM EnergyTable order by deviceId , batteryLevel asc  limit 10;



-- COMMAND ----------

describe EnergyTable;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### Exercise 5: Display high CO<sub>2</sub> levels
-- MAGIC 
-- MAGIC **Summary:** Create a new column to display only CO<sub>2</sub> levels that exceed 1400 ppm. 
-- MAGIC 
-- MAGIC Steps to complete: 
-- MAGIC * Include columns `deviceId`, `deviceType`, `highCO2`, `time`
-- MAGIC * The column `highCO2` should contain an array of CO<sub>2</sub> readings over 1400
-- MAGIC * Show only records that contain `highCO2` values
-- MAGIC * Order by `deviceId`, and then `highCO2`
-- MAGIC 
-- MAGIC **Answer the corresponding question in Coursera**
-- MAGIC 
-- MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> You may need to use a subquery to write this in a single query statement. 

-- COMMAND ----------

--TODO

SELECT
      device_id as deviceId, device_type as deviceType,
      co2_level as c02level,
      to_timestamp(timestamp, 'yyyy/MM/dd HH:mm:ss')as Time,
      FILTER(co2_level, co -> co > 1400) AS highCO2
    FROM EnergyTable order by  highCO2 desc, co2_level;
  


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 6: Create a partitioned table
-- MAGIC 
-- MAGIC **Summary:** Create a new table partitioned by `deviceId`
-- MAGIC 
-- MAGIC Steps to complete: 
-- MAGIC * Include all columns
-- MAGIC * Create the table using Parquet
-- MAGIC * Rename the partitioned column `p_deviceId`
-- MAGIC * Run a `SELECT *`  to view your table. 
-- MAGIC 
-- MAGIC **Answer the corresponding question in Coursera**

-- COMMAND ----------

--TODO create a partitioned table
DROP TABLE IF EXISTS Energy_Partition_table;
CREATE TABLE Energy_Partition_table 
PARTITIONED BY (p_deviceId)

AS 
SELECT  
device_type,
device_id AS p_deviceId, 
battery_level,
co2_level,
signal,
temps,
to_timestamp(timestamp, 'yyyy/MM/dd HH:mm:ss')as Time

FROM EnergyTable;



-- COMMAND ----------

select * from Energy_partition_table;

-- COMMAND ----------

show partitions Energy_Partition_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 7: Visualize average temperatures 

-- COMMAND ----------

--TODO visualize average temperatures 
select 
device_Type,
to_timestamp(timestamp, 'yyyy/MM/dd HH:mm:ss')as Time,
REDUCE(temps, BIGINT(0), (t, acc) -> t + acc, acc ->(acc div size(temps))) as Avg_Daily_Temp
from EnergyTable


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 8: Create a widget

-- COMMAND ----------

--TODO

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exercise 9: Use the widget in a query

-- COMMAND ----------

--TODO


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>