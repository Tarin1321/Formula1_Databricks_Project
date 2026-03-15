# Databricks notebook source
# MAGIC %run "/Workspace/Users/tarin.kathuria@cubastion.com/Formula 1 Project/Circuits Csv and others/Merge Logic/Common Functions"

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE f1_incremental

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

raw_folder_path = "/Volumes/workspace/tracks/formula"
base_path = f"{raw_folder_path}/{v_file_date}"

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

pit_stops_schema=StructType(fields=[StructField("raceId",IntegerType(),False),
                                    StructField("driverId",IntegerType(),True),
                                    StructField("stop",IntegerType(),True),
                                    StructField("lap",IntegerType(),True),
                                    StructField("time",StringType(),True),
                                    StructField("duration",StringType(),True),
                                    StructField("milliseconds",IntegerType(),True)])

# COMMAND ----------

pit_stops_read = spark.read.option("multiline", True).schema(pit_stops_schema).json(f"{base_path}/pit_stops.json")

display(pit_stops_read)


# COMMAND ----------

from pyspark.sql.functions import *
pit_stops_final=pit_stops_read.withColumn("Ingested_Date",current_timestamp()).withColumnRenamed("raceId","Race_ID").withColumnRenamed("driverId","Driver_ID").withColumnRenamed("stop","Stop").withColumnRenamed("lap","Lap").withColumnRenamed("time","Time").withColumn("file_date",lit(v_file_date)).withColumnRenamed("duration","Duration").withColumnRenamed("milliseconds","Milliseconds")

display(pit_stops_final)

# COMMAND ----------

merge_logic(pit_stops_final,"f1_incremental.Incremental_Pit_Stops","a.Race_ID=b.Race_ID AND a.Driver_ID=b.Driver_ID")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM incremental_pit_stops

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM Incremental_Pit_Stops 

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES