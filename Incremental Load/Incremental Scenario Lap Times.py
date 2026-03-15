# Databricks notebook source
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

lap_times_schema=StructType(fields=[StructField("raceId",IntegerType(), False),
                                    StructField("driverId",IntegerType(),True),
                                    StructField("lap",IntegerType(),True),
                                    StructField("position",IntegerType(),True),
                                    StructField("time",StringType(),True),
                                    StructField("milliseconds",IntegerType(),True)])

# COMMAND ----------


lap_times_split=spark.read.option("header","true").schema(lap_times_schema).csv(f"{base_path}/lap_times")
display(lap_times_split)

# COMMAND ----------

from pyspark.sql.functions import *
lap_times_split_final=lap_times_split.withColumn("Ingested_Date",current_timestamp()).withColumnRenamed("raceId","Race_ID").withColumnRenamed("driverId","Driver_ID").withColumnRenamed("lap","Lap").withColumnRenamed("position","Position").withColumnRenamed("time","Lap_Time").withColumn("file_date",lit(v_file_date)).withColumnRenamed("milliseconds","Milli_Seconds")
display(lap_times_split_final)

# COMMAND ----------

lap_times_split_final.write.mode("append").saveAsTable("Incremental_LapTimes")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM Incremental_LapTimes

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_incremental.Incremental_LapTimes WHERE file_date="28-03-2021"

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES