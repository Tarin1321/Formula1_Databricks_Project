# Databricks notebook source
# MAGIC %run "/Workspace/Users/tarin.kathuria@cubastion.com/Formula 1 Project/Circuits Csv and others/Merge Logic/Common Functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date","")

# COMMAND ----------

v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date 

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

results_schema=StructType(fields=[StructField("resultId",IntegerType(),False),
                                  StructField("raceId",IntegerType(),True),
                                  StructField("driverId",IntegerType(),True),
                                  StructField("constructorId",IntegerType(),True),
                                  StructField("number",IntegerType(),True),
                                  StructField("grid",IntegerType(),True),
                                  StructField("position",IntegerType(),True),
                                  StructField("positionText",StringType(),True),
                                  StructField("positionOrder",IntegerType(),True),
                                  StructField("points",FloatType(),True),
                                  StructField("laps",IntegerType(),True),
                                  StructField("time",StringType(),True),
                                  StructField("milliseconds",IntegerType(),True),
                                  StructField("fastestLap",IntegerType(),True),
                                  StructField("rank",IntegerType(),True),
                                  StructField("fastestLapTime",StringType(),True),
                                  StructField("fastestLapSpeed",StringType(),True),
                                  StructField("statusID",IntegerType(),True)])

# COMMAND ----------

raw_folder_path = "/Volumes/workspace/tracks/formula"
base_path = f"{raw_folder_path}/{v_file_date}"
base_path1=f"{raw_folder_path}"

# COMMAND ----------

results_read=spark.read.option("header",True).schema(results_schema).json(f"{base_path}/results.json")
display(results_read)
results_read.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM f1_incremental.incremental_results

# COMMAND ----------

from pyspark.sql.functions import *
results_mid=results_read.drop("statusId")
results_final=results_mid.withColumn("Ingested_Date",current_timestamp()).withColumn("file_date",lit(v_file_date)).withColumnRenamed("resultId","Result_Id").withColumnRenamed("raceId","Race_Id").withColumnRenamed("driverId","Driver_Id").withColumnRenamed("constructorId","Constructor_Id").withColumnRenamed("number","Number").withColumnRenamed("grid","Grid").withColumnRenamed("position","Position").withColumnRenamed("positionText","Position_Text").withColumnRenamed("positionOrder","Position_Order").withColumnRenamed("points","Points").withColumnRenamed("laps","Laps").withColumnRenamed("time","Time").withColumnRenamed("milliseconds","Milliseconds").withColumnRenamed("fastestLap","Fastest_Lap").withColumnRenamed("rank","Rank").withColumnRenamed("fastestLapTime","Fastest_Lap_Time").withColumnRenamed("fastestLapSpeed","Fastest_Lap_Speed")
display(results_final)

# COMMAND ----------

merge_logic(results_final,"f1_incremental.incremental_results","a.Driver_Id=b.Driver_Id AND a.Result_Id=b.Result_Id")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_incremental.incremental_results

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE f1_incremental

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM f1_incremental.incremental_results