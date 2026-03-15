# Databricks notebook source
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

results_read=spark.read.option("header",True).schema(results_schema).json("/Volumes/workspace/tracks/formula/results.json")
display(results_read)
results_read.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *
results_mid=results_read.drop("statusId")
results_final=results_mid.withColumn("Ingested_Date",current_timestamp()).withColumnRenamed("resultId","Result_Id").withColumnRenamed("raceId","Race_Id").withColumnRenamed("driverId","Driver_Id").withColumnRenamed("constructorId","Constructor_Id").withColumnRenamed("number","Number").withColumnRenamed("grid","Grid").withColumnRenamed("position","Position").withColumnRenamed("positionText","Position_Text").withColumnRenamed("positionOrder","Position_Order").withColumnRenamed("points","Points").withColumnRenamed("laps","Laps").withColumnRenamed("time","Time").withColumnRenamed("milliseconds","Milliseconds").withColumnRenamed("fastestLap","Fastest_Lap").withColumnRenamed("rank","Rank").withColumnRenamed("fastestLapTime","Fastest_Lap_Time").withColumnRenamed("fastestLapSpeed","Fastest_Lap_Speed")
display(results_final)


# COMMAND ----------

results_final.write.mode("overwrite").partitionBy("Race_Id").parquet("/Volumes/workspace/tracks/formula/parquet/Results/results_parquet")