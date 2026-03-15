# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

lap_times_schema=StructType(fields=[StructField("raceId",IntegerType(), False),
                                    StructField("driverId",IntegerType(),True),
                                    StructField("lap",IntegerType(),True),
                                    StructField("position",IntegerType(),True),
                                    StructField("time",StringType(),True),
                                    StructField("milliseconds",IntegerType(),True)])

# COMMAND ----------


lap_times_split=spark.read.option("header","true").schema(lap_times_schema).csv("/Volumes/workspace/tracks/formula/lap_times_csv")
display(lap_times_split)

# COMMAND ----------

from pyspark.sql.functions import *
lap_times_split_final=lap_times_split.withColumn("Ingested_Date",current_timestamp()).withColumnRenamed("raceId","Race_ID").withColumnRenamed("driverId","Driver_ID").withColumnRenamed("lap","Lap").withColumnRenamed("position","Position").withColumnRenamed("time","Lap_Time").withColumnRenamed("milliseconds","Milli_Seconds")
display(lap_times_split_final)

# COMMAND ----------

lap_times_split_final.count()

# COMMAND ----------

lap_times_split_final.write.mode("overwrite").parquet("/Volumes/workspace/tracks/formula//parquet/Lap_Times/lap_times_parquet")