# Databricks notebook source
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

pit_stops_read = spark.read.option("multiline", True).schema(pit_stops_schema).json("/Volumes/workspace/tracks/formula/pit_stops.json")

display(pit_stops_read)


# COMMAND ----------

from pyspark.sql.functions import *
pit_stops_final=pit_stops_read.withColumn("Ingested_Date",current_timestamp()).withColumnRenamed("raceId","Race_ID").withColumnRenamed("driverId","Driver_ID").withColumnRenamed("stop","Stop").withColumnRenamed("lap","Lap").withColumnRenamed("time","Time").withColumnRenamed("duration","Duration").withColumnRenamed("milliseconds","Milliseconds")

display(pit_stops_final)

# COMMAND ----------

pit_stops_final.write.mode("overwrite").parquet("/Volumes/workspace/tracks/formula/parquet/Pit_Stops/Pit_Stops_parquet")