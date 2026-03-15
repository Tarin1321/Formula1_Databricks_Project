# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

qualifying_schema=StructType(fields=[StructField("qualifyId",IntegerType(),True),
                                     StructField("raceId",IntegerType(),True),
                                     StructField("driverId",IntegerType(),True),
                                     StructField("constructorId",IntegerType(),True),
                                     StructField("number",IntegerType(),True),
                                     StructField("position",IntegerType(),True),
                                     StructField("q1",StringType(),True),
                                     StructField("q2",StringType(),True),
                                     StructField("q3",StringType(),True)])

# COMMAND ----------

qualifying_split_mid=spark.read.option("multiline",True).schema(qualifying_schema).json("/Volumes/workspace/tracks/formula/Qualifying_Times")
display(qualifying_split_mid)
qualifying_split_mid.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *
qualifying_split_final=qualifying_split_mid.withColumn("Ingested_Date",current_timestamp()).withColumnRenamed("qualifyId","Qualify_ID").withColumnRenamed("raceId","Race_ID").withColumnRenamed("driverId","Driver_ID").withColumnRenamed("constructorId","Constructor_ID").withColumnRenamed("number","Number").withColumnRenamed("position","Position").withColumnRenamed("q1","Q1").withColumnRenamed("q2","Q2").withColumnRenamed("q3","Q3")

display(qualifying_split_final)

# COMMAND ----------

qualifying_split_final.write.mode("overwrite").parquet("/Volumes/workspace/tracks/formula/parquet/qualifying_times/qualifying_times_parquet")