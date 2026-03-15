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

qualifying_split_mid=spark.read.option("multiline",True).schema(qualifying_schema).json(f"{base_path}/qualifying_times")
display(qualifying_split_mid)
qualifying_split_mid.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *
qualifying_split_final=qualifying_split_mid.withColumn("Ingested_Date",current_timestamp()).withColumn("file_date",lit(v_file_date)).withColumnRenamed("qualifyId","Qualify_ID").withColumnRenamed("raceId","Race_ID").withColumnRenamed("driverId","Driver_ID").withColumnRenamed("constructorId","Constructor_ID").withColumnRenamed("number","Number").withColumnRenamed("position","Position").withColumnRenamed("q1","Q1").withColumnRenamed("q2","Q2").withColumnRenamed("q3","Q3")

display(qualifying_split_final)

# COMMAND ----------

qualifying_split_final.write.mode("append").saveAsTable("Incremental_QualifyingTimes")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM f1_incremental.Incremental_QualifyingTimes WHERE file_date="21-03-2021"

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES