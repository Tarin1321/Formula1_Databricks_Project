# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_INCREMENTAL

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE f1_INCREMENTAL

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

raw_folder_path = "/Volumes/workspace/tracks/formula"
base_path = f"{raw_folder_path}/{v_file_date}"

# COMMAND ----------

v_file_date

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

circuits_schema=StructType(fields=[StructField("circuitId",IntegerType(),False),
                                   StructField("circuitRef",StringType(),True),
                                   StructField("name",StringType(),True),
                                   StructField("location",StringType(),True),
                                   StructField("country",StringType(),True),
                                   StructField("lat",DoubleType(),True),
                                   StructField("lng",DoubleType(),True),
                                   StructField("alt",IntegerType(),True),
                                   StructField("url",StringType(),True)
])



# COMMAND ----------

circuits_read=spark.read.option("header",True).schema(circuits_schema).csv(f"{base_path}/circuits.csv")


# COMMAND ----------

circuits_read.count()

# COMMAND ----------

circuits_selected = circuits_read.select(
    col("circuitId").alias("Circuit_ID"),
    col("circuitRef").alias("Circuit_Ref"),
    col("name").alias("Name"),
    col("location").alias("Location"),
    col("country").alias("Country"),
    col("lat").alias("Latitude"),
    col("lng").alias("Longitude"),
    col("alt").alias("Altitude")
)

display(circuits_selected)



# COMMAND ----------

circuits_final=circuits_selected.withColumn("CreatedDate",current_timestamp())
display(circuits_final)
circuits_final.printSchema()

# COMMAND ----------

circuits_final.write.mode("overwrite").saveAsTable("Full_Circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_INCREMENTAL.Full_Circuits