# Databricks notebook source
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

circuits_read=spark.read.option("header",True).schema(circuits_schema).csv("/Volumes/workspace/tracks/formula/circuits.csv")


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

circuits_final.write.mode("overwrite").parquet("/Volumes/workspace/tracks/formula/parquet/circuits/circuit_parquet")