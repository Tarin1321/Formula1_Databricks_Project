# Databricks notebook source
constructors_schema="constructorId INTEGER,constructorRef STRING , name STRING,nationality STRING,url STRING"

# COMMAND ----------

constructors_json_read=spark.read.option("header",True).schema(constructors_schema).json("/Volumes/workspace/tracks/formula/constructors.json")

# COMMAND ----------

from pyspark.sql.functions import *
constructors_med=constructors_json_read.drop(col("url"))
display(constructors_med)

# COMMAND ----------

constructors_pre_final=constructors_med.withColumn("Ingested_date",current_timestamp())
display(constructors_pre_final)
constructors_pre_final.printSchema()

# COMMAND ----------

constructors_final=constructors_pre_final.withColumnRenamed("ConstructorId","Constructor_ID").withColumnRenamed("constructorRef","Constructors_Ref").withColumnRenamed("name","Name").withColumnRenamed("nationality","Nationality")
display(constructors_final)

# COMMAND ----------

constructors_final.write.mode("overwrite").parquet("/Volumes/workspace/tracks/formula/parquet/constructors/constructors_parquet")
