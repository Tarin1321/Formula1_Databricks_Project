# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df=spark.read.option("header",True).option("inferSchema",True).csv("/Volumes/workspace/tracks/formula/circuits.csv")

# COMMAND ----------

df.printSchema()


# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.fs.ls("/")


# COMMAND ----------

df.write.format("delta").saveAsTable("circuits")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT name,country  FROM circuits order by country DESC

# COMMAND ----------

constructors_json=spark.read.json("/Volumes/workspace/tracks/formula/1/raw/constructors.json")

drivers_json=spark.read.json("/Volumes/workspace/tracks/formula/1/raw/drivers.json")

lap_times_split_1=spark.read.csv("/Volumes/workspace/tracks/formula/lap_times_split_1.csv")

lap_times_split_2=spark.read.csv("/Volumes/workspace/tracks/formula/lap_times_split_2.csv")

lap_times_split_3=spark.read.csv("/Volumes/workspace/tracks/formula/lap_times_split_3.csv")

lap_times_split_4=spark.read.csv("/Volumes/workspace/tracks/formula/lap_times_split_4.csv")

lap_times_split_5=spark.read.csv("/Volumes/workspace/tracks/formula/lap_times_split_5.csv")

pit_stops=spark.read.json("/Volumes/workspace/tracks/formula/1/raw/pit_stops.json")

qualifying_split_1=spark.read.json("/Volumes/workspace/tracks/formula/qualifying_split_1.json")

qualifying_split_2=spark.read.json("/Volumes/workspace/tracks/formula/qualifying_split_2.json")

races_csv=spark.read.csv("/Volumes/workspace/tracks/formula/races.csv")

results_json=spark.read.json("/Volumes/workspace/tracks/formula/1/raw/results.json")




# COMMAND ----------

