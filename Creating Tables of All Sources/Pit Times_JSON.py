# Databricks notebook source
# MAGIC %sql
# MAGIC USE DATABASE f1_raw

# COMMAND ----------

pit_stops_df=spark.read.option("header",True).option("inferSchema",True).option("multiline",True).json("/Volumes/workspace/tracks/formula/pit_stops.json")

# COMMAND ----------

pit_stops_df.write.mode("overwrite").saveAsTable("pit_stops_raw")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pit_stops_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED pit_stops_raw