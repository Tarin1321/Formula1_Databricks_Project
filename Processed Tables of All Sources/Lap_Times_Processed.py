# Databricks notebook source
# MAGIC %sql
# MAGIC USE DATABASE f1_processed

# COMMAND ----------

# MAGIC %run "/Workspace/Users/tarin.kathuria@cubastion.com/Formula 1 Project/Circuits Csv and others/Ingestion/Lap Times Read"

# COMMAND ----------

lap_times_split_final.write.mode("overwrite").saveAsTable("Lap_Times_Processed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Lap_Times_Processed