# Databricks notebook source
# MAGIC %sql
# MAGIC USE DATABASE f1_processed

# COMMAND ----------

# MAGIC %run "/Workspace/Users/tarin.kathuria@cubastion.com/Formula 1 Project/Circuits Csv and others/Ingestion/Qualifying Time JSON Split"

# COMMAND ----------

qualifying_split_final.write.mode("overwrite").saveAsTable("Qualifying_Times_Processed")

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Qualifying_Times_Processed