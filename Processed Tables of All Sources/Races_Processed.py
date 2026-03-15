# Databricks notebook source
# MAGIC %sql
# MAGIC USE DATABASE f1_processed

# COMMAND ----------

# MAGIC %run "/Workspace/Users/tarin.kathuria@cubastion.com/Formula 1 Project/Circuits Csv and others/Ingestion/Races Read"

# COMMAND ----------

races_final.write.mode("overwrite").partitionBy("Race_Year").saveAsTable("Races_Processed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Races_Processed