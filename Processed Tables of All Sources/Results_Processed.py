# Databricks notebook source
# MAGIC %sql
# MAGIC USE DATABASE f1_processed

# COMMAND ----------

# MAGIC %run "/Workspace/Users/tarin.kathuria@cubastion.com/Formula 1 Project/Circuits Csv and others/Ingestion/Results JSON"

# COMMAND ----------

results_final.write.mode("overwrite").saveAsTable("Results_Processed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Results_Processed