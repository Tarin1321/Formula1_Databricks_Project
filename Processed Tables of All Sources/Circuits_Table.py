# Databricks notebook source
# MAGIC %sql
# MAGIC SHOW DATABASES
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_processed

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE f1_processed
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %run "/Workspace/Users/tarin.kathuria@cubastion.com/Formula 1 Project/Circuits Csv and others/Ingestion/Circuits Read"

# COMMAND ----------

circuits_final.write.mode("overwrite").saveAsTable("Circuits_Processed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Circuits_Processed