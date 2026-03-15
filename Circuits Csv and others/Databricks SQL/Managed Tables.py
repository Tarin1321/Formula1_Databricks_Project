# Databricks notebook source
race_results_df=spark.read.parquet("/Volumes/workspace/tracks/formula/Presentation_Layer/race_results")

# COMMAND ----------

#Managed table using python 
#Managed table is handled by spark both metadata and data
race_results_df.write.mode("overwrite").saveAsTable("managed_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED managed_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_database();

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM race_results_sql LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.managed_table
# MAGIC WHERE Race_Year=2020;

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE race_results_sql AS
# MAGIC SELECT *
# MAGIC FROM default.managed_table
# MAGIC WHERE Race_Year = 2020;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED race_results_sql;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.race_results_sql

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS raw;