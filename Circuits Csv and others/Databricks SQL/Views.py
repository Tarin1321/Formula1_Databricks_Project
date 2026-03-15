# Databricks notebook source
# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW TEMP_VIEW
# MAGIC AS
# MAGIC SELECT * FROM race_results_sql
# MAGIC WHERE Race_Year=2020;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM TEMP_VIEW;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT Race_Year
# MAGIC FROM race_results_sql
# MAGIC ORDER BY Race_Year;