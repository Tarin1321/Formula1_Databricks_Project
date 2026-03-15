# Databricks notebook source
race_results_df=spark.read.parquet("/Volumes/workspace/tracks/formula/Presentation_Layer/race_results")
display(race_results_df)

# COMMAND ----------

#Scope is only in this notebook
race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1)
# MAGIC FROM v_race_results
# MAGIC WHERE Race_Year=2020

# COMMAND ----------

race_results_2019=spark.sql("SELECT * FROM v_race_results WHERE Race_Year=2019")
display(race_results_2019)