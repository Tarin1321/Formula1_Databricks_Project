# Databricks notebook source
# MAGIC %run "/Workspace/Users/tarin.kathuria@cubastion.com/Formula 1 Project/Circuits Csv and others/Ingestion/Races Read"

# COMMAND ----------

races_df_demo=(spark.read.parquet("/Volumes/workspace/tracks/formula/parquet/races"))

# COMMAND ----------

races_filtered=races_df_demo.filter("Race_Year=2019 AND round<=5")
display(races_filtered)