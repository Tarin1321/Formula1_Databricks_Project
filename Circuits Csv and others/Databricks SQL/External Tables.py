# Databricks notebook source
race_results_df=spark.read.parquet("/Volumes/workspace/tracks/formula/Presentation_Layer/race_results")

# COMMAND ----------

race_results_df.write \
    .format("parquet") \
    .option("path", "dbfs:/Volumes/workspace/tracks/formula/external/race_results") \
    .saveAsTable("race_results_ext")