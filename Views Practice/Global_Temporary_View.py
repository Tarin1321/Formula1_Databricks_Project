# Databricks notebook source
race_results_df=spark.read.parquet("/Volumes/workspace/tracks/formula/Presentation_Layer/race_results")
display(race_results_df)

# COMMAND ----------

#Scope is other notebooks as well as this one 
race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gv_race_results
# MAGIC

# COMMAND ----------

