# Databricks notebook source
# MAGIC %sql
# MAGIC USE DATABASE f1_raw

# COMMAND ----------

Lap_times_df=spark.read.option("header",True).option("inferSchema",True).csv("/Volumes/workspace/tracks/formula/lap_times_csv")

# COMMAND ----------

display(Lap_times_df)

# COMMAND ----------

Lap_times_df.write.mode("overwrite").saveAsTable("Lap_Times_Raw")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED Lap_Times_Raw

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES