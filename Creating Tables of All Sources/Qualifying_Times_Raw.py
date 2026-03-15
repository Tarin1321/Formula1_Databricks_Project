# Databricks notebook source
# MAGIC %sql
# MAGIC USE DATABASE f1_raw

# COMMAND ----------

qualifying_times_df=spark.read.option("multiline",True).option("header",True).option("inferSchema",True).json("/Volumes/workspace/tracks/formula/Qualifying_Times/")

# COMMAND ----------

qualifying_times_df.write.mode("overwrite").saveAsTable("qualifying_times_raw")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED qualifying_times_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM qualifying_times_raw