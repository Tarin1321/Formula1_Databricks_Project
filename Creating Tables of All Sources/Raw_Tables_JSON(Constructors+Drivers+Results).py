# Databricks notebook source
# MAGIC %sql
# MAGIC USE DATABASE f1_raw

# COMMAND ----------

consturctors_df=spark.read.option("header",True).option("inferSchema",True).json("/Volumes/workspace/tracks/formula/constructors.json")

# COMMAND ----------

consturctors_df.write.mode("overwrite").saveAsTable("constructors_raw_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED constructors_raw_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM constructors_raw_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW Tables

# COMMAND ----------

drivers_df=spark.read.option("header",True).option("inferSchema",True).json("/Volumes/workspace/tracks/formula/drivers.json")

# COMMAND ----------

drivers_df.write.mode("overwrite").saveAsTable("Drivers_Raw_Managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Drivers_Raw_Managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

results_df=spark.read.option("header",True).option("inferSchema",True).json("/Volumes/workspace/tracks/formula/results.json")

# COMMAND ----------

results_df.write.mode("overwrite").saveAsTable("Results_Raw_Managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Results_Raw_Managed