# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_raw;
# MAGIC USE DATABASE f1_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE f1_raw

# COMMAND ----------

circuits_df=spark.read.option("header",True).option("inferSchema",True).csv("/Volumes/workspace/tracks/formula/circuits.csv")

# COMMAND ----------

circuits_df.write.mode("overwrite").saveAsTable("Circuits_Raw_Managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM circuits_raw_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW Tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED circuits_raw_managed; 

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES

# COMMAND ----------

races_df=spark.read.option("header",True).option("inferSchema",True).csv("/Volumes/workspace/tracks/formula/races.csv")

# COMMAND ----------

races_df.write.mode("overwrite").saveAsTable("Races_Raw_Managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED Races_Raw_Managed

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Races_Raw_Managed

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW Tables;