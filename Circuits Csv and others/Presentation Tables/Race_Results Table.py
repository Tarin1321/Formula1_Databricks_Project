# Databricks notebook source
# MAGIC %sql
# MAGIC USE f1_processed

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_presentation
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE f1_processed

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE f1_presentation.Race_Results_Processed
# MAGIC AS
# MAGIC SELECT d.Race_Year,
# MAGIC c.Name AS Constructors_Name,
# MAGIC b.Name AS Driver_Name,
# MAGIC a.Position,
# MAGIC a.Points,
# MAGIC 11-a.Position AS Calculated_Points,
# MAGIC e.Location AS Location
# MAGIC FROM results_processed AS a
# MAGIC JOIN drivers_processed AS b ON (a.Driver_ID=b.Driver_ID)
# MAGIC JOIN constructors_processed AS c ON (a.Constructor_ID=c.Constructor_ID)
# MAGIC JOIN races_processed AS d ON (a.Race_ID=d.Race_ID)
# MAGIC JOIN circuits_processed AS e ON (d.Circuit_ID=e.Circuit_ID)
# MAGIC WHERE a.Position<=10

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE f1_presentation

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.Race_Results_Processed