# Databricks notebook source
# MAGIC %sql
# MAGIC USE f1_presentation

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Driver_Name,
# MAGIC COUNT(1) AS Total_Races,
# MAGIC SUM(Calculated_Points) AS Total_Points,
# MAGIC (Total_Points/Total_Races) AS Dominance
# MAGIC FROM Race_Results_Processed
# MAGIC GROUP BY Driver_Name
# MAGIC HAVING Total_Races>50
# MAGIC ORDER BY Dominance DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Driver_Name,
# MAGIC COUNT(1) AS Total_Races,
# MAGIC SUM(Calculated_Points) AS Total_Points,
# MAGIC (Total_Points/Total_Races) AS Dominance
# MAGIC FROM Race_Results_Processed
# MAGIC WHERE Race_Year BETWEEN 2001 AND 2010
# MAGIC GROUP BY Driver_Name
# MAGIC HAVING Total_Races>50
# MAGIC ORDER BY Dominance DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Driver_Name,
# MAGIC COUNT(1) AS Total_Races,
# MAGIC SUM(Calculated_Points) AS Total_Points,
# MAGIC (Total_Points/Total_Races) AS Dominance
# MAGIC FROM Race_Results_Processed
# MAGIC WHERE Race_Year BETWEEN 2011 AND 2020
# MAGIC GROUP BY Driver_Name
# MAGIC HAVING Total_Races>50
# MAGIC ORDER BY Dominance DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW dominant_drivers 
# MAGIC AS
# MAGIC SELECT *,
# MAGIC RANK() OVER(ORDER BY Dominance DESC) AS Driver_Rank
# MAGIC FROM
# MAGIC (
# MAGIC   SELECT Driver_Name,
# MAGIC COUNT(1) AS Total_Races,
# MAGIC SUM(Calculated_Points) AS Total_Points,
# MAGIC (Total_Points/Total_Races) AS Dominance
# MAGIC FROM Race_Results_Processed
# MAGIC GROUP BY Driver_Name
# MAGIC HAVING Total_Races>50
# MAGIC )t
# MAGIC ORDER BY Dominance DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE f1_presentation.dominant_drivers 
# MAGIC AS
# MAGIC SELECT *,
# MAGIC RANK() OVER(ORDER BY Dominance DESC) AS Driver_Rank
# MAGIC FROM
# MAGIC (
# MAGIC   SELECT Driver_Name,
# MAGIC COUNT(1) AS Total_Races,
# MAGIC SUM(Calculated_Points) AS Total_Points,
# MAGIC (Total_Points/Total_Races) AS Dominance
# MAGIC FROM f1_presentation.Race_Results_Processed
# MAGIC GROUP BY Driver_Name
# MAGIC HAVING Total_Races>50
# MAGIC )t

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.dominant_drivers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Race_Year,
# MAGIC Driver_Name,
# MAGIC COUNT(1) AS Total_Races,
# MAGIC SUM(Calculated_Points) AS Total_Points,
# MAGIC (Total_Points/Total_Races) AS Dominance
# MAGIC FROM Race_Results_Processed
# MAGIC WHERE Driver_Name IN (SELECT Driver_Name FROM dominant_drivers WHERE Driver_Rank<=10)
# MAGIC GROUP BY Race_Year,Driver_Name
# MAGIC ORDER BY Race_Year
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Race_Year,
# MAGIC Driver_Name,
# MAGIC COUNT(1) AS Total_Races,
# MAGIC SUM(Calculated_Points) AS Total_Points,
# MAGIC (Total_Points/Total_Races) AS Dominance
# MAGIC FROM Race_Results_Processed
# MAGIC WHERE Driver_Name IN (SELECT Driver_Name FROM dominant_drivers WHERE Driver_Rank<=10)
# MAGIC GROUP BY Race_Year,Driver_Name
# MAGIC ORDER BY Race_Year