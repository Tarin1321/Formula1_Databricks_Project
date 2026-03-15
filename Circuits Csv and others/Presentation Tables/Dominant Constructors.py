# Databricks notebook source
# MAGIC %sql
# MAGIC USE f1_presentation

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Constructors_Name,
# MAGIC COUNT(1) AS Total_Races,
# MAGIC SUM(Calculated_Points) AS Total_Points,
# MAGIC (Total_Points/Total_Races) AS Dominance
# MAGIC FROM Race_Results_Processed
# MAGIC GROUP BY Constructors_Name
# MAGIC HAVING Total_Races>100
# MAGIC ORDER BY Dominance DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Constructors_Name,
# MAGIC COUNT(1) AS Total_Races,
# MAGIC SUM(Calculated_Points) AS Total_Points,
# MAGIC (Total_Points/Total_Races) AS Dominance
# MAGIC FROM Race_Results_Processed
# MAGIC WHERE Race_Year BETWEEN 2000 AND 2011
# MAGIC GROUP BY Constructors_Name
# MAGIC HAVING Total_Races>100
# MAGIC ORDER BY Dominance DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Constructors_Name,
# MAGIC COUNT(1) AS Total_Races,
# MAGIC SUM(Calculated_Points) AS Total_Points,
# MAGIC (Total_Points/Total_Races) AS Dominance
# MAGIC FROM Race_Results_Processed
# MAGIC WHERE Race_Year BETWEEN 2011 AND 2020
# MAGIC GROUP BY Constructors_Name
# MAGIC HAVING Total_Races>100
# MAGIC ORDER BY Dominance DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW dominant_constructors
# MAGIC AS
# MAGIC
# MAGIC   SELECT Constructors_Name,
# MAGIC COUNT(1) AS Total_Races,
# MAGIC SUM(Calculated_Points) AS Total_Points,
# MAGIC AVG(Calculated_Points) AS Dominance,
# MAGIC RANK() OVER(ORDER BY AVG(Calculated_Points)DESC) Constructors_Rank
# MAGIC FROM Race_Results_Processed
# MAGIC GROUP BY Constructors_Name
# MAGIC HAVING COUNT(1)>=50
# MAGIC ORDER BY Dominance DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from f1_presentation.Race_Results_Processed

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dominant_constructors

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Race_Year,
# MAGIC Constructors_Name,
# MAGIC COUNT(1) AS Total_Races,
# MAGIC SUM(Calculated_Points) AS Total_Points,
# MAGIC AVG(Calculated_Points) AS Dominance
# MAGIC FROM Race_Results_Processed
# MAGIC WHERE Constructors_Name IN (SELECT Constructors_Name FROM dominant_constructors WHERE Constructors_Rank<=10)
# MAGIC GROUP BY Race_Year,Constructors_Name
# MAGIC ORDER BY Race_Year,Dominance DESC 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Race_Year,
# MAGIC Constructors_Name,
# MAGIC COUNT(1) AS Total_Races,
# MAGIC SUM(Calculated_Points) AS Total_Points,
# MAGIC AVG(Calculated_Points) AS Dominance
# MAGIC FROM Race_Results_Processed
# MAGIC WHERE Constructors_Name IN (SELECT Constructors_Name FROM dominant_constructors WHERE Constructors_Rank<=10)
# MAGIC GROUP BY Race_Year,Constructors_Name
# MAGIC ORDER BY Race_Year,Dominance DESC 