# Databricks notebook source
# MAGIC %sql
# MAGIC USE f1_incremental

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

raw_folder_path = "/Volumes/workspace/tracks/formula"
base_path = f"{raw_folder_path}/{v_file_date}"

# COMMAND ----------

v_file_date

# COMMAND ----------

race_results_list=spark.read.table("Incremental_Race_Results").filter(f"Results_File_Date = '{v_file_date}'").select("Race_Year").distinct().collect()

# COMMAND ----------

race_results_list

# COMMAND ----------

race_year_list=[]
for race_year in race_results_list:
    race_year_list.append(race_year.Race_Year)

# COMMAND ----------

from pyspark.sql.functions import *
race_results_df=spark.read.table("Incremental_Race_Results").filter(col("Race_Year").isin(race_year_list))
display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import *
constructor_standings=race_results_df.groupBy("Team","Race_Year").agg(sum("Points").alias("Total_Points"),count(when(col("Position")==1,True)).alias("Wins")).orderBy(desc("Total_Points")).select("Team","Wins","Total_Points","Race_Year")
display(constructor_standings)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *
window_spec=Window.partitionBy("Race_Year").orderBy(desc("Total_Points"))
constructors_final_standings=constructor_standings.withColumn("Rank",dense_rank().over(window_spec)).filter("Race_Year=2008").select("Team","Wins","Total_Points","Rank")    
display(constructors_final_standings)

# COMMAND ----------

constructors_final_standings.write.mode("append").saveAsTable("Final_Constructor_Standings")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM final_constructor_standings

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Race_Name, Team, Points
# MAGIC FROM Incremental_Race_Results
# MAGIC WHERE Race_Year = 2021
# MAGIC AND results_file_date = '${p_file_date}'
# MAGIC ORDER BY Race_Name, Points DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL f1_incremental.final_constructor_standings

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_incremental. final_constructor_standings