# Databricks notebook source
race_results_df=spark.read.parquet("/Volumes/workspace/tracks/formula/Presentation_Layer/race_results")
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

constructors_final_standings.write.mode("overwrite").parquet("Volumes/workspace/tracks/formula/Presentation_Layer/constructors_standings")