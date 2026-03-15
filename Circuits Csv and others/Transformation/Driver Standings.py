# Databricks notebook source
race_results_df=spark.read.parquet("/Volumes/workspace/tracks/formula/Presentation_Layer/race_results")
display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import *
racers_standings=race_results_df.filter("Race_Year=2020") \
.groupBy("Race_Year","Driver_Name","Driver_Nationality","Team") \
.agg(
    (sum)("Points").alias("Total_Points"),count(when(col("Position")==1,True)).alias("Wins"),count(when(col("Position")==2,True)).alias("P2_Count"),count(when(col("Position")==3,True)).alias("P3_Count"),
    count(when(col("Position")==4,True)).alias("P4_Count"),count(when(col("Position")==5,True)).alias("P5_Count"),count(when(col("Position")==6,True)).alias("P6_Count"),count(when(col("Position")==7,True)).alias("P7_Count"),count(when(col("Position")==8,True)).alias("P8_Count"),count(when(col("Position")==9,True)).alias("P9_Count"),count(when(col("Position")==10,True)).alias("P10_Count"),count(when(col("Position")==11,True)).alias("P11_Count"),count(when(col("Position")==12,True)).alias("P12_Count"),count(when(col("Position")==13,True)).alias("P13_Count"),count(when(col("Position")==14,True)).alias("P14_Count"),count(when(col("Position")==15,True)).alias("P15_Count"),count(when(col("Position")==16,True)).alias("P16_Count"),count(when(col("Position")==17,True)).alias("P17_Count"),count(when(col("Position")==18,True)).alias("P18_Count"),count(when(col("Position")==19,True)).alias("P19_Count")
    ) \
.orderBy(desc("Total_Points"))
display(racers_standings)

# COMMAND ----------

from pyspark.sql.window import Window
window_spec=Window.partitionBy("Race_Year").orderBy(desc("Total_Points"),desc("Wins"),desc("P2_Count"),desc("P3_Count"),desc("P4_Count"),desc("P5_Count"),desc("P6_Count"),desc("P7_Count"),desc("P8_Count"),desc("P9_Count"),desc("P10_Count"),desc("P11_Count"),desc("P12_Count"),desc("P13_Count"),desc("P14_Count"),desc("P15_Count"),desc("P16_Count"),desc("P17_Count"),desc("P18_Count"),desc("P19_Count"))
final_driver_standings=racers_standings.withColumn("Rank",dense_rank().over(window_spec).alias("Rank")).select("Race_Year","Driver_Name","Driver_Nationality","Team","Total_Points","Wins","Rank")
display(final_driver_standings)

# COMMAND ----------

final_driver_standings.write.mode("overwrite").parquet("/Volumes/workspace/tracks/formula/Presentation_Layer/driver_standings")