# Databricks notebook source
race_results_df=spark.read.parquet("/Volumes/workspace/tracks/formula/Presentation_Layer/")

# COMMAND ----------

demo_df=race_results_df.filter("race_year=2020")
display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import *
demo_df.select(count("*")).show()

# COMMAND ----------

demo_df.select(countDistinct("Race_Name")).show()

# COMMAND ----------

demo_df.filter("Driver_Name='Max Verstappen'").select(sum("Points")).show()

# COMMAND ----------

from pyspark.sql.functions import *
demo_df.groupBy("Driver_Name").agg(sum("Points").alias("Total_Points"),countDistinct("Race_Name").alias("Distinct_Races")).orderBy(sum("Points").desc()).show()

# COMMAND ----------

demo_df=race_results_df.filter("Race_Year in (2019,2020)")
display(demo_df)

# COMMAND ----------

demo_grouped_df=demo_df.groupBy("Race_Year","Driver_Name").agg(sum("Points").alias("Total_Points"),countDistinct("Race_Name").alias("Distinct_Races")).orderBy(desc("Total_Points"))
display(demo_grouped_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
driverRankSpec= Window.partitionBy("Race_year").orderBy(desc("Total_Points"))
demo_grouped_df.withColumn("Rank",rank().over(driverRankSpec)).show(100)
