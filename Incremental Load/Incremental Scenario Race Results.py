# Databricks notebook source
# MAGIC %sql
# MAGIC USE DATABASE f1_incremental

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

raw_folder_path = "/Volumes/workspace/tracks/formula"
base_path = f"{raw_folder_path}/{v_file_date}"

# COMMAND ----------

v_file_date

# COMMAND ----------

drivers_df=spark.read.table("f1_incremental.Full_Drivers").withColumnRenamed("Number","Driver_Number").withColumnRenamed("Name","Driver_Name").withColumnRenamed('Nationality',"Driver_Nationality")
drivers_df.printSchema()


# COMMAND ----------

constructors_df=spark.read.table("f1_incremental.Full_Constructors").withColumnRenamed("Name","Team")
constructors_df.printSchema()

# COMMAND ----------

circuits_df=spark.read.table("f1_incremental.Full_Circuits").withColumnRenamed("Location","Circuit_Location")

# COMMAND ----------

races_df=spark.read.table("f1_incremental.Full_Races").withColumnRenamed("Name",'Race_Name').withColumnRenamed("race_timestamp","Race_Date").withColumnRenamed("Race_year","Race_Year")
races_df.printSchema()

# COMMAND ----------

results_df=spark.read.table("f1_incremental.Incremental_Results").withColumnRenamed("Time","Race_Time").withColumnRenamed("Race_Id","Race_ID").withColumnRenamed("Driver_Id","Driver_ID").withColumnRenamed("Constructor_Id","Constructor_ID").withColumnRenamed("file_date","Results_File_Date")
#here we have applied incremental load on results that's why we have put in a filter on file_date of result because this is the only incremental file or delta file we are receiving others are just full load files.
results_inc_df = results_df.filter(f"Results_File_Date = '{v_file_date}'")
results_inc_df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_incremental.Incremental_Results where file_date = '21-03-2021'

# COMMAND ----------

#Join of circuits and races first
races_circuits_df = circuits_df.join(races_df,circuits_df.Circuit_ID == races_df.Circuit_ID,"inner").select(races_df.Race_ID,races_df.Race_Year,races_df.Race_Name,races_df.Race_Date,circuits_df.Circuit_Location)
races_circuits_df.printSchema()

# COMMAND ----------

#Join results to all other dataframes
race_results_df=results_inc_df.join(races_circuits_df,results_inc_df.Race_ID==races_circuits_df.Race_ID).join(drivers_df,results_inc_df.Driver_ID==drivers_df.Driver_ID).join(constructors_df,results_inc_df.Constructor_ID==constructors_df.Constructor_ID)

# COMMAND ----------

#from pyspark.sql.functions import *
#final_dff=race_results_df.select("Race_Year","Race_Name","Race_Date","Circuit_Location","Driver_Name","Driver_Number",#"Driver_Nationality","Team","Grid","Fastest_Lap","Race_Time","Points","Position").withColumn("Created_Date",current_timestamp()).#filter("Race_Year=2020 AND Race_Name='Abu Dhabi Grand Prix'").orderBy(col("Points").desc())
#display(final_dff)

# COMMAND ----------

final_dff=race_results_df.select("Race_Year","Race_Name","Race_Date","Circuit_Location","Driver_Name","Driver_Number","Driver_Nationality","Team","Grid","Fastest_Lap","Race_Time","Points","Position","Results_File_Date").withColumn("Created_Date",current_timestamp())

# COMMAND ----------

final_dff.write.mode("append").saveAsTable("Incremental_Race_Results")

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE IF EXISTS incremental_race_results

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT results_file_date, COUNT(*) 
# MAGIC FROM f1_incremental.Incremental_Race_Results
# MAGIC GROUP BY results_file_date

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) from incremental_race_results

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Incremental_Race_Results

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Race_Year, COUNT(*)
# MAGIC FROM incremental_race_results
# MAGIC GROUP BY Race_Year
# MAGIC ORDER BY Race_Year