# Databricks notebook source
#%run "/Workspace/Users/tarin.kathuria@cubastion.com/Formula 1 Project/Circuits Csv and others/Ingestion/Driver Single Line NESTED JSON"

# COMMAND ----------

#%run "/Workspace/Users/tarin.kathuria@cubastion.com/Formula 1 Project/Circuits Csv and others/Ingestion/Circuits Read"

# COMMAND ----------

#%run "/Workspace/Users/tarin.kathuria@cubastion.com/Formula 1 Project/Circuits Csv and others/Ingestion/Constructorsn JSON Read"

# COMMAND ----------

#%run "/Workspace/Users/tarin.kathuria@cubastion.com/Formula 1 Project/Circuits Csv and others/Ingestion/Races Read"

# COMMAND ----------

#%run "/Workspace/Users/tarin.kathuria@cubastion.com/Formula 1 Project/Circuits Csv and others/Ingestion/Circuits Read"

# COMMAND ----------

drivers_df=spark.read.parquet("/Volumes/workspace/tracks/formula/parquet/Drivers").withColumnRenamed("Number","Driver_Number").withColumnRenamed("Name","Driver_Name").withColumnRenamed('Nationality',"Driver_Nationality")
drivers_df.printSchema()


# COMMAND ----------

constructors_df=spark.read.parquet("/Volumes/workspace/tracks/formula/parquet/constructors").withColumnRenamed("Name","Team")
constructors_df.printSchema()

# COMMAND ----------

circuits_df=spark.read.parquet("/Volumes/workspace/tracks/formula/parquet/circuits").withColumnRenamed("Location","Circuit_Location")

# COMMAND ----------

races_df=spark.read.parquet("/Volumes/workspace/tracks/formula/parquet/races").withColumnRenamed("Name",'Race_Name').withColumnRenamed("race_timestamp","Race_Date").withColumnRenamed("Race_year","Race_Year")
races_df.printSchema()

# COMMAND ----------

results_df=spark.read.parquet("/Volumes/workspace/tracks/formula/parquet/Results/results_parquet").withColumnRenamed("Time","Race_Time").withColumnRenamed("Race_Id","Race_ID").withColumnRenamed("Driver_Id","Driver_ID").withColumnRenamed("Constructor_Id","Constructor_ID")
results_df.printSchema()

# COMMAND ----------

#Join of circuits and races first
races_circuits_df = circuits_df.join(races_df,circuits_df.Circuit_ID == races_df.Circuit_ID,"inner").select(races_df.Race_ID,races_df.Race_Year,races_df.Race_Name,races_df.Race_Date,circuits_df.Circuit_Location)
races_circuits_df.printSchema()

# COMMAND ----------

#Join results to all other dataframes
race_results_df=results_df.join(races_circuits_df,results_df.Race_ID==races_circuits_df.Race_ID).join(drivers_df,results_df.Driver_ID==drivers_df.Driver_ID).join(constructors_df,results_df.Constructor_ID==constructors_df.Constructor_ID)

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp
final_dff=race_results_df.select("Race_Year","Race_Name","Race_Date","Circuit_Location","Driver_Name","Driver_Number","Driver_Nationality","Team","Grid","Fastest_Lap","Race_Time","Points","Position").withColumn("Created_Date",current_timestamp()).filter("Race_Year=2020 AND Race_Name='Abu Dhabi Grand Prix'").orderBy(col("Points").desc())
display(final_dff)

# COMMAND ----------

final_df=race_results_df.select("Race_Year","Race_Name","Race_Date","Circuit_Location","Driver_Name","Driver_Number","Driver_Nationality","Team","Grid","Fastest_Lap","Race_Time","Points","Position").withColumn("Created_Date",current_timestamp())

# COMMAND ----------

final_df.count()

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/Volumes/workspace/tracks/formula/Presentation_Layer/race_results")