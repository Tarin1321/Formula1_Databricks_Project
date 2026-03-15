# Databricks notebook source
# MAGIC %run "/Workspace/Users/tarin.kathuria@cubastion.com/Formula 1 Project/Circuits Csv and others/Ingestion/Races Read"

# COMMAND ----------

circuits_finally=circuits_final.withColumnRenamed('Name',"Circuit_Name")
races_finally=races_final.withColumnRenamed('Name',"Race_Name")

# COMMAND ----------

# MAGIC %run "/Workspace/Users/tarin.kathuria@cubastion.com/Formula 1 Project/Circuits Csv and others/Ingestion/Circuits Read"

# COMMAND ----------

races_filtered=races_finally.filter("Race_Year=2019")
display(races_filtered)

# COMMAND ----------

circuits_races_df=circuits_finally.join(races_filtered,circuits_final.Circuit_ID==races_filtered.Circuit_ID).select(circuits_finally.Circuit_Name,circuits_final.Location,circuits_final.Country,races_filtered.Race_Name,races_filtered.Round)
display(circuits_races_df)

# COMMAND ----------

#Left Outer Join
circuits_filtered=circuits_finally.filter("Circuit_ID<70")
circuits_races_df_left=circuits_filtered.join(races_filtered,circuits_filtered.Circuit_ID==races_filtered.Circuit_ID,"left").select(circuits_filtered.Circuit_Name,circuits_filtered.Location,circuits_filtered.Country,races_filtered.Race_Name,races_filtered.Round)
display(circuits_races_df_left)

# COMMAND ----------

#Right Outer Join
circuits_races_df_right=circuits_filtered.join(races_filtered,circuits_filtered.Circuit_ID==races_filtered.Circuit_ID,"right").select(circuits_filtered.Circuit_Name,circuits_filtered.Location,circuits_filtered.Country,races_filtered.Race_Name,races_filtered.Round)
display(circuits_races_df_right)

# COMMAND ----------

#Full Outer Join
circuits_races_df_full=circuits_filtered.join(races_filtered,circuits_filtered.Circuit_ID==races_filtered.Circuit_ID,"full").select(circuits_filtered.Circuit_Name,circuits_filtered.Location,circuits_filtered.Country,races_filtered.Race_Name,races_filtered.Round)
display(circuits_races_df_full)

# COMMAND ----------

#Semi Join
circuits_races_df_semi=circuits_filtered.join(races_filtered,circuits_filtered.Circuit_ID==races_filtered.Circuit_ID,"semi").select(circuits_filtered.Circuit_Name,circuits_filtered.Location,circuits_filtered.Country)
display(circuits_races_df_semi)

# COMMAND ----------

#Anti Join,represent records from left not on right
circuits_races_df_anti=circuits_filtered.join(races_filtered,circuits_filtered.Circuit_ID==races_filtered.Circuit_ID,"anti").select(circuits_filtered.Circuit_Name,circuits_filtered.Location,circuits_filtered.Country)
display(circuits_races_df_anti)

# COMMAND ----------

#Cross Join
circuits_races_df_cross=circuits_filtered.crossJoin(races_filtered)
display(circuits_races_df_cross)