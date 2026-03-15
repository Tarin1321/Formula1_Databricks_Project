# Databricks notebook source
# MAGIC %sql
# MAGIC USE DATABASE f1_incremental

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

raw_folder_path = "/Volumes/workspace/tracks/formula"
base_path = f"{raw_folder_path}/{v_file_date}"
base_path1=f"{raw_folder_path}"

# COMMAND ----------

df=spark.read.json(f"{base_path}/results.json")
display(df)

# COMMAND ----------

from pyspark.sql.functions import *
def results_merge(file_date):
    df1=spark.read.json(f"{base_path1}/{file_date}/results.json")

    df1=df1.withColumn("file_date",lit(file_date))
    df1.createOrReplaceTempView("new_results")
    spark.sql("""MERGE INTO f1_incremental.incremental_results a USING 
              new_results b ON
              a.Result_Id=b.resultId 
              AND
              a.Driver_Id=b.driverId
              WHEN MATCHED THEN UPDATE SET *
              WHEN NOT MATCHED THEN INSERT * """)

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

results_schema=StructType(fields=[StructField("resultId",IntegerType(),False),
                                  StructField("raceId",IntegerType(),True),
                                  StructField("driverId",IntegerType(),True),
                                  StructField("constructorId",IntegerType(),True),
                                  StructField("number",IntegerType(),True),
                                  StructField("grid",IntegerType(),True),
                                  StructField("position",IntegerType(),True),
                                  StructField("positionText",StringType(),True),
                                  StructField("positionOrder",IntegerType(),True),
                                  StructField("points",FloatType(),True),
                                  StructField("laps",IntegerType(),True),
                                  StructField("time",StringType(),True),
                                  StructField("milliseconds",IntegerType(),True),
                                  StructField("fastestLap",IntegerType(),True),
                                  StructField("rank",IntegerType(),True),
                                  StructField("fastestLapTime",StringType(),True),
                                  StructField("fastestLapSpeed",StringType(),True),
                                  StructField("statusID",IntegerType(),True)])

# COMMAND ----------

results_read=spark.read.option("header",True).schema(results_schema).json(f"{base_path}/results.json")
display(results_read)
results_read.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *
results_mid=results_read.drop("statusId")
results_final=results_mid.withColumn("Ingested_Date",current_timestamp()).withColumn("file_date",lit(v_file_date)).withColumnRenamed("resultId","Result_Id").withColumnRenamed("raceId","Race_Id").withColumnRenamed("driverId","Driver_Id").withColumnRenamed("constructorId","Constructor_Id").withColumnRenamed("number","Number").withColumnRenamed("grid","Grid").withColumnRenamed("position","Position").withColumnRenamed("positionText","Position_Text").withColumnRenamed("positionOrder","Position_Order").withColumnRenamed("points","Points").withColumnRenamed("laps","Laps").withColumnRenamed("time","Time").withColumnRenamed("milliseconds","Milliseconds").withColumnRenamed("fastestLap","Fastest_Lap").withColumnRenamed("rank","Rank").withColumnRenamed("fastestLapTime","Fastest_Lap_Time").withColumnRenamed("fastestLapSpeed","Fastest_Lap_Speed")
display(results_final)


# COMMAND ----------

# MAGIC %sql
# MAGIC --TRUNCATE TABLE f1_incremental.incremental_results

# COMMAND ----------

results_final.write.mode("overwrite").saveAsTable("f1_incremental.incremental_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Incremental_Results ;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM  Incremental_Results ;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %skip
# MAGIC from pyspark.sql.functions import *
# MAGIC def results_merge(file_date):
# MAGIC     df1=spark.read.json(f"{base_path1}/{file_date}/results.json")
# MAGIC     df1=df1.withColumn("Ingested_Date",current_timestamp()).withColumnRenamed("resultId","Result_Id").withColumnRenamed("raceId","Race_Id").withColumnRenamed("driverId","Driver_Id").withColumnRenamed("constructorId","Constructor_Id").withColumnRenamed("number","Number").withColumnRenamed("grid","Grid").withColumnRenamed("position","Position").withColumnRenamed("positionText","Position_Text").withColumnRenamed("positionOrder","Position_Order").withColumnRenamed("points","Points").withColumnRenamed("laps","Laps").withColumnRenamed("time","Time").withColumnRenamed("milliseconds","Milliseconds").withColumnRenamed("fastestLap","Fastest_Lap").withColumnRenamed("rank","Rank").withColumnRenamed("fastestLapTime","Fastest_Lap_Time").withColumnRenamed("fastestLapSpeed","Fastest_Lap_Speed")
# MAGIC     display(results_final)
# MAGIC
# MAGIC     df1=df1.withColumn("file_date",lit(file_date))
# MAGIC     df1 = df1.replace("\\N", None)
# MAGIC     results_final.createOrReplaceTempView("new_results")
# MAGIC     spark.sql("""MERGE INTO f1_incremental.incremental_results a USING 
# MAGIC               new_results b ON
# MAGIC               a.Result_Id=b.Result_Id 
# MAGIC               AND
# MAGIC               a.Driver_Id=b.Driver_Id
# MAGIC               WHEN MATCHED THEN UPDATE SET *
# MAGIC               WHEN NOT MATCHED THEN INSERT * """)

# COMMAND ----------

# MAGIC %skip
# MAGIC results_merge("28-03-2021")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY incremental_results

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM incremental_results