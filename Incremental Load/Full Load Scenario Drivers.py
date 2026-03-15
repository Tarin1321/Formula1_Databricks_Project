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

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

names_schema=StructType(fields=[StructField("forename",StringType(),True)
                                ,StructField("surname",StringType(),True)])

# COMMAND ----------

drivers_schema=StructType(fields=[StructField("driverId",IntegerType(),False),
                                  StructField("driverRef",StringType(),True),
                                  StructField("number",LongType(),True),
                                  StructField("code",StringType(),True),
                                  StructField("name",names_schema,True),
                                  StructField("dob",DateType(),True),
                                  StructField("nationality",StringType(),True),
                                  StructField("url",StringType(),True)])

# COMMAND ----------

drivers_read=spark.read.option("header",True).schema(drivers_schema).json(f"{base_path}/drivers.json")
display(drivers_read)
drivers_read.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *
drivers_mid=drivers_read.drop(col("url"))
display(drivers_mid)

# COMMAND ----------

drivers_final=drivers_mid.withColumn("name",concat(col("name.forename"),lit(' '),col("name.surname"))).withColumn("ingested_date",current_timestamp()).withColumnRenamed("driverId","Driver_ID").withColumnRenamed("driverRef","Driver_Ref").withColumnRenamed("number","Number").withColumnRenamed("code","Code").withColumnRenamed("name","Name").withColumnRenamed("dob","DOB").withColumnRenamed("nationality","Nationality")
display(drivers_final)

# COMMAND ----------

drivers_final.write.mode("overwrite").saveAsTable("Full_Drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM full_drivers

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES