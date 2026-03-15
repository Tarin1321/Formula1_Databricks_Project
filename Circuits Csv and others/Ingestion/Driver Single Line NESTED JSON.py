# Databricks notebook source
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

drivers_read=spark.read.option("header",True).schema(drivers_schema).json("/Volumes/workspace/tracks/formula/drivers.json")
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

drivers_final.write.mode("overwrite").parquet("/Volumes/workspace/tracks/formula/parquet/drivers/drivers_parquet")