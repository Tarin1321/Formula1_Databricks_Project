# Databricks notebook source
# MAGIC %sql
# MAGIC USE DATABASE f1_INCREMENTAL

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

raw_folder_path = "/Volumes/workspace/tracks/formula"
base_path = f"{raw_folder_path}/{v_file_date}"

# COMMAND ----------

v_file_date

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

races_schema=StructType(fields=[StructField("raceId",IntegerType(),False),
                                StructField("year",IntegerType(),True),
                                StructField("round",IntegerType(),True),
                                StructField("circuitID",IntegerType(),True),
                                StructField("name",StringType(),True),
                                StructField("date",DateType(),True),
                                StructField("time",StringType(),True),
                                StructField("url",StringType(),True)
])

# COMMAND ----------

races_read=spark.read.option("header",True).schema(races_schema).csv(f"{base_path}/races.csv")
display(races_read)

# COMMAND ----------

races_med = races_read.withColumn(
    "race_timestamp",
    to_timestamp(
        concat(
            col("date"),
            lit(" "),
            when(col("time") == "\\N", lit("00:00:00"))
            .otherwise(col("time"))
        ),
        "yyyy-MM-dd HH:mm:ss"
    )
)

display(races_med)
races_med.printSchema()

# COMMAND ----------

races_medd=races_med.select(col("raceId").alias("Race_ID"),col("year").alias("Race_Year"),col("round").alias("Round"),col("circuitId").alias("Circuit_ID"),col("name").alias("Name"),col("race_timestamp").alias("Race_Timestamp"))
display(races_medd)

# COMMAND ----------


races_final=races_medd.withColumn("Ingested_DateTime",current_timestamp())
display(races_final)
races_final.printSchema()


# COMMAND ----------

races_final.write.mode("overwrite").partitionBy("Race_Year").saveAsTable("Full_Races")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Full_Races

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES