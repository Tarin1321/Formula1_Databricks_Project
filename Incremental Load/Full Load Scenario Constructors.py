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

constructors_schema=constructors_schema="constructorId INTEGER,constructorRef STRING, name STRING,nationality STRING,url STRING"

# COMMAND ----------

constructors_read=spark.read.option("header",True).schema(constructors_schema).json(f"{base_path}/constructors.json")
display(constructors_read)

# COMMAND ----------

from pyspark.sql.functions import *
constructors_med=constructors_read.drop(col("url"))
display(constructors_med)

# COMMAND ----------

constructors_pre_final=constructors_med.withColumn("Ingested_date",current_timestamp())
display(constructors_pre_final)
constructors_pre_final.printSchema()

# COMMAND ----------

constructors_final=constructors_pre_final.withColumnRenamed("ConstructorId","Constructor_ID").withColumnRenamed("constructorRef","Constructors_Ref").withColumnRenamed("name","Name").withColumnRenamed("nationality","Nationality")
display(constructors_final)
constructors_final.printSchema()

# COMMAND ----------

constructors_final.write.mode("overwrite").saveAsTable("Full_Constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Full_Constructors

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT initcap(Constructors_Ref) FROM Full_Constructors

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES