# Databricks notebook source
def merge_logic(final_df,main_table,join_logic):
    final_df.createOrReplaceTempView("final_view") 
    spark.sql(f"""MERGE INTO {main_table} a USING final_view b
              ON ({join_logic})
              WHEN MATCHED THEN UPDATE SET *
              WHEN NOT MATCHED THEN INSERT * """)


# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN f1_presentation