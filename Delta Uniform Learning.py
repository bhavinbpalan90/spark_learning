# Databricks notebook source
# MAGIC %md
# MAGIC Read the Parquet Files from External Storage to Spark Dataframe

# COMMAND ----------

from pyspark.sql.functions import input_file_name, from_json, schema_of_json, col

# Read the Parquet Files from S3 using External Storage Location and store in Dataframe
df_parquet_read = spark.read.option("header", "true").parquet(
    's3://aws-s3-open/EXPORT/zillow/*.parquet'
    , inferSchema=True
)

display(df_parquet_read)



# COMMAND ----------

# MAGIC %md
# MAGIC Create a Delta Table from Spark Dataframe

# COMMAND ----------

df_parquet_read.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable("bhavinpalan.learning.zillow_v1")

# COMMAND ----------

# MAGIC %md
# MAGIC View Properties of Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED bhavinpalan.learning.zillow_v1 ;

# COMMAND ----------

# MAGIC %md
# MAGIC Enable Uniform on the Delta Table to Generate Iceberg Metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC REORG TABLE bhavinpalan.learning.zillow_v1 APPLY (UPGRADE UNIFORM(ICEBERG_COMPAT_VERSION=2));

# COMMAND ----------


