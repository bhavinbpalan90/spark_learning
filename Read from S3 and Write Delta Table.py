# Databricks notebook source
# MAGIC %sql
# MAGIC SHOW EXTERNAL LOCATIONS

# COMMAND ----------

from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import from_json

# Read the Parquet Files from S3 using External Storage Location and store in Dataframe

df_parquet_read = spark.read.parquet(
    f's3://bp-parquet/DATAEXPORT/*.parquet', inferSchema=True
).withColumn(
    "inputFile", input_file_name()
).withColumnRenamed('OBJECT_CONSTRUCT(*)::VARIANT','RECORD')


# For the Column that is VARIANT/JSON/SEMI-STRUCTURED, need to determine the Schema and then create dataframe from it.

json_schema = spark.read.json(df_parquet_read.select("RECORD").rdd.map(lambda x: x[0])).schema
df_parquet_json_transformed = df_parquet_read.withColumn("RECORD", from_json("RECORD", json_schema)).select("RECORD.*")

# Below to Print the output
#display(df_parquet_json_transformed)

df_parquet_json_transformed.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable("CRUNCHBASE_DELTA")



# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED learning.default.crunchbase_delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM learning.default.crunchbase_delta LIMIT 10;
