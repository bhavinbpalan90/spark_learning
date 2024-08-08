# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('iceberg_lab') \
.config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.1,software.amazon.awssdk:bundle:2.20.160,software.amazon.awssdk:url-connection-client:2.20.160') \
.config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
.config('spark.sql.defaultCatalog', 'polaris') \
.config('spark.sql.catalog.polaris', 'org.apache.iceberg.spark.SparkCatalog') \
.config('spark.sql.catalog.polaris.type', 'rest') \
.config('spark.sql.catalog.polaris.header.X-Iceberg-Access-Delegation','true') \
.config('spark.sql.catalog.polaris.uri','https://vgb22968.us-east-1.snowflakecomputing.com/polaris/api/catalog') \
.config('spark.sql.catalog.polaris.credential','DKRZyPFtNttJfKKaY5hpiOoa/SM=:5N2dfD3GaYjz6J633yDMsc1kCIYxtvHb7qwQXhHAZ60=') \
.config('spark.sql.catalog.polaris.warehouse','BPALAN_EXTERNAL') \
.config('spark.sql.catalog.polaris.scope','PRINCIPAL_ROLE:ALL') \
.getOrCreate()



# COMMAND ----------

spark.sql("show namespaces").show()

