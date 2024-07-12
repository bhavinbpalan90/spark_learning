import pyspark
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.master("local[*]") \
                    .appName('BigData-ETL.com') \
                    .getOrCreate()

print(f'The PySpark {spark.version} version is running...')