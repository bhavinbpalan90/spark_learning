from pyspark import SparkConf
from pyspark.sql import SparkSession
from delta import *
import time
 
conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider')

spark = SparkSession.builder.config(conf=conf).getOrCreate()

df = spark.read.parquet(f's3a://aws-s3-open/crunchbase/CRUNCHBASE_2.parquet_0_0_0.snappy.parquet', inferSchema=True)
df.show()

