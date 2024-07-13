from pyspark import SparkConf
from pyspark.sql import SparkSession
import time
 
conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider')
#conf.set('spark.hadoop.fs.s3a.access.key', '')
#conf.set('spark.hadoop.fs.s3a.secret.key', '')
#conf.set('spark.hadoop.fs.s3a.session.token', <token>) 
 
spark = SparkSession.builder.config(conf=conf).getOrCreate()

#df = spark.read.parquet(f's3a://bp-parquet/DATAEXPORT/CRUNCHBASE_2.parquet_0_0_0.snappy.parquet', inferSchema=True) 
df = spark.read.parquet(f's3a://bp-parquet/DATAEXPORT/CRUNCHBASE2/CRUNCHBASE_2.parquet_0_0_0.snappy.parquet', inferSchema=True)
df.show()
