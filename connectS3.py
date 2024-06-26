from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


# Create a Spark session with your AWS Credentials

conf = (
    SparkConf()
    .setAppName("MY_APP") # replace with your desired name
    .set("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.3.2")
    .set("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
    .set("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)
    .set("spark.hadoop.fs.s3a.session.token", aws_session_token) # optional
    .set("spark.sql.shuffle.partitions", "4") # default is 200 partitions which is too many for local
    .setMaster("local[*]") # replace the * with your desired number of cores. * for use all.
)

spark = SparkSession.builder.config(conf=conf).getOrCreate()