import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("PySpark Installation Test").getOrCreate()
df = spark.createDataFrame([(1, "Hello"), (2, "World")], ["id", "message"])
df.show()