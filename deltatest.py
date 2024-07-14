import pyspark
from delta import *

#Config SparkSession
builder = (
    pyspark.sql.SparkSession.builder.appName("MyApp")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

columns = ["Name", "Age"]
data = [("ABC",25),("xyz",27),("zaq",31)]
rdd=spark.sparkContext.parallelize(data)
df=rdd.toDF(columns)

df.write.format("delta").saveAsTable("MyFirstDeltaTable")

DeltaTable.isDeltaTable(spark,"spark-warehouse/myfirstdeltatable/")