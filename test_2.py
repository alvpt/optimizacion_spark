# Import the necessary modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("NYC Yellow Trip in 2022 - Analytics To Database").master("spark://spark-master:7077").getOrCreate()
#df = spark.read().format("parquet").load("s3a://landing/yellow_tripdata_2023-01.parquet")
df = spark.read.parquet("s3a://landing/yellow_tripdata_2023-01.parquet")



print(df.columns)

print(df.collect())




