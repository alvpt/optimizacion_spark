# Import the necessary modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Cache and Uncache").master("spark://spark-master:7077").getOrCreate()


# When performing a lot of transformations
    # Cache / persist the data, but also remember to unpersist

df = spark.range(1000000)
df.persist(storageLevel="DISK_ONLY")
df_filtered = df.filter("id % 2 == 0")
df_sum = df_filtered.selectExpr("sum(id)").collect()
df.unpersist()