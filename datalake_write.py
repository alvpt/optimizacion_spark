# Import the necessary modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Datalake Write").master("spark://spark-master:7077").getOrCreate()


# When performing a lot of transformations
    # If necessary, write to the datalake# If necessary, write to the datalake