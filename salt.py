# Salting the keys



import pyspark.sql.functions as F
df = df.withColumn('salt', F.rand())
df = df.repartition(8, 'salt')

df.groupBy(F.spark_partition_id()).count().show()