from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("InspecionarParquet").getOrCreate()

df = spark.read.parquet("datalake/processed")
df.printSchema()
df.show(10, truncate=False)
