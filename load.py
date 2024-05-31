from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Read Transformed Data").getOrCreate()

parquet_file_path = "resources/transformed_data"

df = spark.read.parquet(parquet_file_path)
df.show(truncate=False)
df.printSchema()
spark.stop()
