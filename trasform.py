from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, expr, max

spark = SparkSession.builder.appName("GitHub PRs Transformation").getOrCreate()

json_files_path = "resources/*.json"
df = spark.read.option("multiline", "true").json(json_files_path)
df.cache()

clean_df = df.filter(df["body"].isNull())

transformed_df = (
    clean_df.groupBy(
        col("head.repo.id").alias("repository_id"),
        col("head.repo.name").alias("repository_name"),
        col("head.repo.owner.login").alias("repository_owner"),
        col("head.repo.full_name").alias("full_name"),
    )
    .agg(
        count("*").alias("num_prs"),
        count(when(col("merged_at").isNotNull(), True)).alias("num_prs_merged"),
        max("merged_at").alias("merged_at"),
    )
    .withColumn("Organization Name", expr("split(full_name, '/')[0]"))
    .withColumn(
        "is_compliant",
        (col("num_prs") == col("num_prs_merged"))
        & (col("repository_owner").contains("scytale")),
    )
    .select(
        "Organization Name",
        "repository_id",
        "repository_name",
        "repository_owner",
        "num_prs",
        "num_prs_merged",
        "merged_at",
        "is_compliant",
    )
)

output_path = "resources/transformed_data"
transformed_df.write.mode("overwrite").parquet(output_path)

print(f"Transformed data saved to {output_path}")

spark.stop()
