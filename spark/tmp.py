from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataCheck").getOrCreate()
df = spark.read.parquet("data/silver")

print("--- Schema ---")
df.printSchema()

print("\n--- Sample Data ---")
df.show(1, truncate=False)
# df.select("content", "account", "created_at").show(2, truncate=False)
