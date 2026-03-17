import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, FloatType, StructType, StructField, StringType, LongType, TimestampType, MapType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# 1. Setup Environment
# os.environ['HADOOP_HOME'] = r'C:\hadoop'
# os.environ['PATH'] = r'C:\hadoop\bin;C:\Windows\System32;' + os.environ.get('PATH', '')

spark = SparkSession.builder \
    .appName("MastodonSilverLayer") \
    .getOrCreate()


# Define the schema based on your Bronze Parquet structure
bronze_schema = StructType([
    StructField("topic", StringType(), True),
    StructField("partition", LongType(), True),
    StructField("offset", LongType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("key", StringType(), True),
    StructField("value", StringType(), True), # This matches your JSON string in Bronze
    StructField("event_date", StringType(), True)
])

# 2. VADER Sentiment Logic
analyzer = SentimentIntensityAnalyzer()

def get_sentiment(text):
    if not text: return 0.0
    # Returns the compound score (-1 to 1)
    return float(analyzer.polarity_scores(text)['compound'])

sentiment_udf = udf(get_sentiment, FloatType())

# 3. HTML Cleaner Logic
def clean_html(text):
    if not text: return ""
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)

clean_html_udf = udf(clean_html, StringType())

# 4. Read from Bronze
bronze_df = spark.readStream \
    .format("parquet") \
    .schema(bronze_schema) \
    .load("data/bronze")

# 5. Transform & Enrich
silver_df = bronze_df.select(
    col("value.id").alias("post_id"),
    col("value.created_at").cast("timestamp").alias("created_at"),
    # Clean the HTML FIRST, then run Sentiment
    clean_html_udf(col("value.content")).alias("clean_content"),
    col("value.language"),
    # Keep tags as an array (don't explode here!)
    col("value.tags.name").alias("tags"),
    col("value.raw.reblogs_count").cast("int"),
    col("value.raw.favourites_count").cast("int"),
    col("value.raw.account.display_name").alias("author_name"),
    col("value.raw.account.followers_count").cast("int").alias("author_followers"),
    col("value.raw.account.bot").alias("is_bot"),
    "event_date"
).filter((col("language") == "en") & (col("is_bot") == False)) # Filter bots out

# Add the Sentiment Score based on cleaned text
silver_df = silver_df.withColumn("sentiment_score", sentiment_udf(col("clean_content")))

# 6. Write to Silver Parquet
query = silver_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "data/silver") \
    .option("checkpointLocation", "data/checkpoints/silver_streaming") \
    .trigger(availableNow=True) \
    .start()

query.awaitTermination()
