"""
Silver layer: read from Bronze Parquet, clean and enrich, write to Silver.

No sentiment here (avoids Spark worker issues). Sentiment is added later by
a batch job (e.g. sentiment_batch.py) which reads Silver and writes
silver_enriched with VADER. Gold then reads silver_enriched.

Bronze schema: topic, partition, offset, timestamp, key, value (struct), event_timestamp, event_date.
value.raw is JSON string; we parse it for engagement, tags, account, etc.
"""
import os
import sys
from typing import Dict

# Spark launches Python workers for UDFs; on Windows "python" may not resolve.
# Use the same interpreter that runs this script so workers can start.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

def bronze_schema() -> StructType:
    """Schema of Bronze Parquet (required for readStream; Spark does not infer for streaming)."""
    return StructType(
        [
            StructField("topic", StringType(), True),
            StructField("partition", IntegerType(), True),
            StructField("offset", LongType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("key", StringType(), True),
            StructField(
                "value",
                StructType(
                    [
                        StructField("id", StringType(), True),
                        StructField("created_at", StringType(), True),
                        StructField("content", StringType(), True),
                        StructField(
                            "account",
                            StructType(
                                [
                                    StructField("id", StringType(), True),
                                    StructField("acct", StringType(), True),
                                    StructField("display_name", StringType(), True),
                                ]
                            ),
                            True,
                        ),
                        StructField("visibility", StringType(), True),
                        StructField("language", StringType(), True),
                        StructField("raw", StringType(), True),
                    ]
                ),
                True,
            ),
            StructField("event_timestamp", TimestampType(), True),
            StructField("event_date", DateType(), True),
        ]
    )


def raw_json_schema() -> StructType:
    """Schema for value.raw (full Mastodon status JSON). Only fields needed for Silver/Gold."""
    return StructType(
        [
            StructField("reblogs_count", IntegerType(), True),
            StructField("favourites_count", IntegerType(), True),
            StructField("replies_count", IntegerType(), True),
            StructField("quotes_count", IntegerType(), True),
            StructField(
                "application",
                StructType([StructField("name", StringType(), True)]),
                True,
            ),
            StructField(
                "account",
                StructType(
                    [
                        StructField("followers_count", IntegerType(), True),
                        StructField("following_count", IntegerType(), True),
                        StructField("statuses_count", IntegerType(), True),
                        StructField("bot", BooleanType(), True),
                    ]
                ),
                True,
            ),
            StructField(
                "tags",
                ArrayType(
                    StructType(
                        [
                            StructField("name", StringType(), True),
                            StructField("url", StringType(), True),
                        ]
                    ),
                    True,
                ),
                True,
            ),
        ]
    )


def load_config() -> Dict[str, str]:
    load_dotenv()
    bronze_path = os.getenv("BRONZE_PARQUET_PATH", "data/bronze")
    silver_path = os.getenv("SILVER_PARQUET_PATH", "data/silver")
    checkpoint_path = os.getenv(
        "SILVER_CHECKPOINT_PATH", "data/checkpoints/silver_streaming"
    )
    return {
        "bronze_path": bronze_path,
        "silver_path": silver_path,
        "checkpoint_path": checkpoint_path,
    }


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("mastodon-silver-stream")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
        .getOrCreate()
    )


def main() -> None:
    cfg = load_config()
    spark = build_spark()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    # Read from Bronze (streaming requires explicit schema)
    bronze_df = (
        spark.readStream.format("parquet")
        .schema(bronze_schema())
        .load(cfg["bronze_path"])
    )

    # Parse value.raw (full Mastodon status JSON) for dashboard/Gold
    raw_parsed = F.from_json(
        F.coalesce(F.col("value.raw"), F.lit("{}")), raw_json_schema()
    )
    bronze_with_raw = bronze_df.withColumn("raw_parsed", raw_parsed)

    # Strip HTML tags with Spark SQL (no Python worker)
    content_clean = F.regexp_replace(
        F.coalesce(F.col("value.content"), F.lit("")), r"<[^>]+>", ""
    )

    # Tag names array for Gold (e.g. gold_trending_tags)
    tag_names = F.expr(
        "transform(coalesce(raw_parsed.tags, array()), t -> t.name)"
    )

    # Transform: value fields + parsed raw fields for dashboard
    silver_df = bronze_with_raw.select(
        F.col("value.id").alias("post_id"),
        F.to_timestamp(F.col("value.created_at")).alias("created_at"),
        content_clean.alias("clean_content"),
        F.col("value.language").alias("language"),
        F.col("value.visibility").alias("visibility"),
        F.col("value.account.id").alias("author_id"),
        F.col("value.account.acct").alias("author_acct"),
        F.col("value.account.display_name").alias("author_name"),
        # From value.raw (engagement & account)
        F.coalesce(F.col("raw_parsed.reblogs_count"), F.lit(0)).alias(
            "reblogs_count"
        ),
        F.coalesce(F.col("raw_parsed.favourites_count"), F.lit(0)).alias(
            "favourites_count"
        ),
        F.coalesce(F.col("raw_parsed.replies_count"), F.lit(0)).alias(
            "replies_count"
        ),
        F.coalesce(F.col("raw_parsed.quotes_count"), F.lit(0)).alias(
            "quotes_count"
        ),
        F.col("raw_parsed.application.name").alias("application_name"),
        F.col("raw_parsed.account.followers_count").alias(
            "author_followers_count"
        ),
        F.col("raw_parsed.account.following_count").alias(
            "author_following_count"
        ),
        F.col("raw_parsed.account.statuses_count").alias(
            "author_statuses_count"
        ),
        F.coalesce(F.col("raw_parsed.account.bot"), F.lit(False)).alias(
            "is_bot"
        ),
        tag_names.alias("tags"),
        F.col("event_date"),
    ).filter(F.col("language") == "en")

    # Sentiment is added later by batch job (sentiment_batch.py) → silver_enriched

    query = (
        silver_df.writeStream.outputMode("append")
        .format("parquet")
        .option("path", cfg["silver_path"])
        .option("checkpointLocation", cfg["checkpoint_path"])
        .partitionBy("event_date")
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
