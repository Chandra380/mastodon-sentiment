import json
import os
from typing import Dict

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


def load_config() -> Dict[str, str]:
    load_dotenv()
    broker = os.getenv("KAFKA_BROKER", "localhost:29092")
    topic = os.getenv("KAFKA_TOPIC", "mastodon-posts")
    bronze_path = os.getenv("BRONZE_PARQUET_PATH", "data/bronze")
    checkpoint_path = os.getenv(
        "BRONZE_CHECKPOINT_PATH", "data/checkpoints/bronze_streaming"
    )
    return {
        "broker": broker,
        "topic": topic,
        "bronze_path": bronze_path,
        "checkpoint_path": checkpoint_path,
    }


def build_spark() -> SparkSession:
    """
    Build a local SparkSession with the Kafka connector attached.

    The PySpark package does not bundle the Kafka source by default, so we
    explicitly add the matching spark-sql-kafka artifact via spark.jars.packages.
    """
    return (
        SparkSession.builder.appName("mastodon-bronze-stream")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1",
        )
        # Force Hadoop to use Java-based IO on Windows
        .config("spark.hadoop.io.native.lib.available", "false")
        .getOrCreate()
    )


def mastodon_value_schema() -> StructType:
    # Schema for the value we send from the producer (not the full raw Mastodon payload)
    return StructType(
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
    )


def transform_stream(df: DataFrame) -> DataFrame:
    """
    Transform raw Kafka messages into a structured Bronze schema.

    We keep this close to the JSONL structure but as Parquet, with basic
    types and an event_date partition column derived from created_at.
    """
    value_schema = mastodon_value_schema()

    parsed = (
        df.select(
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp"),
            F.col("key").cast(StringType()).alias("key"),
            F.col("value").cast(StringType()).alias("value_str"),
        )
        .withColumn("value", F.from_json("value_str", value_schema))
        .drop("value_str")
    )

    # Derive event_date from created_at where possible; otherwise from Kafka timestamp
    parsed = parsed.withColumn(
        "created_at_ts",
        F.to_timestamp(F.col("value.created_at")),
    )

    parsed = parsed.withColumn(
        "event_timestamp",
        F.coalesce(F.col("created_at_ts"), F.col("timestamp")),
    ).drop("created_at_ts")

    parsed = parsed.withColumn(
        "event_date", F.to_date("event_timestamp")
    )

    return parsed


def main() -> None:
    cfg = load_config()
    spark = build_spark()

    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg["broker"])
        .option("subscribe", cfg["topic"])
        .option("startingOffsets", "earliest")
        .load()
    )

    transformed = transform_stream(df)

    query = (
        transformed.writeStream.outputMode("append")
        .format("parquet")
        .option("path", cfg["bronze_path"])
        .option("checkpointLocation", cfg["checkpoint_path"])
        .partitionBy("event_date")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()

