"""Spark Structured Streaming — chuyển thể spark-stream/spark-kafka-streaming.py.

Đọc topic `data-stream` từ Kafka localhost:9092, parse JSON theo schema BĐS
và in batch ra console (chưa ghi MongoDB).
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, when
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

BROKER = "localhost:9092"
TOPIC = "data-stream"

KAFKA_PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"


def build_schema() -> StructType:
    ad = StructType([
        StructField("list_id", LongType(), True),
        StructField("price", LongType(), True),
        StructField("rooms", IntegerType(), True),
        StructField("size", DoubleType(), True),
        StructField("category_name", StringType(), True),
        StructField("living_size", DoubleType(), True),
        StructField("street_name", StringType(), True),
        StructField("ward_name", StringType(), True),
        StructField("area_name", StringType(), True),
        StructField("is_main_street", BooleanType(), True),
        StructField("property_legal_document", IntegerType(), True),
        StructField("status", StringType(), True),
    ])
    value = StructType([StructField("ad", ad, True)])
    return StructType([
        StructField("id", StringType(), True),
        StructField("value", value, True),
        StructField("timestamp", DoubleType(), True),
        StructField("cycle", IntegerType(), True),
    ])


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("Kafka-Stream-RealEstate")
        .config("spark.jars.packages", KAFKA_PACKAGES)
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BROKER)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    schema = build_schema()
    parsed = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_ts"),
        col("partition"),
        col("offset"),
    )

    flat = parsed.select(
        "data.timestamp",
        "data.cycle",
        "data.value.ad.*",
        "kafka_ts",
        "partition",
        "offset",
    ).withColumn("processed_at", current_timestamp())

    processed = (
        flat
        .withColumnRenamed("list_id", "id")
        .withColumn(
            "property_legal_document",
            when(col("property_legal_document") == 1, True).otherwise(False),
        )
        .withColumn(
            "status",
            when(col("status") == "active", True).otherwise(False),
        )
        .filter(col("id").isNotNull())
    )

    query = (
        processed.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 10)
        .trigger(processingTime="5 seconds")
        .start()
    )

    # Chạy giới hạn — đủ vài batch rồi thoát để không treo
    query.awaitTermination(timeout=60)
    query.stop()
    spark.stop()


if __name__ == "__main__":
    main()
