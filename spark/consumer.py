"""
Spark Structured Streaming: đọc tin BĐS từ Kafka -> thống kê -> ghi HDFS (Parquet).
Schema khớp với bản ghi do kafka/push_data_to_kafka.py gửi lên.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, count, avg, current_timestamp, round as sround
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import config
import os
from sentence_transformers import SentenceTransformer

model = SentenceTransformer(
    "BAAI/bge-m3"
)

os.environ['HADOOP_HOME'] = r'C:\hadoop'  # Cần nếu chạy trên Windows và dùng winutils.exe

# Schema bản ghi BĐS (phải trùng key với push_data_to_kafka.normalize_ad)
listing_schema = StructType([
    StructField("list_id",       StringType(),  True),
    StructField("title",         StringType(),  True),
    StructField("description",   StringType(),  True),
    StructField("listing_type",  StringType(),  True),
    StructField("property_type", StringType(),  True),
    StructField("price",         DoubleType(),  True),
    StructField("price_text",    StringType(),  True),
    StructField("area_m2",       DoubleType(),  True),
    StructField("rooms",         IntegerType(), True),
    StructField("toilets",       IntegerType(), True),
    StructField("region",        StringType(),  True),
    StructField("district",      StringType(),  True),
    StructField("ward",          StringType(),  True),
    StructField("street",        StringType(),  True),
    StructField("latitude",      DoubleType(),  True),
    StructField("longitude",     DoubleType(),  True),
    StructField("posted_at",     StringType(),  True),
    StructField("url",           StringType(),  True),
    StructField("full_text",     StringType(),  True),
])


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName(config.SPARK_APP_NAME)
        .master(config.SPARK_MASTER)
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2")
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.streaming.checkpointLocation", config.HDFS_CHECKPOINT_PATH)
        .config("spark.hadoop.fs.defaultFS", config.HDFS_NAMENODE)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def process_and_save_batch(batch_df, batch_id):
    """Thống kê đặc thù BĐS rồi ghi Parquet vào HDFS."""
    try:
        total = batch_df.count()
        if total == 0:
            print(f"\nBatch {batch_id}: không có dữ liệu mới")
            return

        print(f"\n{'='*60}\nBATCH {batch_id} - THỐNG KÊ BẤT ĐỘNG SẢN\n{'='*60}")
        print(f"Tổng số tin: {total}")

        # Bán / Cho thuê
        print("\nTheo hình thức:")
        for r in (batch_df.groupBy("listing_type").agg(count("*").alias("n"))
                  .orderBy(col("n").desc()).collect()):
            print(f"  • {str(r['listing_type'])[:15]:15s}: {r['n']:5d}")

        # Phân bố theo loại hình
        print("\nTheo loại hình:")
        for r in (batch_df.groupBy("property_type").agg(count("*").alias("n"))
                  .orderBy(col("n").desc()).collect()):
            print(f"  • {str(r['property_type'])[:30]:30s}: {r['n']:5d}")

        # Top quận/huyện
        print("\nTop khu vực:")
        for r in (batch_df.filter(col("district") != "")
                  .groupBy("district").agg(count("*").alias("n"))
                  .orderBy(col("n").desc()).limit(10).collect()):
            print(f"  • {str(r['district'])[:30]:30s}: {r['n']:5d}")

        # Thống kê giá (VND) — CHỈ tính tin BÁN để không lẫn với giá thuê/tháng
        priced = batch_df.filter(
            (col("listing_type") == "Bán") & col("price").isNotNull() & (col("price") > 0)
        )
        if priced.count() > 0:
            s = priced.selectExpr(
                "min(price) mn", "max(price) mx", "avg(price) av"
            ).collect()[0]
            med = priced.approxQuantile("price", [0.5], 0.05)
            med = med[0] if med else None
            print("\nGiá BÁN (tỷ VND):")
            print(f"  • Thấp nhất : {s['mn']/1e9:.2f}")
            print(f"  • Cao nhất  : {s['mx']/1e9:.2f}")
            print(f"  • Trung bình: {s['av']/1e9:.2f}")
            if med:
                print(f"  • Trung vị  : {med/1e9:.2f}")

        sized = batch_df.filter(col("area_m2").isNotNull() & (col("area_m2") > 0))
        if sized.count() > 0:
            a = sized.selectExpr("avg(area_m2) av", "min(area_m2) mn", "max(area_m2) mx").collect()[0]
            print(f"\nDiện tích (m²): TB {a['av']:.1f} | nhỏ nhất {a['mn']:.0f} | lớn nhất {a['mx']:.0f}")

        # Ghi HDFS dạng Parquet, partition theo loại hình
        # Ghi HDFS — CHỈ ghi tin có list_id chưa tồn tại
        spark = batch_df.sparkSession
        out_df = (batch_df
                  .dropDuplicates(["list_id"])              # khử trùng trong chính batch
                  .withColumn("processed_at", current_timestamp()))

        try:
            existing = spark.read.parquet(config.HDFS_OUTPUT_PATH).select("list_id")
            out_df = out_df.join(existing, "list_id", "left_anti")  # bỏ tin đã có trong HDFS
        except Exception:
            pass  # lần chạy đầu chưa có thư mục output -> ghi tất cả

        n_new = out_df.count()
        if n_new == 0:
            print("\n⏭️  Không có tin mới (tất cả list_id đã có trong HDFS).")
            return

        (out_df.write
            .mode("append")
            .partitionBy("property_type")
            .parquet(config.HDFS_OUTPUT_PATH))
        print(f"\n✅ Đã ghi {n_new} tin MỚI vào {config.HDFS_OUTPUT_PATH}")

        
    except Exception as e:
        print(f"❌ Lỗi xử lý batch {batch_id}: {e}")
        import traceback
        traceback.print_exc()


def main():
    spark = create_spark_session()
    try:
        kafka_df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", config.KAFKA_TOPIC)
            .option("startingOffsets", config.KAFKA_STARTING_OFFSET)
            .load()
        )

        parsed_df = (
            kafka_df
            .select(from_json(col("value").cast("string"), listing_schema).alias("d"),
                    col("timestamp").alias("kafka_ts"))
            .select("d.*", "kafka_ts")
            .filter(col("list_id").isNotNull() & col("title").isNotNull())
        )

        query = (
            parsed_df.writeStream
            .foreachBatch(process_and_save_batch)
            .outputMode("append")
            .trigger(processingTime="10 seconds")
            .start()
        )
        print("✅ Spark Streaming đã bắt đầu. Ctrl+C để dừng.")
        query.awaitTermination()

    except KeyboardInterrupt:
        print("\n⏹️  Đang dừng...")
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()
        print("✅ Đã dừng Spark Session")


if __name__ == "__main__":
    main()