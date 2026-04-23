"""
Spark Consumer để đọc dữ liệu từ Kafka và lưu vào HDFS
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, count, current_timestamp, length
from pyspark.sql.types import StructType, StructField, StringType
import config

# Schema cho dữ liệu từ Kafka
document_schema = StructType([
    StructField("domain", StringType(), True),
    StructField("filename", StringType(), True),
    StructField("content", StringType(), True),
    StructField("file_path", StringType(), True)
])


def create_spark_session():
    """Tạo Spark Session với HDFS support"""
    spark = SparkSession.builder \
        .appName(config.SPARK_APP_NAME) \
        .master(config.SPARK_MASTER) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.streaming.kafka.maxRatePerPartition", "1000") \
        .config("spark.sql.streaming.checkpointLocation", config.HDFS_CHECKPOINT_PATH) \
        .config("spark.hadoop.fs.defaultFS", config.HDFS_NAMENODE) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def process_and_save_batch(batch_df, batch_id):
    """Xử lý batch: hiển thị thống kê và lưu vào HDFS"""
    try:
        if batch_df.count() == 0:
            print(f"\n{'='*60}")
            print(f"Batch {batch_id}: Không có dữ liệu mới")
            print(f"{'='*60}\n")
            return
        
        print(f"\n{'='*60}")
        print(f"BATCH {batch_id} - THỐNG KÊ DỮ LIỆU")
        print(f"{'='*60}")
        
        # Thêm timestamp cho dữ liệu
        batch_with_ts = batch_df.withColumn("processed_at", current_timestamp())
        
        # Tổng số documents
        total_count = batch_df.count()
        print(f"\n📊 Tổng số documents: {total_count}")
        
        # Thống kê theo domain
        print(f"\n📁 Phân bố theo Domain:")
        print("-" * 60)
        domain_stats = batch_df.groupBy("domain").agg(count("*").alias("doc_count")) \
            .orderBy(col("doc_count").desc())
        
        domain_list = domain_stats.collect()
        for row in domain_list:
            print(f"  • {row.domain:30s}: {row.doc_count:5d} documents")
        
        # Thống kê kích thước content
        print(f"\n📏 Thống kê kích thước Content:")
        print("-" * 60)
        
        from pyspark.sql.functions import min as spark_min, max as spark_max, avg, sum as spark_sum
        
        size_stats = batch_df.select(
            length(col("content").cast("string")).alias("content_length")
        )
        
        stats = size_stats.agg(
            spark_min("content_length").alias("min_size"),
            spark_max("content_length").alias("max_size"),
            avg("content_length").alias("avg_size"),
            spark_sum("content_length").alias("total_size")
        ).collect()[0]
        
        print(f"  • Kích thước nhỏ nhất: {int(stats.min_size):,} ký tự")
        print(f"  • Kích thước lớn nhất: {int(stats.max_size):,} ký tự")
        print(f"  • Kích thước trung bình: {int(stats.avg_size):,} ký tự")
        print(f"  • Tổng kích thước: {int(stats.total_size):,} ký tự")
        
        # === LƯU VÀO HDFS ===
        hdfs_path = f"{config.HDFS_NAMENODE}{config.HDFS_OUTPUT_PATH}"
        print(f"\n💾 Đang lưu vào HDFS: {hdfs_path}")
        
        try:
            batch_with_ts.write \
                .mode("append") \
                .partitionBy("domain") \
                .parquet(hdfs_path)
            
            print(f"✅ Đã lưu {total_count} documents vào HDFS thành công!")
        except Exception as e:
            print(f"❌ Lỗi khi lưu vào HDFS: {e}")
        
        # Hiển thị một vài documents mẫu
        print(f"\n📄 Mẫu documents (3 documents đầu tiên):")
        print("-" * 60)
        sample_df = batch_df.select("domain", "filename", "file_path").limit(3)
        for row in sample_df.collect():
            print(f"  • Domain: {row.domain}")
            print(f"    File: {row.filename}")
            print(f"    Path: {row.file_path}")
            print()
        
        print(f"{'='*60}\n")
        
    except Exception as e:
        print(f"❌ Lỗi khi xử lý batch {batch_id}: {e}")
        import traceback
        traceback.print_exc()


def main():
    """Hàm main để chạy Spark Streaming"""
    print("=" * 60)
    print("🚀 KHỞI TẠO SPARK CONSUMER + HDFS")
    print("=" * 60)
    print(f"Kafka Bootstrap Servers: {config.KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka Topic: {config.KAFKA_TOPIC}")
    print(f"HDFS NameNode: {config.HDFS_NAMENODE}")
    print(f"HDFS Output Path: {config.HDFS_OUTPUT_PATH}")
    print(f"Starting Offset: {config.KAFKA_STARTING_OFFSET}")
    print("=" * 60)
    
    # Tạo Spark Session
    spark = create_spark_session()
    
    try:
        # Đọc stream từ Kafka
        print("\n📡 Đang kết nối với Kafka...")
        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", config.KAFKA_TOPIC) \
            .option("startingOffsets", config.KAFKA_STARTING_OFFSET) \
            .option("failOnDataLoss", "false") \
            .load()
        
        print("✅ Đã kết nối với Kafka thành công!")
        
        # Parse JSON từ value
        print("\n🔄 Đang parse dữ liệu JSON...")
        parsed_df = kafka_df.select(
            col("key").cast("string").alias("kafka_key"),
            from_json(col("value").cast("string"), document_schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select(
            col("data.domain").alias("domain"),
            col("data.filename").alias("filename"),
            col("data.content").alias("content"),
            col("data.file_path").alias("file_path"),
            col("kafka_timestamp")
        ).filter(
            col("domain").isNotNull() & 
            col("filename").isNotNull() & 
            col("content").isNotNull()
        )
        
        print("✅ Đã parse dữ liệu thành công!")
        
        # Xử lý và lưu với foreachBatch
        print("\n📊 Bắt đầu xử lý, thống kê và lưu dữ liệu...")
        print("   (Dữ liệu sẽ được xử lý mỗi 10 giây)")
        print("\n" + "=" * 60)
        
        query = parsed_df.writeStream \
            .foreachBatch(process_and_save_batch) \
            .outputMode("append") \
            .trigger(processingTime="10 seconds") \
            .start()
        
        print("\n✅ Spark Streaming đã bắt đầu!")
        print("📊 Đang đọc, xử lý và lưu dữ liệu từ Kafka vào HDFS...")
        print("⏹️  Nhấn Ctrl+C để dừng\n")
        
        # Đợi query chạy
        query.awaitTermination()
            
    except KeyboardInterrupt:
        print("\n\n⏹️  Đang dừng Spark Streaming...")
    except Exception as e:
        print(f"\n❌ Lỗi trong quá trình xử lý: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()
        print("✅ Đã dừng Spark Session")


if __name__ == "__main__":
    main()