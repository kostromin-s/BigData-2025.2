"""
Spark Structured Streaming: Đọc tin BĐS từ Kafka -> Thống kê real-time -> Ghi kho Parquet.
Cấu hình thích ứng đa môi trường (Local Windows / Docker Container HDFS).
"""
import os
import pathlib
import traceback
import sys

# ----------------------------------------------------------------------------- #
# Cấu hình biến môi trường hệ thống (Chỉ có tác dụng khi chạy Local Windows)
# ----------------------------------------------------------------------------- #
if os.name == 'nt':  # Nếu là hệ điều hành Windows
    os.environ['JAVA_HOME']   = r'C:\Program Files\Microsoft\jdk-17.0.19.10-hotspot'
    os.environ['HADOOP_HOME'] = r'C:\hadoop'
    os.environ['PATH']        = r'C:\Program Files\Microsoft\jdk-17.0.19.10-hotspot\bin;' + os.environ.get('PATH', '')

os.environ['PYSPARK_PYTHON']        = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, count, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Import file cấu hình động của hệ thống
import config

# ----------------------------------------------------------------------------- #
# Schema — Khớp hoàn toàn với cấu trúc JSON do Kafka Producer gửi lên
# ----------------------------------------------------------------------------- #
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

# Ghép nối đường dẫn động dựa trên môi trường cấu hình (HDFS URI hoặc Local Path)
OUTPUT_URI = f"{config.HDFS_NAMENODE}{config.HDFS_OUTPUT_PATH}"
CHECKPOINT_URI = f"{config.HDFS_NAMENODE}{config.HDFS_CHECKPOINT_PATH}"


# ----------------------------------------------------------------------------- #
# Khởi tạo Spark Session đồng bộ hạ tầng
# ----------------------------------------------------------------------------- #
def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(config.SPARK_APP_NAME)
        .master(config.SPARK_MASTER)
        # SỬA LẠI: Đồng nhất package Kafka sang bản Scala 2.12 khớp 100% với file Docker hạ tầng
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ----------------------------------------------------------------------------- #
# Hàm xử lý logic cho từng Micro-Batch (Mỗi 10 giây)
# ----------------------------------------------------------------------------- #
def process_and_save_batch(batch_df, batch_id):
    """Phân tích thống kê dữ liệu real-time và ghi Parquet chống trùng lặp."""
    # Đưa dữ liệu batch lên bộ nhớ RAM để tối ưu hóa hiệu năng tính toán lặp lại phía dưới
    batch_df.cache()
    try:
        total = batch_df.count()
        if total == 0:
            print(f"\n[Batch {batch_id}]: Không có dữ liệu mới trong 10 giây qua.")
            return

        print(f"\n{'='*60}\nBATCH {batch_id} - BÁO CÁO PHÂN TÍCH BẤT ĐỘNG SẢN REAL-TIME\n{'='*60}")
        print(f"Tổng số tin tiếp nhận: {total}")

        # Thống kê 1: Phân loại theo hình thức (Bán / Cho thuê)
        print("\nTheo hình thức thị trường:")
        for r in (batch_df.groupBy("listing_type")
                  .agg(count("*").alias("n"))
                  .orderBy(col("n").desc()).collect()):
            print(f"  • {str(r['listing_type'])[:15]:15s}: {r['n']:5d} tin")

        # Thống kê 2: Phân loại theo loại hình sản phẩm (Chung cư, Nhà ở, Đất nền...)
        print("\nTheo loại hình bất động sản:")
        for r in (batch_df.groupBy("property_type")
                  .agg(count("*").alias("n"))
                  .orderBy(col("n").desc()).collect()):
            print(f"  • {str(r['property_type'])[:30]:30s}: {r['n']:5d} tin")

        # Thống kê 3: Top 10 Khu vực Quận/Huyện sôi động nhất
        print("\nTop 10 khu vực biến động nguồn cung lớn nhất:")
        for r in (batch_df.filter(col("district") != "")
                  .groupBy("district").agg(count("*").alias("n"))
                  .orderBy(col("n").desc()).limit(10).collect()):
            print(f"  • {str(r['district'])[:30]:30s}: {r['n']:5d} tin")

        # Thống kê 4: Phân tích biên độ giá (Chỉ tính định dạng "Bán" và giá trị hợp lệ)
        priced = batch_df.filter(
            (col("listing_type") == "Bán") &
            col("price").isNotNull() & (col("price") > 0)
        )
        if priced.count() > 0:
            s = priced.selectExpr("min(price) mn", "max(price) mx", "avg(price) av").collect()[0]
            med = priced.approxQuantile("price", [0.5], 0.05)
            print("\nBiên độ GIÁ BÁN thị trường (tỷ VND):")
            print(f"  • Thấp nhất : {s['mn']/1e9:.2f}")
            print(f"  • Cao nhất  : {s['mx']/1e9:.2f}")
            print(f"  • Trung bình: {s['av']/1e9:.2f}")
            if med:
                print(f"  • Trung vị  : {med[0]/1e9:.2f}")

        # Thống kê 5: Phân tích diện tích (m²)
        sized = batch_df.filter(col("area_m2").isNotNull() & (col("area_m2") > 0))
        if sized.count() > 0:
            a = sized.selectExpr("avg(area_m2) av", "min(area_m2) mn", "max(area_m2) mx").collect()[0]
            print(f"\nThông số diện tích (m²): Trung bình {a['av']:.1f} | Nhỏ nhất {a['mn']:.0f} | Lớn nhất {a['mx']:.0f}")

        # ----------------------------------------------------------------------------- #
        # Khối chuẩn bị lưu trữ dữ liệu bền vững (Storage Layer)
        # ----------------------------------------------------------------------------- #
        spark = batch_df.sparkSession
        
        # 1. Lọc trùng lặp ngay trong chính batch hiện tại dựa trên list_id
        out_df = (batch_df
                  .dropDuplicates(["list_id"])
                  .withColumn("processed_at", current_timestamp()))

        # 2. Độc nhất hóa dữ liệu (DEDUP) bằng kỹ thuật LEFT ANTI JOIN với kho dữ liệu lịch sử
        try:
            # Đọc danh sách ID cũ từ thư mục Parquet đầu ra (Bất kể là HDFS hay Local)
            existing = spark.read.parquet(OUTPUT_URI).select("list_id")
            # Loại bỏ hoàn toàn những ID đã tồn tại trong kho lưu trữ
            out_df = out_df.join(existing, "list_id", "left_anti")
        except Exception:
            # Nếu gặp lỗi (Nghĩa là hệ thống chạy lần đầu tiên, chưa có thư mục Parquet cũ) -> Bỏ qua, ghi nhận toàn bộ data
            pass  

        n_new = out_df.count()
        if n_new == 0:
            print("\n⏭️  Không có dữ liệu mới (Tất cả bài tin trong batch này đã tồn tại trong kho lưu trữ).")
            return

        # 3. Ghi dữ liệu Parquet phân vùng (Partitioning) tối ưu hóa tốc độ truy vấn
        (out_df.write
         .mode("append")
         .partitionBy("property_type")  # Phân chia thư mục vật lý theo loại hình BĐS
         .parquet(OUTPUT_URI))
        
        print(f"\n✅ Đã đồng bộ thành công {n_new} tin MỚI vào kho dữ liệu: {OUTPUT_URI}")

    except Exception as e:
        print(f"❌ Xảy ra sự cố xử lý batch {batch_id}: {e}")
        traceback.print_exc()
    finally:
        # Giải phóng bộ nhớ RAM sau khi kết thúc một chu kỳ Micro-batch
        batch_df.unpersist()


# ----------------------------------------------------------------------------- #
# Luồng chạy chính (Main Pipeline)
# ----------------------------------------------------------------------------- #
def main():
    spark = create_spark_session()
    try:
        # Khởi tạo luồng nhận dữ liệu (Stream Reader) từ Kafka Broker
        kafka_df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", config.KAFKA_TOPIC)
            .option("startingOffsets", config.KAFKA_STARTING_OFFSET)
            # CHỈ ĐỌC dữ liệu đã committed thành công từ Transaction Producer (Exactly-Once chuẩn doanh nghiệp)
            .option("kafka.isolation.level", "read_committed")
            .load()
        )

        # Định hình cấu trúc (Parsing) từ chuỗi JSON nhị phân của Kafka sang cấu trúc cột phẳng
        parsed_df = (
            kafka_df
            .select(
                from_json(col("value").cast("string"), listing_schema).alias("d"),
                col("timestamp").alias("kafka_ts"),
            )
            .select("d.*", "kafka_ts")
            # Bộ lọc sơ bộ loại bỏ tin rác khuyết tiêu đề hoặc mã định danh
            .filter(col("list_id").isNotNull() & col("title").isNotNull())
        )

        # Kích hoạt trạm đẩy dữ liệu (Stream Writer) theo chu kỳ 10 giây một lần
        query = (
            parsed_df.writeStream
            .foreachBatch(process_and_save_batch)
            .outputMode("append")
            # ÉP BUỘC: Đưa đường dẫn checkpoint động vào writeStream để đảm bảo tính sẵn sàng cao (Fault Tolerance)
            .option("checkpointLocation", CHECKPOINT_URI)
            .trigger(processingTime="10 seconds")
            .start()
        )
        
        print(f"✅ Spark Structured Streaming đã khởi động thành công!")
        print(f"   • Đang nghe cổng Kafka: {config.KAFKA_BOOTSTRAP_SERVERS}")
        print(f"   • Kho lưu trữ cấu hình: {OUTPUT_URI}")
        print(f"   • Trạm ghi nhớ Checkpoint: {CHECKPOINT_URI}")
        print("▶ Press Ctrl+C to terminate system...")
        
        query.awaitTermination()

    except KeyboardInterrupt:
        print("\n⏹️  Nhận lệnh dừng hệ thống từ người dùng. Đang đóng luồng dữ liệu...")
    except Exception as e:
        print(f"❌ Hệ thống gặp sự cố chí mạng: {e}")
        traceback.print_exc()
        raise
    finally:
        # Đóng phiên làm việc giải phóng tài nguyên CPU/RAM cho máy chủ
        spark.stop()
        print("✅ Đã giải phóng hoàn toàn Spark Session. Hệ thống tắt an toàn.")


if __name__ == "__main__":
    main()