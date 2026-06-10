"""
PySpark Analytics - Phân tích dữ liệu bất động sản từ HDFS (Parquet).
Thay thế phần thống kê theo "văn bản" bằng các chỉ số đặc thù BĐS:
  - phân bố theo loại hình / khu vực
  - thống kê giá, diện tích
  - giá trung bình & giá/m² theo quận
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, round as sround
import config
import os
import pathlib
import sys

os.environ['JAVA_HOME']   = r'C:\Program Files\Microsoft\jdk-17.0.19.10-hotspot'
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PATH']        = r'C:\Program Files\Microsoft\jdk-17.0.19.10-hotspot\bin;' + os.environ.get('PATH', '')

os.environ['PYSPARK_PYTHON']        = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(config.SPARK_APP_NAME)
        .master(config.SPARK_MASTER)
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0")
        .config("spark.sql.adaptive.enabled", "true")
        # Tắt log noise trên Windows
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.network.timeout", "300s")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")  # Chỉ hiện ERROR thật sự, bỏ WARN/INFO
    
    # Tắt thêm logger cụ thể gây noise
    log4j = spark._jvm.org.apache.log4j
    log4j.Logger.getLogger("org.apache.spark.storage.BlockManagerMasterEndpoint").setLevel(log4j.Level.OFF)
    log4j.Logger.getLogger("org.apache.spark.executor.Executor").setLevel(log4j.Level.OFF)
    
    return spark


def load_data_from_hdfs(spark):
    """Đọc Parquet, thêm cột giá/m²; trả None nếu rỗng."""
    try:
        df = spark.read.parquet(config.HDFS_INPUT_PATH)
    except Exception as e:
        print(f"❌ Không đọc được dữ liệu từ {config.HDFS_INPUT_PATH}: {e}")
        return None

    if df.rdd.isEmpty():
        print("⚠️ Không có dữ liệu trong HDFS.")
        return None

    # Giá trên mỗi m² (chỉ khi cả price và area hợp lệ)
    df = df.withColumn(
        "price_per_m2",
        (col("price") / col("area_m2")).cast("double"),
    )
    return df


def analyze_listing_type(df):
    print("\n--- Theo hình thức (Bán / Cho thuê) ---")
    stats = (df.groupBy("listing_type").agg(count("*").alias("n"))
             .orderBy(col("n").desc()))
    for r in stats.collect():
        print(f"  • {str(r['listing_type'])[:15]:15s}: {r['n']:6d}")
    return stats


def analyze_property_type(df):
    print("\n--- Phân bố theo loại hình ---")
    stats = (df.groupBy("property_type").agg(count("*").alias("n"))
             .orderBy(col("n").desc()))
    for r in stats.collect():
        print(f"  • {str(r['property_type'])[:35]:35s}: {r['n']:6d}")
    return stats


def analyze_district(df):
    print("\n--- Top khu vực (tin BÁN, theo số tin) ---")
    stats = (df.filter((col("listing_type") == "Bán") &
                       col("district").isNotNull() & (col("district") != ""))
             .groupBy("district")
             .agg(count("*").alias("n"),
                  sround(avg("price") / 1e9, 2).alias("avg_price_ty"),
                  sround(avg("area_m2"), 1).alias("avg_area_m2"))
             .orderBy(col("n").desc())
             .limit(config.TOP_N))
    for r in stats.collect():
        print(f"  • {str(r['district'])[:25]:25s}: {r['n']:5d} tin | "
              f"giá TB {r['avg_price_ty']} tỷ | DT TB {r['avg_area_m2']} m²")
    return stats


def analyze_price(df):
    print("\n--- Thống kê giá BÁN (tỷ VND) ---")
    priced = df.filter((col("listing_type") == "Bán") &
                       col("price").isNotNull() & (col("price") > 0))
    n = priced.count()
    if n == 0:
        print("  (không có dữ liệu giá)")
        return None
    s = priced.selectExpr("min(price) mn", "max(price) mx", "avg(price) av").collect()[0]
    q = priced.approxQuantile("price", [0.25, 0.5, 0.75], 0.05)
    print(f"  • Số tin có giá : {n}")
    print(f"  • Thấp nhất     : {s['mn']/1e9:.2f}")
    print(f"  • Cao nhất      : {s['mx']/1e9:.2f}")
    print(f"  • Trung bình    : {s['av']/1e9:.2f}")
    if len(q) == 3:
        print(f"  • Q1/Trung vị/Q3: {q[0]/1e9:.2f} / {q[1]/1e9:.2f} / {q[2]/1e9:.2f}")
    return s


def analyze_area(df):
    print("\n--- Phân bố diện tích (m²) ---")
    ranges = [(0, 30, "< 30"), (30, 50, "30-50"), (50, 70, "50-70"),
              (70, 100, "70-100"), (100, 150, "100-150"), (150, float("inf"), "> 150")]
    sized = df.filter(col("area_m2").isNotNull() & (col("area_m2") > 0))
    out = []
    for lo, hi, label in ranges:
        if hi == float("inf"):
            c = sized.filter(col("area_m2") >= lo).count()
        else:
            c = sized.filter((col("area_m2") >= lo) & (col("area_m2") < hi)).count()
        out.append((label, c))
        print(f"  • {label:>8} m²: {c:6d}")
    return out


def run_analytics(spark):
    print("\n" + "=" * 60)
    print("PYSPARK ANALYTICS - REAL ESTATE")
    print("=" * 60)
    df = load_data_from_hdfs(spark)
    if df is None:
        return None
    df.cache()
    print(f"Tổng số tin: {df.count()}")
    results = {
        "listing_type": analyze_listing_type(df),
        "property_type": analyze_property_type(df),
        "district": analyze_district(df),
        "price": analyze_price(df),
        "area": analyze_area(df),
    }
    print("\n" + "=" * 60 + "\nHOÀN THÀNH PHÂN TÍCH\n" + "=" * 60)
    return results, df


def main():
    spark = create_spark_session()
    try:
        run_analytics(spark)
        print("\nDùng visualize.py để xuất biểu đồ.")
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()