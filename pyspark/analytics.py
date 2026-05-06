"""
PySpark Analytics - Đọc dữ liệu từ HDFS và thực hiện thống kê
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, 
    min as spark_min, max as spark_max,
    length, desc
)
import config
import os


def create_spark_session():
    """Tạo Spark Session với HDFS support"""
    spark = SparkSession.builder \
        .appName(config.SPARK_APP_NAME) \
        .master(config.SPARK_MASTER) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.hadoop.fs.defaultFS", config.HDFS_NAMENODE) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_data_from_hdfs(spark):
    """Đọc dữ liệu Parquet từ HDFS"""
    hdfs_path = f"{config.HDFS_NAMENODE}{config.HDFS_INPUT_PATH}"
    print(f"\nĐang đọc dữ liệu từ: {hdfs_path}")
    
    try:
        df = spark.read.parquet(hdfs_path)
        total = df.count()
        print(f"Đã đọc {total:,} documents từ HDFS")
        return df
    except Exception as e:
        print(f"Lỗi khi đọc dữ liệu: {e}")
        return None


def analyze_domain_distribution(df):
    """Phân tích phân bố theo domain"""
    print("\n" + "=" * 60)
    print("THỐNG KÊ THEO DOMAIN")
    print("=" * 60)
    
    domain_stats = df.groupBy("domain").agg(
        count("*").alias("doc_count"),
        spark_sum(length(col("content"))).alias("total_chars"),
        avg(length(col("content"))).alias("avg_chars")
    ).orderBy(desc("doc_count"))
    
    domain_data = domain_stats.collect()
    
    print(f"\n{'Domain':<35} {'Số doc':>10} {'Tổng chars':>15} {'TB chars':>12}")
    print("-" * 75)
    
    for row in domain_data:
        print(f"{row.domain:<35} {row.doc_count:>10,} {int(row.total_chars):>15,} {int(row.avg_chars):>12,}")
    
    return domain_data


def analyze_content_size(df):
    """Phân tích kích thước content"""
    print("\n" + "=" * 60)
    print("THỐNG KÊ KÍCH THƯỚC CONTENT")
    print("=" * 60)
    
    size_df = df.select(length(col("content")).alias("size"))
    
    stats = size_df.agg(
        count("*").alias("total_docs"),
        spark_min("size").alias("min_size"),
        spark_max("size").alias("max_size"),
        avg("size").alias("avg_size"),
        spark_sum("size").alias("total_size")
    ).collect()[0]
    
    print(f"\n  • Tổng số documents: {stats.total_docs:,}")
    print(f"  • Kích thước nhỏ nhất: {stats.min_size:,} ký tự")
    print(f"  • Kích thước lớn nhất: {stats.max_size:,} ký tự")
    print(f"  • Kích thước trung bình: {int(stats.avg_size):,} ký tự")
    print(f"  • Tổng kích thước: {stats.total_size:,} ký tự")
    
    # Phân bố kích thước theo khoảng
    print("\nPhân bố theo khoảng kích thước:")
    print("-" * 50)
    
    size_ranges = [
        (0, 1000, "0 - 1K"),
        (1000, 5000, "1K - 5K"),
        (5000, 10000, "5K - 10K"),
        (10000, 50000, "10K - 50K"),
        (50000, 100000, "50K - 100K"),
        (100000, float('inf'), "> 100K")
    ]
    
    size_distribution = []
    for min_s, max_s, label in size_ranges:
        if max_s == float('inf'):
            cnt = size_df.filter(col("size") >= min_s).count()
        else:
            cnt = size_df.filter((col("size") >= min_s) & (col("size") < max_s)).count()
        size_distribution.append((label, cnt))
        print(f"  • {label:<15}: {cnt:>8,} documents")
    
    return {
        "stats": stats,
        "distribution": size_distribution
    }


def run_analytics(spark):
    """Chạy tất cả các phân tích"""
    print("\n" + "=" * 60)
    print("PYSPARK ANALYTICS - LEGAL DOCUMENTS")
    print("=" * 60)
    print(f"HDFS NameNode: {config.HDFS_NAMENODE}")
    print(f"Input Path: {config.HDFS_INPUT_PATH}")
    
    # Load data
    df = load_data_from_hdfs(spark)
    if df is None:
        return None
    
    # Run analytics
    results = {
        "domain_stats": analyze_domain_distribution(df),
        "size_stats": analyze_content_size(df)
    }
    
    print("\n" + "=" * 60)
    print("HOÀN THÀNH PHÂN TÍCH")
    print("=" * 60)
    
    return results, df


def main():
    spark = create_spark_session()
    
    try:
        results = run_analytics(spark)
        if results:
            print("\n Sử dụng visualize.py để tạo biểu đồ từ kết quả này")
    except Exception as e:
        print(f"\n Lỗi: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
