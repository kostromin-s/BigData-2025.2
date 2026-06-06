"""
Cấu hình cho tầng phân tích PySpark (analytics / visualize / dashboard).
"""
import os

# HDFS
HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000")
HDFS_INPUT_PATH = os.getenv("HDFS_INPUT_PATH", "/data/real-estate")

# Spark
SPARK_APP_NAME = "RealEstateAnalytics"
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

# Output ảnh
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/app/output")
OUTPUT_FORMAT = os.getenv("OUTPUT_FORMAT", "png")

# Biểu đồ
CHART_FIGSIZE = (14, 8)
CHART_DPI = 120

# Số nhóm hiển thị tối đa (top-N quận/huyện)
TOP_N = 15