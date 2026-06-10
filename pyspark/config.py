"""
Cấu hình cho tầng phân tích PySpark (analytics / visualize / dashboard).
"""
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent

# HDFS
HDFS_NAMENODE   = os.getenv("HDFS_NAMENODE", "")
HDFS_INPUT_PATH = os.getenv("HDFS_INPUT_PATH", str(PROJECT_ROOT.parent / "spark" / "output" / "real-estate"))

# Spark
SPARK_APP_NAME = "RealEstateAnalytics"
SPARK_MASTER   = os.getenv("SPARK_MASTER", "local[*]")

# Output ảnh
OUTPUT_DIR    = os.getenv("OUTPUT_DIR",    str(PROJECT_ROOT / "output"))
OUTPUT_FORMAT = os.getenv("OUTPUT_FORMAT", "png")

# Biểu đồ
CHART_FIGSIZE = (14, 8)
CHART_DPI     = 120

# Top-N
TOP_N = 15