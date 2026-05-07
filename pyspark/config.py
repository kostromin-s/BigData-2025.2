"""
Cấu hình cho PySpark Analytics
"""
import os

# HDFS Configuration
HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000")
HDFS_INPUT_PATH = os.getenv("HDFS_INPUT_PATH", "/data/real-estate")

# Spark Configuration
SPARK_APP_NAME = "RealEstateDocumentsAnalytics"
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

# Output Configuration
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/app/output")
OUTPUT_FORMAT = os.getenv("OUTPUT_FORMAT", "png")  

# Chart Configuration
CHART_DPI = 150
CHART_FIGSIZE = (12, 8)
CHART_STYLE = "seaborn-v0_8-darkgrid"
