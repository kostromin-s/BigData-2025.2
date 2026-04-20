"""
Cấu hình cho Spark Consumer
"""
import os

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "legal-documents")
KAFKA_STARTING_OFFSET = os.getenv("KAFKA_STARTING_OFFSET", "earliest")

# Spark Configuration
SPARK_APP_NAME = "LegalDocumentsConsumer"
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

# HDFS Configuration
HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000")
HDFS_OUTPUT_PATH = os.getenv("HDFS_OUTPUT_PATH", "/data/legal-documents")
HDFS_CHECKPOINT_PATH = os.getenv("HDFS_CHECKPOINT_PATH", "/tmp/spark-checkpoint")
 