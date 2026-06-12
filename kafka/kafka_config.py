"""
Cấu hình Kafka
"""
import os

# Cho phép override qua biến môi trường (để chạy trong K8s dùng "kafka:9092").
# Mặc định localhost:29092 cho chạy local với docker-compose.
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "real-estate-documents")
KAFKA_CLIENT_ID = "real-estate-documents-producer"

