# BigData-2025.2 — Nền tảng Phân tích Bất động sản Hà Nội

Hệ thống Big Data end-to-end thu thập, xử lý, phân tích dữ liệu bất động sản từ Chợ Tốt và cung cấp giao diện dashboard + chatbot RAG.

## Kiến trúc & Luồng dữ liệu

```
Chợ Tốt API
    ↓  crawler/crawl.py           → crawler/data/all_raw_data.json
    ↓  kafka/push_data_to_kafka.py → Kafka topic: real-estate-documents
    ↓  spark/consumer.py           → HDFS /data/real-estate (Parquet)
    ├→ pyspark/analytics.py        → thống kê batch
    ├→ pyspark/visualize.py        → biểu đồ PNG (pyspark/output/)
    └→ pyspark/dashboard.py        → Streamlit UI (port 8501)

Luồng RAG riêng:
    crawler/data/ → chatbot/build_index_real_estate.py → Qdrant vector DB
                                                              ↓
                                              chatbot/rag_real_estate.py → Streamlit chatbot
```

## Cấu trúc thư mục

```
BigData-2025.2/
├── crawler/                  # Thu thập dữ liệu
│   ├── crawl.py              # Scrape Chợ Tốt API, lưu JSON
│   └── data/                 # all_ids.json, all_raw_data.json
│
├── kafka/                    # Message queue
│   ├── docker-compose.yml    # Kafka KRaft + Schema Registry + AKHQ
│   ├── kafka_config.py       # Bootstrap servers, topic name
│   └── push_data_to_kafka.py # Normalize JSON → produce to Kafka
│
├── hdfs/                     # Distributed storage
│   └── docker-compose.yml    # NameNode + DataNode
│
├── spark/                    # Stream processing
│   ├── docker-compose.yml    # Spark Structured Streaming container
│   ├── config.py             # Kafka, HDFS, Spark config
│   └── consumer.py           # Kafka → Parquet trên HDFS
│
├── pyspark/                  # Batch analytics & visualization
│   ├── docker-compose.yml    # Profiles: run, stats, dashboard
│   ├── config.py             # HDFS namenode, output settings
│   ├── analytics.py          # Thống kê batch từ HDFS
│   ├── visualize.py          # Tạo biểu đồ PNG
│   ├── dashboard.py          # Streamlit dashboard (port 8501)
│   └── output/               # PNG charts
│
├── chatbot/                  # RAG chatbot
│   ├── docker-compose.yml    # Qdrant vector DB
│   ├── build_index_real_estate.py  # Tạo vector index trong Qdrant
│   ├── rag_backend.py        # Backend dùng Google Gemini
│   ├── rag_real_estate.py    # Backend dùng Ollama/Llama (local)
│   └── demo.py               # Streamlit chatbot UI (PropAI)
│
├── Qdrant/                   # Vector DB setup
│   ├── docker-compose.yml    # Qdrant (port 6333/6334)
│   └── collections.py        # Khởi tạo collections
│
└── requirements.txt          # Python dependencies
```

## Công nghệ

| Layer | Công nghệ |
|-------|-----------|
| Crawler | Python requests, Chợ Tốt API |
| Streaming | Apache Kafka 7.5.0 (KRaft), Kafka-python 2.3.1 |
| Processing | Apache Spark 4.1.2 Structured Streaming, PySpark |
| Storage | Apache Hadoop 3.2.1 (HDFS), Parquet format |
| Vector DB | Qdrant |
| LLM / RAG | LangChain 0.3.30, Google Gemini / Ollama Llama 3.2 |
| Embeddings | sentence-transformers (BAAI/bge-m3, all-MiniLM-L6-v2) |
| Dashboard | Streamlit 1.50.0, Plotly 6.8.0 |
| Data | Pandas 2.3.3, PyArrow 21.0.0 |
| Infrastructure | Docker Compose |

## Cổng dịch vụ Docker

| Service | Port | File docker-compose |
|---------|------|---------------------|
| Kafka Broker | 9092, 29092 | kafka/ |
| Schema Registry | 8081 | kafka/ |
| AKHQ (Kafka UI) | 8080 | kafka/ |
| Redis | 6379 | kafka/ |
| HDFS NameNode UI | 9870 | hdfs/ |
| HDFS NameNode RPC | 9000 | hdfs/ |
| HDFS DataNode UI | 9864 | hdfs/ |
| Qdrant HTTP | 6333 | chatbot/ hoặc Qdrant/ |
| Qdrant gRPC | 6334 | chatbot/ hoặc Qdrant/ |
| Streamlit Dashboard | 8501 | pyspark/ |

## Cách khởi chạy

### 1. Khởi động infrastructure

```bash
# Kafka
cd kafka && docker compose up -d

# HDFS
cd hdfs && docker compose up -d

# Qdrant (cho chatbot)
cd chatbot && docker compose up -d
```

### 2. Thu thập dữ liệu

```bash
python crawler/crawl.py
# Output: crawler/data/all_raw_data.json (~60 listings từ Hà Nội)
```

### 3. Push dữ liệu lên Kafka

```bash
python kafka/push_data_to_kafka.py
# --inspect để xem dữ liệu mà không gửi
```

### 4. Chạy Spark Streaming consumer

```bash
# Qua Docker Compose
cd spark && docker compose up

# Hoặc trực tiếp
spark-submit spark/consumer.py
```

### 5. Analytics & Dashboard

```bash
# Batch analytics
cd pyspark && docker compose --profile run up
# hoặc: spark-submit pyspark/analytics.py

# Tạo biểu đồ PNG
spark-submit pyspark/visualize.py

# Web dashboard (port 8501)
cd pyspark && docker compose --profile dashboard up
# hoặc: streamlit run pyspark/dashboard.py
```

### 6. Chatbot RAG

```bash
# Tạo vector index (chạy một lần)
python chatbot/build_index_real_estate.py

# Khởi chạy chatbot (Gemini backend)
streamlit run chatbot/demo.py

# Hoặc Ollama backend (cần Ollama + Llama 3.2 cài sẵn)
streamlit run chatbot/rag_real_estate.py
```

## Schema dữ liệu

Normalized listing (20 fields) được normalize tại `kafka/push_data_to_kafka.py`:

```
list_id, title, description, listing_type (Bán/Cho thuê),
property_type, price (float, tỷ VND), price_text,
area_m2, rooms, toilets,
region, district, ward, street,
latitude, longitude, posted_at, url, full_text
```

HDFS Parquet được partition theo `property_type`.

## Cấu hình

| File | Nội dung |
|------|----------|
| `kafka/kafka_config.py` | `KAFKA_BOOTSTRAP_SERVERS`, `TOPIC_NAME = "real-estate-documents"` |
| `spark/config.py` | Kafka config, HDFS path `/data/real-estate`, checkpoint `/tmp/spark-checkpoint` |
| `pyspark/config.py` | `HDFS_NAMENODE`, `TOP_N = 10` districts |
| `chatbot/rag_backend.py` | Qdrant URL, collection name, Gemini API key (env: `GOOGLE_API_KEY`) |

## Lưu ý phát triển

- **Tránh duplicate trong HDFS**: `spark/consumer.py` dùng left anti-join giữa batch mới và dữ liệu HDFS hiện có theo `list_id`.
- **Tránh duplicate Kafka**: `kafka/push_data_to_kafka.py` lưu set `sent_ids` và bỏ qua các bản ghi đã gửi.
- **Outlier trong biểu đồ**: `pyspark/visualize.py` cắt tại percentile 95 trước khi vẽ histogram/scatter.
- **Micro-batch**: Spark consumer batch mỗi 10 giây (`trigger(processingTime="10 seconds")`).
- **Embedding dimension**: Qdrant collection dùng 384 chiều (all-MiniLM-L6-v2) hoặc 1024 chiều (bge-m3) — phải khớp model khi query.
- **Dashboard refresh**: `pyspark/dashboard.py` tự động refresh dữ liệu từ HDFS mỗi 5 phút.
