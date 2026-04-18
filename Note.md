# Note — Triển khai pipeline Crawl → Kafka → Spark

Tham chiếu repo gốc: https://github.com/30sweetener09/BigData_Real_Estate_Group13

Phạm vi đã triển khai: **crawler → Kafka broker → producer → Spark Structured Streaming**.
Các phần *chưa* làm theo yêu cầu: HDFS, consumer ghi HDFS, spark-batch, MongoDB, Metabase, Minikube/K8s.

---

## 1. Môi trường

| Thành phần | Phiên bản | Đường dẫn |
|---|---|---|
| macOS | darwin 24.6.0 (arm64) | — |
| Python | 3.14.3 | `/Library/Frameworks/Python.framework/Versions/3.14` |
| Temurin JDK | 17.0.13+11 | `~/tools/jdk-17.0.13+11/Contents/Home` |
| Apache Kafka | 3.8.1 (Scala 2.13) | `~/tools/kafka_2.13-3.8.1` |
| PySpark | 4.1.1 | pip user-site |
| kafka-python | 2.3.1 | pip user-site |
| requests | 2.33.1 | pip user-site |

> Brew **không dùng** được do `/opt/homebrew` không writable. JDK + Kafka tải binary trực tiếp vào `~/tools`.

Set env trước khi chạy Kafka/Spark:
```bash
export JAVA_HOME="$HOME/tools/jdk-17.0.13+11/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
```

---

## 2. Cấu trúc thư mục

```
BigData-2025.2/
├── crawler/
│   ├── crawl.py                  # thu list_id + crawl detail
│   └── data/
│       ├── all_ids.json          # set list_id
│       └── all_raw_data.json     # {list_id: raw_ad_json}
├── producer/
│   └── producer.py               # Kafka producer, sinks to localhost:9092
├── kafka/
│   ├── kafka_config.py           # (có sẵn từ trước)
│   └── start_kafka.sh            # khởi động Kafka KRaft single-node
├── spark/
│   └── spark_stream.py           # Structured Streaming, console sink
└── Note.md                       # file này
```

---

## 3. Crawler

**Nguồn dữ liệu:** REST API nội bộ Chợ Tốt (cùng backend với `nhatot.com`), KHÔNG crawl HTML.

- `GET https://gateway.chotot.com/v1/public/ad-listing?region_v2=12000&cg=1000&o={offset}&limit=20`
  - `region_v2=12000` = Hà Nội, `cg=1000` = Bất động sản
  - trả danh sách `ads[].list_id`
- `GET https://gateway.chotot.com/v1/public/ad-listing/{list_id}` → raw JSON chi tiết

**Tham số hiện tại** (trong `crawler/crawl.py`):
- `PAGE_START=0`, `PAGE_END=3` → 3 trang × 20 = 60 IDs
- `SLEEP_LIST=1.5s`, `SLEEP_DETAIL=0.8s`
- Checkpoint mỗi 20 records vào `all_raw_data.json`, bắt 404 → bỏ id khỏi danh sách

**Chạy:**
```bash
python3 crawler/crawl.py
```

---

## 4. Kafka broker (KRaft, single-node)

Script `kafka/start_kafka.sh` (bị `.gitignore` *.sh chặn, nên đưa inline để tái tạo):

```bash
#!/usr/bin/env bash
set -euo pipefail

KAFKA_HOME="${KAFKA_HOME:-$HOME/tools/kafka_2.13-3.8.1}"
JAVA_HOME="${JAVA_HOME:-$HOME/tools/jdk-17.0.13+11/Contents/Home}"
export JAVA_HOME PATH="$JAVA_HOME/bin:$PATH"

DATA_DIR="$HOME/tools/kafka-kraft-data"
CONFIG="$KAFKA_HOME/config/kraft/server.properties"
OVERLAY="$HOME/tools/kafka-server.properties"

mkdir -p "$DATA_DIR"
cp "$CONFIG" "$OVERLAY"
sed -i '' -e "s|^log.dirs=.*|log.dirs=$DATA_DIR|" "$OVERLAY"
grep -q '^advertised.listeners' "$OVERLAY" \
  && sed -i '' -e "s|^advertised.listeners=.*|advertised.listeners=PLAINTEXT://localhost:9092|" "$OVERLAY" \
  || echo "advertised.listeners=PLAINTEXT://localhost:9092" >> "$OVERLAY"

if [ ! -f "$DATA_DIR/meta.properties" ]; then
  CLUSTER_ID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"
  "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$OVERLAY"
fi

exec "$KAFKA_HOME/bin/kafka-server-start.sh" "$OVERLAY"
```

**Tạo topic (lần đầu):**
```bash
$HOME/tools/kafka_2.13-3.8.1/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --topic data-stream --partitions 3 --replication-factor 1
```

---

## 5. Producer

`producer/producer.py`: gửi mỗi record từ `all_raw_data.json` vào topic với format:
```json
{"id": "<list_id>", "value": <raw_ad>, "timestamp": <epoch>, "cycle": <n>}
```
`acks=all`, `retries=5`, chạy hữu hạn 1 cycle, interval 0.3s.

```bash
python3 producer/producer.py
```

---

## 6. Spark Structured Streaming

`spark/spark_stream.py`:
- Connector: `org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1` (tự tải lần đầu qua Ivy)
- Subscribe `data-stream`, `startingOffsets=earliest`
- Parse JSON theo schema hẹp (12 field BĐS), flatten `value.ad.*`
- Transform: `list_id → id`, `property_legal_document`/`status` → Bool
- Sink: `console`, trigger 5s, tự thoát sau 60s

```bash
python3 spark/spark_stream.py
```

---

## 7. Flow chạy

### 7.1. Sơ đồ

```
chotot gateway API  →  crawler/crawl.py  →  all_raw_data.json
                                                  ↓
                                         producer/producer.py
                                                  ↓
                                   Kafka broker (localhost:9092, topic data-stream)
                                                  ↓
                                        spark/spark_stream.py (console)
```

### 7.2. Trình tự (chạy lần đầu)

| Bước | Terminal | Lệnh |
|---|---|---|
| 1. env | any | `export JAVA_HOME=...; export PATH=...` |
| 2. crawl | T1 | `python3 crawler/crawl.py` |
| 3. Kafka | T2 giữ nền | `./kafka/start_kafka.sh` (đợi log `Kafka Server started`) |
| 4. tạo topic | T1 | `kafka-topics.sh --create --topic data-stream ...` |
| 5. produce | T1 | `python3 producer/producer.py` |
| 6. spark | T1 | `python3 spark/spark_stream.py` |

### 7.3. Kiểm tra nhanh

```bash
# topic có data?
$KH/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic data-stream --from-beginning --max-messages 3

# broker còn sống?
$KH/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 | head -3
```

### 7.4. Dừng / reset

```bash
pkill -f "kafka.Kafka"                     # dừng broker
pkill -f spark_stream.py                   # dừng spark
rm -rf ~/tools/kafka-kraft-data            # xoá state Kafka (format lại)
```

---

## 8. Khác biệt so với repo gốc

| Hạng mục | Repo gốc | Local |
|---|---|---|
| Orchestration | Minikube + K8s | process trực tiếp trên macOS |
| Kafka | StatefulSet DNS k8s | `localhost:9092` |
| Producer interval | 10s/record, vô hạn | 0.3s/record, 1 cycle |
| Spark sink | pymongo → MongoDB | `console` |
| Java | 11 | 17 (tương thích Spark 4.x) |

---

## 9. Bước tiếp theo (nếu muốn mở rộng)

- **Consumer → HDFS**: cần cài Hadoop local hoặc bật WebHDFS (`hdfs.InsecureClient`).
- **Spark batch**: đọc `hdfs:///data/stream/*.json` → transform → MongoDB.
- **MongoDB sink trong Spark Stream**: thêm `pymongo` vào `foreachBatch`, unique index trên `id`, bắt `DuplicateKeyError`.
- **Metabase**: dashboard đọc từ MongoDB collection `listings`.
