"""Kafka producer — chuyển thể producer_kafka/producer.py cho local.

Đọc all_raw_data.json đã crawl, gửi mỗi record dưới format:
    { "id": <list_id>, "value": <raw_ad>, "timestamp": <epoch>, "cycle": <n> }
vào topic `data-stream` ở localhost:9092.

Khác với repo: chạy hữu hạn (2 cycle) thay vì vòng lặp vô hạn.
"""
import json
import sys
import time
from pathlib import Path

from kafka import KafkaProducer

BROKER = "localhost:9092"
TOPIC = "data-stream"
DATA_FILE = Path(__file__).parent.parent / "crawler" / "data" / "all_raw_data.json"
INTERVAL = 0.3   # giây giữa các record (repo dùng 10s)
CYCLES = 1       # số vòng lặp toàn bộ dataset


def create_producer() -> KafkaProducer:
    print(f"→ kết nối Kafka {BROKER}")
    for attempt in range(1, 11):
        try:
            p = KafkaProducer(
                bootstrap_servers=BROKER,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8"),
                acks="all",
                retries=5,
                request_timeout_ms=30000,
                max_block_ms=30000,
            )
            p.send(TOPIC, key="test", value={"test": "ping"}).get(timeout=15)
            print("✓ Kafka sẵn sàng")
            return p
        except Exception as e:
            print(f"✗ lần {attempt}/10: {e}")
            time.sleep(3)
    sys.exit("❌ không kết nối được Kafka")


def main() -> None:
    data = json.loads(DATA_FILE.read_text(encoding="utf-8"))
    print(f"Load {len(data)} records từ {DATA_FILE.name}")

    producer = create_producer()
    sent = 0
    for cycle in range(1, CYCLES + 1):
        print(f"\n── cycle {cycle}/{CYCLES} ──")
        for rid, value in data.items():
            msg = {"id": rid, "value": value, "timestamp": time.time(), "cycle": cycle}
            producer.send(TOPIC, key=rid, value=msg).get(timeout=10)
            sent += 1
            if sent % 10 == 0:
                print(f"  gửi {sent}")
            time.sleep(INTERVAL)
    producer.flush()
    producer.close()
    print(f"\n✓ tổng đã gửi: {sent}")


if __name__ == "__main__":
    main()
