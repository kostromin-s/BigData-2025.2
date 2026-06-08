"""
Đẩy dữ liệu bất động sản lên Kafka.

Nguồn dữ liệu: crawler/data/all_raw_data.json  (dạng { list_id: raw_ad_json } lấy từ API Chợ Tốt)
Mỗi tin rao được CHUẨN HOÁ về một schema có cấu trúc rồi gửi lên Kafka topic real-estate-documents.

Lưu ý:
- Tên trường của API Chợ Tốt có thể thay đổi theo từng nhóm tin. Hàm normalize_ad() lấy
  dữ liệu phòng thủ bằng .get() và quét cả ad_params/parameters. Chạy thử:
      python push_data_to_kafka.py --inspect
  để in ra các key của bản ghi đầu tiên và đối chiếu, nếu cần thì chỉnh map bên dưới.
"""
import sys
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer
import kafka_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# File JSON thô do crawler sinh ra
RAW_FILE = Path(__file__).parent.parent / "crawler" / "data" / "all_raw_data.json"
SENT_FILE = Path(__file__).parent / "sent_ids.txt"   # nhớ các list_id đã gửi lên Kafka
def load_sent_ids() -> set:
    return set(SENT_FILE.read_text(encoding="utf-8").split()) if SENT_FILE.exists() else set()
# ----------------------------------------------------------------------------- #
# Helpers ép kiểu an toàn
# ----------------------------------------------------------------------------- #
def to_float(value):
    """Ép về float, chỉ giữ chữ số ASCII và dấu thập phân. Trả None nếu không parse được."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    digits = "".join(ch for ch in str(value) if ch in "0123456789.,")
    digits = digits.replace(",", "")
    try:
        return float(digits) if digits.strip(".") else None
    except ValueError:
        return None


def to_int(value):
    f = to_float(value)
    return int(f) if f is not None else None


def build_param_lookup(raw: dict) -> dict:
    """
    Gộp tất cả thuộc tính có cấu trúc về 1 dict {key: value}.
    Chợ Tốt lưu thuộc tính (rooms, toilets, legal...) ở nhiều chỗ:
      - ad["params"]: [ {"id": "rooms", "value": "2"}, ... ]   <-- hay dùng nhất
      - ad_params   : { "size": {"value": 50, ...}, ... }
      - parameters  : [ {"id": "size", "value": "50 m²"}, ... ]
    """
    lookup = {}
    ad = raw.get("ad", raw)
    sources = [ad.get("params") if isinstance(ad, dict) else None,
               raw.get("ad_params"),
               raw.get("parameters")]

    for src in sources:
        if isinstance(src, dict):
            for key, obj in src.items():
                val = obj.get("value") if isinstance(obj, dict) else obj
                if val not in (None, "", []):
                    lookup.setdefault(key, val)
        elif isinstance(src, list):
            for p in src:
                if isinstance(p, dict) and "id" in p:
                    val = p.get("value")
                    if val not in (None, "", []):
                        lookup.setdefault(p["id"], val)

    return lookup


# ----------------------------------------------------------------------------- #
# Chuẩn hoá 1 tin rao -> schema thống nhất
# ----------------------------------------------------------------------------- #
def normalize_ad(list_id, raw: dict) -> dict:
    """Map raw JSON Chợ Tốt -> bản ghi BĐS có cấu trúc."""
    ad = raw.get("ad", raw)          # field chính nằm trong "ad"; fallback dùng raw
    params = build_param_lookup(raw)

    def pick(*keys):
        """Lấy giá trị đầu tiên không rỗng từ ad rồi tới params."""
        for k in keys:
            v = ad.get(k)
            if v not in (None, "", []):
                return v
        for k in keys:
            v = params.get(k)
            if v not in (None, "", []):
                return v
        return None

    title = (pick("subject") or "").strip()
    description = pick("body") or ""

    # Bán hay cho thuê? Tin thuê luôn có "/tháng" trong chuỗi giá -> heuristic đáng tin
    price_text = pick("price_string") or ""
    listing_type = "Cho thuê" if "tháng" in price_text.lower() else "Bán"

    # Thời điểm đăng (list_time thường là epoch mil‑giây)
    posted_at = ""
    lt = pick("list_time")
    if lt is not None:
        ts = to_float(lt)
        if ts is not None:
            if ts > 1e12:        # milisecond -> second
                ts = ts / 1000.0
            try:
                posted_at = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
            except (OverflowError, OSError, ValueError):
                posted_at = str(lt)

    record = {
        "list_id":       str(list_id),
        "title":         title,
        "description":   description,
        "listing_type":  listing_type,
        "property_type": pick("category_name") or "Khác",
        "price":         to_float(pick("price")),
        "price_text":    price_text,
        "area_m2":       to_float(pick("size", "area", "living_size")),
        "rooms":         to_int(pick("rooms")),
        "toilets":       to_int(pick("toilets")),
        "region":        pick("region_name") or "",
        "district":      pick("area_name") or "",
        "ward":          pick("ward_name") or "",
        "street":        (pick("street_name", "street_number") or "").strip(),
        "latitude":      to_float(pick("latitude")),
        "longitude":     to_float(pick("longitude")),
        "posted_at":     posted_at,
        "url":           f"https://www.chotot.com/{list_id}.htm",
    }
    # Văn bản dùng cho RAG/chatbot sau này
    record["full_text"] = (title + "\n" + description).strip()
    return record


# ----------------------------------------------------------------------------- #
# Kafka
# ----------------------------------------------------------------------------- #
def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=kafka_config.KAFKA_BOOTSTRAP_SERVERS,
        client_id=kafka_config.KAFKA_CLIENT_ID,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        
        enable_idempotence=True, 
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=5,
        
        linger_ms=10,
        batch_size=16384,
        compression_type="gzip",
    )


def load_raw() -> dict:
    if not RAW_FILE.exists():
        logger.error("Không tìm thấy %s. Hãy chạy crawler trước (python crawler/crawl.py).", RAW_FILE)
        return {}
    return json.loads(RAW_FILE.read_text(encoding="utf-8"))


def send_all(producer: KafkaProducer, raw_map: dict) -> int:
    sent_ids = load_sent_ids()
    success, skipped = 0, 0
    total = len(raw_map)
    for list_id, raw in raw_map.items():
        if str(list_id) in sent_ids:        # đã gửi rồi -> KHÔNG đẩy lại lên Kafka
            skipped += 1
            continue
        try:
            record = normalize_ad(list_id, raw)
        except Exception as e:
            logger.warning("Bỏ qua %s do lỗi normalize: %s", list_id, e)
            continue

        producer.send(
            kafka_config.KAFKA_TOPIC,
            key=record["property_type"],
            value=record,
        )
        sent_ids.add(str(list_id))
        success += 1
        if success % 100 == 0:
            logger.info("Đã đưa %d/%d tin vào hàng đợi...", success, total)

    producer.flush()
    SENT_FILE.write_text("\n".join(sorted(sent_ids)), encoding="utf-8")   # lưu lại sau khi gửi xong
    logger.info("Gửi mới %d tin, bỏ qua %d tin đã gửi trước đó.", success, skipped)
    return success


def inspect(raw_map: dict) -> None:
    """In key của bản ghi đầu tiên + 1 bản ghi đã chuẩn hoá để kiểm tra map."""
    if not raw_map:
        return
    first_id, first_raw = next(iter(raw_map.items()))
    ad = first_raw.get("ad", first_raw)
    print("== Các key trong 'ad' ==")
    print(sorted(ad.keys()) if isinstance(ad, dict) else type(ad))
    print("\n== Bản ghi sau khi chuẩn hoá ==")
    print(json.dumps(normalize_ad(first_id, first_raw), ensure_ascii=False, indent=2))


def main() -> None:
    raw_map = load_raw()
    if not raw_map:
        return

    if "--inspect" in sys.argv:
        inspect(raw_map)
        return

    logger.info("Topic: %s | Bootstrap: %s", kafka_config.KAFKA_TOPIC, kafka_config.KAFKA_BOOTSTRAP_SERVERS)
    logger.info("Tổng số tin trong file thô: %d", len(raw_map))

    producer = None
    try:
        producer = create_producer()
        logger.info("Kết nối Kafka thành công!")
        sent = send_all(producer, raw_map)
        logger.info("Hoàn thành! Đã gửi %d tin lên Kafka.", sent)
    except Exception as e:
        logger.error("Lỗi: %s. Kiểm tra Kafka đã chạy chưa (docker-compose up).", e)
    finally:
        if producer:
            producer.close()
            logger.info("Đã đóng kết nối Kafka.")


if __name__ == "__main__":
    main()