import json
import logging
from pathlib import Path
from kafka import KafkaProducer
from kafka.errors import KafkaError
import kafka_config

# Cấu hình Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DATA_FOLDER = Path(__file__).parent.parent / "crawl" / "3"

def on_send_success(record_metadata):
    """Callback khi gửi thành công"""
    pass

def on_send_error(excp):
    """Callback khi gửi thất bại"""
    logger.error(f'Lỗi khi gửi message: {excp}')

def read_document(file_path, domain):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        return {
            'domain': domain,
            'filename': file_path.name,
            'content': content,
            'file_path': str(file_path.relative_to(DATA_FOLDER))
        }, f"{domain}_{file_path.name}"
    except Exception as e:
        logger.error(f"Lỗi khi đọc file {file_path}: {e}")
        return None, None

def create_producer():
    return KafkaProducer(
        bootstrap_servers=kafka_config.KAFKA_BOOTSTRAP_SERVERS,
        # Tối ưu hóa hiệu năng:
        batch_size=16384,       # Gom nhóm các tin nhắn (16KB)
        linger_ms=10,           # Đợi 10ms để gom nhóm tin nhắn trước khi gửi
        compression_type='gzip', # Nén dữ liệu (rất hiệu quả với văn bản .txt)
        
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks='all',
        retries=5,             
        max_in_flight_requests_per_connection=5 
    )

def send_documents_to_kafka(producer):
    total_files = 0
    success_count = 0
    
    
    if not DATA_FOLDER.exists():
        logger.error(f"Thư mục {DATA_FOLDER} không tồn tại!")
        return

    # Tìm tất cả file .txt trong các folder con của DATA_FOLDER
    all_txt_files = list(DATA_FOLDER.rglob("*.txt"))
    logger.info(f"Bắt đầu xử lý {len(all_txt_files)} tệp tin...")

    for file_path in all_txt_files:
        domain = file_path.parent.name
        doc_data, key = read_document(file_path, domain)
        
        if doc_data:
            # Gửi Async
            producer.send(
                kafka_config.KAFKA_TOPIC,
                key=key,
                value=doc_data
            ).add_callback(on_send_success).add_errback(on_send_error)
            
            success_count += 1
            if success_count % 100 == 0:
                logger.info(f"Đã đưa {success_count}/{len(all_txt_files)} file vào hàng đợi gửi...")

    
    logger.info("Đang flush dữ liệu lên Kafka...")
    producer.flush() 
    
    logger.info("-" * 30)
    logger.info(f"Hoàn thành! Đã gửi thành công: {success_count} file.")
    logger.info("-" * 30)

def main():
    producer = None
    try:
        producer = create_producer()
        logger.info("Kết nối Kafka thành công!")
        send_documents_to_kafka(producer)
    except Exception as e:
        logger.error(f"Lỗi hệ thống: {e}")
    finally:
        if producer:
            producer.close()
            logger.info("Đã đóng kết nối Kafka.")

if __name__ == "__main__":
    main()