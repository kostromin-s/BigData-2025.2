from qdrant_client import QdrantClient
from qdrant_client.models import Filter

client = QdrantClient(url="http://localhost:6333")

# Xóa sạch mọi points bằng filter rỗng
client.delete(
    collection_name="real_estate",
    points_selector=Filter()
)

print("Đã dọn sạch toàn bộ dữ liệu bên trong collection real_estate!")