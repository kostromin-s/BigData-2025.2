"""
Tạo vector index cho chatbot RAG TỪ DỮ LIỆU TỰ CRAWL (300 tin BĐS).

Đọc crawler/data/all_raw_data.json -> sinh text mô tả mỗi tin -> embed (e5-small, 384)
-> tạo + upsert vào Qdrant in-cluster (qdrant:6333).

Khớp với chatbot/rag_bakend.py:
  - cùng embed model (e5-small) -> cùng 384 chiều
  - payload lưu field "text" (đúng cái retrieve_context() đọc)
  - e5 quy ước: tài liệu dùng tiền tố "passage: ", truy vấn dùng "query: "

Thay cho build_index_real_estate.py (đọc sai nguồn ./data/real_estate, đang hỏng).
"""
import json
import os
import sys
from pathlib import Path

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.config import settings

from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct

RAW_FILE = Path(__file__).parent.parent / "crawler" / "data" / "all_raw_data.json"


def listing_text(list_id, raw: dict) -> str:
    """Gộp các trường chính của tin Chợ Tốt thành 1 đoạn text cho RAG."""
    ad = raw.get("ad", raw)
    parts = [
        ad.get("subject", ""),
        f"Khu vực: {ad.get('area_name', '')}, {ad.get('region_name', '')}".strip(", "),
        f"Giá: {ad.get('price_string', '')}",
        f"Loại hình: {ad.get('category_name', '')}",
        ad.get("body", ""),
    ]
    return "\n".join(str(p) for p in parts if p and str(p).strip())


def main():
    if not RAW_FILE.exists():
        print(f"[LỖI] Không thấy {RAW_FILE}. Hãy chạy crawler trước.")
        sys.exit(1)
    raw_map = json.loads(RAW_FILE.read_text(encoding="utf-8"))
    print(f"Đọc {len(raw_map)} tin từ {RAW_FILE}")

    print(f"Tải embedding model: {settings.EMBED_MODEL} ...")
    model = SentenceTransformer(settings.EMBED_MODEL)
    dim = model.get_sentence_embedding_dimension()

    client = QdrantClient(url=settings.QDRANT_URL, api_key=settings.QDRANT_API_KEY,
                          check_compatibility=False)

    # Tạo lại collection cho sạch (khớp số chiều của embed model)
    if client.collection_exists(settings.QDRANT_COLLECTION):
        client.delete_collection(settings.QDRANT_COLLECTION)
    client.create_collection(
        collection_name=settings.QDRANT_COLLECTION,
        vectors_config=VectorParams(size=dim, distance=Distance.COSINE),
    )

    points = []
    for i, (list_id, raw) in enumerate(raw_map.items()):
        text = listing_text(list_id, raw)
        if not text.strip():
            continue
        vec = model.encode(f"passage: {text}", normalize_embeddings=True).tolist()
        ad = raw.get("ad", raw)
        points.append(PointStruct(
            id=i,
            vector=vec,
            payload={
                "text": text,
                "list_id": str(list_id),
                "district": ad.get("area_name", ""),
                "url": f"https://www.chotot.com/{list_id}.htm",
            },
        ))

    client.upsert(collection_name=settings.QDRANT_COLLECTION, points=points)
    print(f"✅ Đã index {len(points)} tin vào collection "
          f"'{settings.QDRANT_COLLECTION}' (dim={dim}) tại {settings.QDRANT_URL}")


if __name__ == "__main__":
    main()
