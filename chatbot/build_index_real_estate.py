import os
import re
from pathlib import Path

from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter

from langchain_community.embeddings import HuggingFaceEmbeddings

from qdrant_client import QdrantClient
from langchain_qdrant import QdrantVectorStore


# ================= CONFIG =================
DATA_DIR = "./data/real_estate"
QDRANT_URL = "http://localhost:6333"
COLLECTION = "real_estate"
EMBED_MODEL = "intfloat/multilingual-e5-base"
# ==========================================


def extract_metadata(text: str) -> dict:
    """
    Trích xuất metadata từ nội dung bất động sản.

    Ví dụ:
    - giá
    - diện tích
    - vị trí

    Lưu ý:
    - version đơn giản (regex)
    """

    price = None
    area = None
    location = None

    # Giá (ví dụ: 2.5 tỷ, 3 tỷ)
    m = re.search(r"(\d+(\.\d+)?)\s*tỷ", text.lower())
    if m:
        price = float(m.group(1)) * 1e9

    # Diện tích (ví dụ: 70m2)
    m = re.search(r"(\d+)\s*m2", text.lower())
    if m:
        area = int(m.group(1))

    # Location đơn giản (ví dụ: quận 9)
    m = re.search(r"quận\s*\d+", text.lower())
    if m:
        location = m.group(0)

    return {
        "price": price,
        "area": area,
        "location": location,
    }


def load_docs():
    """
    Load và parse document.

    Version 2:
    - Có metadata
    - Chuẩn bị dữ liệu tốt hơn cho RAG
    """

    docs = []
    root = Path(DATA_DIR)

    for file_path in root.rglob("*.*"):
        try:
            text = file_path.read_text(encoding="utf-8", errors="ignore")

            meta = extract_metadata(text)

            doc = Document(
                page_content=text,
                metadata={
                    "source": file_path.name,
                    "location": meta["location"],
                    "price": meta["price"],
                    "area": meta["area"],
                }
            )

            docs.append(doc)

        except Exception as e:
            print("Lỗi:", file_path, e)

    return docs


def chunk_docs(raw_docs):
    """
    Chunk document nhưng giữ metadata.

    Cải tiến:
    - mỗi chunk giữ nguyên metadata
    """

    splitter = RecursiveCharacterTextSplitter(
        chunk_size=500,
        chunk_overlap=100,
    )

    chunks = []

    for d in raw_docs:
        parts = splitter.split_documents([d])

        for i, part in enumerate(parts):
            md = dict(d.metadata)
            md["chunk_id"] = i

            chunks.append(
                Document(
                    page_content = part.page_content,
                    metadata=md
                )
            )

    return chunks


def build_index():
    """
    Pipeline build index hoàn chỉnh:

    1. Load dữ liệu
    2. Extract metadata
    3. Chunk
    4. Embedding
    5. Lưu vào Qdrant
    """

    raw_docs = load_docs()
    chunks = chunk_docs(raw_docs)

    print("Raw docs:", len(raw_docs))
    print("Chunks:", len(chunks))

    if not chunks:
        print("Không có dữ liệu")
        return

    emb = HuggingFaceEmbeddings(model_name=EMBED_MODEL)

    client = QdrantClient(url=QDRANT_URL)

    # Reset collection
    try:
        client.delete_collection(collection_name=COLLECTION)
    except:
        pass

    QdrantVectorStore.from_documents(
        documents=chunks,
        embedding=emb,
        url=QDRANT_URL,
        collection_name=COLLECTION,
    )

    print("Build index thành công")


if __name__ == "__main__":
    build_index()

"""
Hò ơ con ơi con ngủ cho tròn
Để mẹ ngồi vót cho rồi bó chông
Chông này gìn giữ non sông
Chông này góp sức
Hò ơ
Chông này góp sức diệt quân bạo tàn
Nam quốc sơn hà nam đế cư
Tiệt nhiên định phận tại thiên thư
Như hà nghịch lỗ lai xâm phạm
Nhữ đẳng hành khang thủ bại hư
Nam Quốc Sơn Hà nam đế cư
Tiệt nhiên định phận tại thiên thư
Như hà nghịch lỗ lai xâm phạm
Nhữ đẳng hành khang thủ bại hư

Đi lên từ đường phố băng qua mọi hiểm nguy
Vai năm anh tấc rộng tâm anh như phỉ thúy
Và họ nói là máu đỏ da vàng
Đâu đơn giản đây là vàng không ngại thử lửa
Mỗi khi mà càn quét cứ gọi là như mưa!
Môn quan anh đón họ ra từ cửa
Tới những nơi phải cứu chữa việc của níu giữ
Những người anh những người em đang cứu lửa cứu lửa
Nơi đây đầu ngọn gió cỏ hương vẫn còn đó biển mặn nó đi ngang qua đường ta
Bọn anh đây là đồng chí bên kia là đồng đội nơi đây á Hoàng Sa! Trường Sa!
Thao trường đổ mồ hôi, chiến trường không đổ máu đấy là nhiệm vụ phải cân phải cân
Rắn rỏi cả bất khuất kiên cường từng tấc đất họ gọi anh là chiến sĩ hải quân
Giặc đến nhà đàn bà cũng đánh
Biết ơn Hai Bà Trưng cho hay thế nào hào kiệt
Việc nhà việc nước quyết liệt xung phong
Trung thành dũng mãnh chung lòng
Là con cháu rồng tiên
Chữ S mảnh đất thiêng
Dân Việt người cốt tiên
Chiến thắng theo cách riêng
Là con cháu rồng tiên
Chữ S mảnh đất thiêng
Dân Việt người cốt tiên
Chiến thắng theo cách riêng
Nam quốc sơn hà nam đế cư
Tiệt nhiên định phận tại thiên thư
Như hà nghịch lỗ lai xâm phạm
Nhữ đẳng hành khang thủ bại hư

Nam Quốc Sơn Hà nam đế cư
Tiệt nhiên định phận tại thiên thư
Như hà nghịch lỗ lai xâm phạm
Nhữ đẳng hành khang thủ bại hư
Nam Quốc Sơn Hà nam đế cư
Nam Quốc Sơn Hà nam đế cư

Việt Nam sinh ra tinh hoa
Tiếp nối truyền thống ông cha
Giặc đến nhà đàn bà cũng đánh
Chiến thắng này ắt không xa
Bao năm khi xưa loạn lạc
Sải cánh Lạc Hồng ta mang
Dân ta quyết đánh quyết thắng
Đất ta quyết chiến quyết giành

Ta vừa khoác lên vai những thử thách rất tự hào
Của nòi giống tiên rồng của bốn ngàn năm bất khuất
Lưng ngựa là ngai vàng ta thiết triều nơi trận mạc
Cùng ba quân đuổi giặc cứu sơn hà
Nam Quốc Sơn Hà nam đế cư
Tiệt nhiên định phận tại thiên thư
Như hà nghịch lỗ lai xâm phạm
Nhữ đẳng hành khang thủ bại hư
Nam Quốc Sơn Hà nam đế cư
Nam Quốc Sơn Hà nam đế cư
"""