import os
os.environ["TORCH_DISTRIBUTED_DEBUG"] = "DETAIL"
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
import re
from pathlib import Path
import pandas as pd



from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter

from langchain_huggingface import HuggingFaceEmbeddings

from qdrant_client import QdrantClient
from langchain_qdrant import QdrantVectorStore
from common.config import settings




# ================= CONFIG =================
DATA_DIR: str = "../spark/output"
QDRANT_URL: str = settings.QDRANT_URL
COLLECTION: str = "real_estate"
EMBED_MODEL: str = settings.EMBED_MODEL
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
    Load dữ liệu từ các file Parquet phân vùng của Spark
    và chuyển đổi thành đối tượng Document của LangChain phục vụ RAG.
    """
    docs = []
    root = Path(DATA_DIR)
    
    # Quét toàn bộ các file đuôi .parquet nằm rải rác trong các thư mục phân vùng
    files = list(root.rglob("*.parquet"))
    
    if not files:
        print(f"Không tìm thấy file .parquet nào trong thư mục {DATA_DIR}!")
        return []

    print(f"Tìm thấy {len(files)} file dữ liệu Parquet phân vùng.")

    for file_path in files:
        try:
            # Dùng pandas đọc file nhị phân Parquet lên thành DataFrame
            df = pd.read_parquet(file_path)
            
            # Duyệt qua từng dòng (bài tin) trong Dataframe
            for _, row in df.iterrows():
                # Gộp tiêu đề và nội dung chi tiết thành một chuỗi text hoàn chỉnh để mô hình AI dễ hiểu ngữ nghĩa
                # Thay các tên cột ("title", "description") cho đúng với schema file Parquet của bạn
                title = row.get("title", "")
                content = row.get("description", row.get("content", ""))
                full_text = f"Tiêu đề: {title}\nNội dung chi tiết: {content}"
                
                # Trích xuất metadata từ các cột có sẵn của Spark thay vì dùng Regex cào lại từ text
                location = str(row.get("district", row.get("location", "Khác")))
                price = row.get("price", None)
                area = row.get("area_m2", row.get("area", None))
                
                # Ép kiểu dữ liệu chuẩn để Qdrant không bị lỗi
                price_val = float(price) if pd.notna(price) else None
                area_val = int(area) if pd.notna(area) else None

                doc = Document(
                    page_content=full_text,
                    metadata={
                        "source": file_path.name,
                        "location": location,
                        "price": price_val,
                        "area": area_val,
                    }
                )
                docs.append(doc)
                
        except Exception as e:
            print("Lỗi khi parse file Parquet:", file_path, e)

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
    
    ACTUAL_QDRANT_URL = "http://localhost:6333"

    client = QdrantClient(
        url=ACTUAL_QDRANT_URL,
        api_key=settings.QDRANT_API_KEY
    )

    # Reset collection
    try:
        client.delete_collection(collection_name=COLLECTION)
    except:
        pass

    QdrantVectorStore.from_documents(
        documents=chunks,
        embedding=emb,
        url=ACTUAL_QDRANT_URL,
        api_key=settings.QDRANT_API_KEY,
        collection_name=COLLECTION,
        batch_size=10,
    )

    print("Build index thành công")

if __name__ == "__main__":
    build_index()
