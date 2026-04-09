import os
import re
from pathlib import Path

from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter

try:
    from langchain_huggingface import HuggingFaceEmbeddings
except:
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
        chunk_size=1500,
        chunk_overlap=200,
    )

    chunks = []

    for d in raw_docs:
        parts = splitter.split_documents([d])

        for i, part in enumerate(parts):
            md = dict(d.metadata)
            md["chunk_id"] = i

            chunks.append(
                Document(
                    page_content=part,
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