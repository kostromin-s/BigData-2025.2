import os
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
EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
# ==========================================


def load_docs():
    """
    Load toàn bộ file text/html trong thư mục data.

    Version 1:
    - Đọc file thô
    - Không parse metadata
    - Chỉ lấy content
    """
    docs = []

    root = Path(DATA_DIR)

    for file_path in root.rglob("*.*"):
        try:
            text = file_path.read_text(encoding="utf-8", errors="ignore")

            doc = Document(
                page_content=text,
                metadata={
                    "source": str(file_path.name)
                }
            )
            docs.append(doc)

        except Exception as e:
            print("Lỗi đọc file:", file_path, e)

    return docs


def chunk_docs(raw_docs):
    """
    Chia nhỏ document thành các chunk.

    Version 1:
    - chunk đơn giản
    - không quan tâm semantic
    """
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=2000,
        chunk_overlap=200,
    )

    chunks = []

    for d in raw_docs:
        parts = splitter.split_text(d.page_content)

        for i, part in enumerate(parts):
            chunks.append(
                Document(
                    page_content=part,
                    metadata=d.metadata
                )
            )

    return chunks


def build_index():
    """
    Pipeline build index:

    1. Load document
    2. Chunk
    3. Embedding
    4. Push vào Qdrant
    """

    raw_docs = load_docs()
    chunks = chunk_docs(raw_docs)

    print("Docs:", len(raw_docs))
    print("Chunks:", len(chunks))

    emb = HuggingFaceEmbeddings(model_name=EMBED_MODEL)

    client = QdrantClient(url=QDRANT_URL)

    # Xóa collection cũ nếu tồn tại
    try:
        client.delete_collection(collection_name=COLLECTION)
    except:
        pass

    # Insert dữ liệu
    QdrantVectorStore.from_documents(
        documents=chunks,
        embedding=emb,
        url=QDRANT_URL,
        collection_name=COLLECTION,
    )

    print("Index done")


if __name__ == "__main__":
    build_index()