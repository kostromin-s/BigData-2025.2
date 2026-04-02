import os
from typing import List

from qdrant_client import QdrantClient
from langchain_qdrant import QdrantVectorStore

from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_google_genai import ChatGoogleGenerativeAI


# ================= CONFIG =================
QDRANT_URL = "http://localhost:6333"
COLLECTION = "real_estate"
EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
GENAI_MODEL = "gemini-2.5-flash-lite"
# ==========================================


# Hàm khởi tạo LLM (Gemini)
def get_llm():
    """
    Khởi tạo model LLM dùng để generate câu trả lời.

    Yêu cầu:
    - Phải có API key trong biến môi trường GOOGLE_API_KEY hoặc GEMINI_API_KEY

    Trả về:
    - Instance của ChatGoogleGenerativeAI
    """
    api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")

    if not api_key:
        raise RuntimeError("Thiếu API key")

    return ChatGoogleGenerativeAI(
        model=GENAI_MODEL,
        temperature=0.2,
        google_api_key=api_key,
    )


# Hàm load vector store từ Qdrant
def get_vector_store():
    """
    Kết nối tới Qdrant và load collection chứa dữ liệu bất động sản.

    Trả về:
    - QdrantVectorStore dùng để search embedding
    """
    emb = HuggingFaceEmbeddings(model_name=EMBED_MODEL)
    client = QdrantClient(url=QDRANT_URL)

    store = QdrantVectorStore(
        client=client,
        collection_name=COLLECTION,
        embedding=emb,
    )
    return store


# Hàm chính: hỏi đáp bằng RAG
def rag_answer(question: str, k: int = 5) -> str:
    """
    Pipeline RAG đơn giản:

    1. Nhận câu hỏi
    2. Search top-k document tương tự
    3. Ghép context
    4. Gửi vào LLM để trả lời

    Tham số:
    - question: câu hỏi người dùng
    - k: số lượng document lấy ra

    Trả về:
    - Chuỗi câu trả lời
    """

    store = get_vector_store()
    llm = get_llm()

    # Bước 1: tìm document tương tự
    docs = store.similarity_search(question, k=k)

    if not docs:
        return "Không tìm thấy dữ liệu"

    # Bước 2: ghép context
    context = ""
    for i, d in enumerate(docs, start=1):
        context += f"[Doc {i}]\n{d.page_content}\n\n"

    # Bước 3: prompt đơn giản
    prompt = f"""
Bạn là trợ lý bất động sản.

Câu hỏi:
{question}

Dữ liệu:
{context}

Trả lời:
"""

    # Bước 4: gọi LLM
    result = llm.invoke(prompt)

    return result.content