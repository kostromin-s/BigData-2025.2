import os
from typing import List

from qdrant_client import QdrantClient
from langchain_qdrant import QdrantVectorStore

try:
    from langchain_huggingface import HuggingFaceEmbeddings
except:
    from langchain_community.embeddings import HuggingFaceEmbeddings

from langchain_core.prompts import PromptTemplate

# Llama local qua Ollama
from langchain_community.chat_models import ChatOllama


# ================= CONFIG =================
QDRANT_URL = "http://localhost:6333"
COLLECTION = "real_estate"
EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
OLLAMA_MODEL = "llama3.2:3b"
# ==========================================


# Prompt tối ưu cho bất động sản
QA_PROMPT = PromptTemplate.from_template("""
Bạn là chuyên gia bất động sản.

QUY TẮC:
- Chỉ dùng dữ liệu được cung cấp
- Không suy diễn ngoài dữ liệu
- Nếu không có → nói rõ "Không có dữ liệu phù hợp"

YÊU CẦU:
- Trả lời có cấu trúc rõ ràng
- Trích dẫn thông tin quan trọng (giá, diện tích, vị trí)
- Nếu có nhiều lựa chọn → so sánh và đề xuất cái tốt nhất

Câu hỏi:
{question}

--- DỮ LIỆU ---
{context}
----------------

Trả lời:
""")


# Cache vector store
_VECTOR_STORE = None


def get_llm():
    """
    Khởi tạo Llama local thông qua Ollama.

    Yêu cầu:
    - Ollama đang chạy (ollama serve)
    - Model đã được pull (ollama pull llama3)

    Cải tiến:
    - Không cần API key
    - Chạy hoàn toàn local
    """
    llm = ChatOllama(
        model=OLLAMA_MODEL,
        temperature=0.1,     # giảm hallucination
    )
    return llm


def get_vector_store():
    """
    Load vector store từ Qdrant.

    Có cache để tránh load lại nhiều lần.
    """
    global _VECTOR_STORE

    if _VECTOR_STORE is None:
        emb = HuggingFaceEmbeddings(model_name=EMBED_MODEL)
        client = QdrantClient(url=QDRANT_URL)

        _VECTOR_STORE = QdrantVectorStore(
            client=client,
            collection_name=COLLECTION,
            embedding=emb,
        )

    return _VECTOR_STORE


def build_context(docs) -> str:
    """
    Xây dựng context có cấu trúc rõ ràng cho LLM.

    Bao gồm metadata:
    - source
    - location
    - price
    - area
    """
    context_parts = []

    for i, d in enumerate(docs, start=1):
        md = d.metadata

        source = md.get("source", "unknown")
        location = md.get("location", "")
        price = md.get("price", "")
        area = md.get("area", "")

        block = f"""
[Document {i}]
Source: {source}
Location: {location}
Price: {price}
Area: {area}

Content:
{d.page_content}
"""
        context_parts.append(block)

    return "\n".join(context_parts)


def retrieve_docs(question: str, k: int = 5):
    """
    Bước retrieve:
    - Tìm top-k document liên quan

    Tách riêng function để dễ mở rộng (rerank sau này)
    """
    store = get_vector_store()
    docs = store.similarity_search(question, k=k)
    return docs


def rag_answer(question: str, k: int = 5) -> str:
    """
    Pipeline RAG hoàn chỉnh (local Llama):

    1. Retrieve documents
    2. Build context
    3. Format prompt
    4. Generate bằng Llama local

    Ưu điểm:
    - Không phụ thuộc API
    - Chạy offline
    """

    llm = get_llm()

    docs = retrieve_docs(question, k=k)

    if not docs:
        return "Không tìm thấy dữ liệu"

    context = build_context(docs)

    prompt = QA_PROMPT.format(
        question=question,
        context=context
    )

    result = llm.invoke(prompt)

    return result.content


def rag_answer_with_filter(
    question: str,
    location: str = None,
    max_price: float = None,
    k: int = 5
) -> str:
    """
    Version nâng cao có filter theo metadata.

    Hỗ trợ:
    - location
    - price (cơ bản)

    Lưu ý:
    - Qdrant filter cần metadata chuẩn từ lúc index
    """

    store = get_vector_store()
    llm = get_llm()

    filter_conditions = []

    if location:
        filter_conditions.append({
            "key": "location",
            "match": {"value": location}
        })

    # price filter (giả sử lưu dạng số)
    if max_price:
        filter_conditions.append({
            "key": "price",
            "range": {"lte": max_price}
        })

    if filter_conditions:
        docs = store.similarity_search(
            question,
            k=k,
            filter={"must": filter_conditions}
        )
    else:
        docs = store.similarity_search(question, k=k)

    if not docs:
        return "Không tìm thấy dữ liệu phù hợp"

    context = build_context(docs)

    prompt = QA_PROMPT.format(
        question=question,
        context=context
    )

    result = llm.invoke(prompt)

    return result.content