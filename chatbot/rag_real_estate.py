import os
from typing import List

from qdrant_client import QdrantClient
from langchain_qdrant import QdrantVectorStore

try:
    from langchain_huggingface import HuggingFaceEmbeddings
except:
    from langchain_community.embeddings import HuggingFaceEmbeddings

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import PromptTemplate


# ================= CONFIG =================
QDRANT_URL = "http://localhost:6333"
COLLECTION = "real_estate"
EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
GENAI_MODEL = "gemini-2.5-flash-lite"
# ==========================================


# Prompt chuẩn cho bất động sản
QA_PROMPT = PromptTemplate.from_template(
"""
Bạn là chuyên gia bất động sản.

Chỉ được trả lời dựa trên dữ liệu cung cấp.
Nếu không đủ dữ liệu, hãy nói rõ: "Không có dữ liệu phù hợp".

Yêu cầu:
- Trả lời rõ ràng
- Nếu có giá, vị trí, pháp lý thì phải nêu
- Không bịa thông tin

Câu hỏi:
{question}

--- DỮ LIỆU ---
{context}
----------------

Trả lời:
"""
)


# Cache vector store (tránh load lại nhiều lần)
_VECTOR_STORE = None


def get_llm():
    """
    Khởi tạo LLM với cấu hình ổn định hơn.

    Cải tiến:
    - Giảm temperature để hạn chế bịa
    - Giới hạn output
    """
    api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")

    if not api_key:
        raise RuntimeError("Thiếu API key")

    return ChatGoogleGenerativeAI(
        model=GENAI_MODEL,
        temperature=0.1,
        max_output_tokens=1024,
        google_api_key=api_key,
    )


def get_vector_store():
    """
    Load vector store nhưng có cache để tăng performance.

    Cải tiến:
    - Không load lại mỗi lần query
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
    Xây dựng context có structure rõ ràng.

    Cải tiến:
    - Có metadata: source, price, location
    - Dễ đọc hơn cho LLM
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


def rag_answer(question: str, k: int = 5) -> str:
    """
    Pipeline RAG cải tiến:

    1. Retrieve document
    2. Build context có metadata
    3. Prompt chuẩn hóa
    4. Generate answer

    Cải tiến:
    - Context rõ ràng hơn
    - Prompt hạn chế hallucination
    """

    store = get_vector_store()
    llm = get_llm()

    # Step 1: search
    docs = store.similarity_search(question, k=k)

    if not docs:
        return "Không tìm thấy dữ liệu"

    # Step 2: build context
    context = build_context(docs)

    # Step 3: format prompt
    prompt = QA_PROMPT.format(
        question=question,
        context=context
    )

    # Step 4: generate
    result = llm.invoke(prompt)

    return result.content


def rag_answer_with_filter(question: str, location: str = None, k: int = 5) -> str:
    """
    Version nâng cao: có filter theo location.

    Ví dụ:
    - chỉ tìm nhà ở Quận 9
    - chỉ tìm dự án ở Hà Nội

    Tham số:
    - location: lọc theo metadata location
    """

    store = get_vector_store()
    llm = get_llm()

    # Filter theo metadata (Qdrant hỗ trợ)
    if location:
        docs = store.similarity_search(
            question,
            k=k,
            filter={"must": [{"key": "location", "match": {"value": location}}]}
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