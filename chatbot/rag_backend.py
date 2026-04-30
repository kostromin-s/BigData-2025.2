
import os
from typing import List
from common.config import settings
from qdrant_client import QdrantClient
from langchain_qdrant import QdrantVectorStore
# Prefer the new langchain-huggingface package; fall back for compatibility
try:
    from langchain_huggingface import HuggingFaceEmbeddings  # type: ignore
    _EMB_DEPRECATION_MSG = None
except Exception:
    from langchain_community.embeddings import HuggingFaceEmbeddings  # type: ignore
    _EMB_DEPRECATION_MSG = (
        "Using deprecated HuggingFaceEmbeddings from langchain_community. "
        "Install and switch to 'langchain-huggingface' for future compatibility."
    )
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import PromptTemplate


# ================== CONFIG ==================
QDRANT_URL   = settings.QDRANT_URL
COLLECTION   = settings.COLLECTION_NAME
EMBED_MODEL  = settings.EMBED_MODEL
GENAI_MODEL  = settings.EMBED_MODEL  # Reuse same model for LLM; adjust if you want a different one
# ============================================


QA_PROMPT = PromptTemplate.from_template(
    """
Bạn là trợ lý pháp lý, chỉ được phép trả lời dựa trên NGỮ CẢNH dưới đây.

Nếu không tìm thấy thông tin, hãy nói: 
"Không có dữ liệu để kết luận trong kho văn bản hiện tại."

Câu hỏi:
{question}

--- NGỮ CẢNH ---
{context} 
----------------

Trả lời ngắn gọn, trích dẫn rõ điều khoản nếu có:
"""
)


# 1️⃣ LLM
def _get_llm() -> ChatGoogleGenerativeAI:
    # Read API key from environment variables (do not hardcode secrets)
    api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
    
    if not api_key:
        raise RuntimeError(
            "⚠️ Thiếu API key. Set GOOGLE_API_KEY hoặc GEMINI_API_KEY trong môi trường."
        )

    # ChatGoogleGenerativeAI reads the key from env, but pass explicitly for clarity
    llm = ChatGoogleGenerativeAI(
        model=GENAI_MODEL,
        temperature=0.2,
        max_output_tokens=1024,
        google_api_key=api_key,
    )
    return llm


# 2️⃣ Global store
_VECTOR_STORE = None


# 3️⃣ Load vector store TỪ client — KHÔNG xài from_existing_collection()
def get_vector_store() -> QdrantVectorStore:
    global _VECTOR_STORE

    if _VECTOR_STORE is None:
        # Notify once if running on deprecated embeddings
        if _EMB_DEPRECATION_MSG:
            print(_EMB_DEPRECATION_MSG)

        emb = HuggingFaceEmbeddings(model_name=EMBED_MODEL)

        client = QdrantClient(url=QDRANT_URL)

        _VECTOR_STORE = QdrantVectorStore(
            client=client,
            collection_name=COLLECTION,
            embedding=emb,
        )

    return _VECTOR_STORE


# 4️⃣ RAG main
def rag_answer(question: str, k: int = 5):

    store = get_vector_store()
    llm = _get_llm()

    docs = store.similarity_search(question, k=k)
    if not docs:
        return "Không tìm thấy thông tin trong cơ sở dữ liệu."

    context_list = []
    for i, d in enumerate(docs, start=1):
        src = d.metadata.get("source", "unknown")
        code = d.metadata.get("main_code", "")
        dt = d.metadata.get("issue_date", "")
        context_list.append(
            f"[Đoạn {i} — {src} — {code} — {dt}]\n{d.page_content}"
        )

    context = "\n\n".join(context_list)

    prompt = QA_PROMPT.format(
        question=question,
        context=context,
    )

    out = llm.invoke(prompt)
    return out.content


# 5️⃣ Free mode
def llm_answer(question: str):
    llm = _get_llm()
    out = llm.invoke(question)
    return out.content