import sys
import os
from typing import Optional
from openai import OpenAI
from qdrant_client import QdrantClient
from qdrant_client.models import SearchRequest
from typing import cast
from openai.types.chat import ChatCompletionMessageParam

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.config import settings

from sentence_transformers import SentenceTransformer

# ── Config ────────────────────────────────────────────────────────────────────
MODEL         = settings.MODEL          # model chính để trả lời
EMBED_MODEL   = "text-embedding-3-small"  # hoặc đổi sang multilingual-e5 nếu cần
COLLECTION    = settings.QDRANT_COLLECTION
QDRANT_API_KEY = settings.QDRANT_API_KEY
TOP_K         = 5                       # số chunk retrieve từ Qdrant
os.environ["HF_HUB_DISABLE_SYMLINKS_WARNING"] = "1"

groq_client = OpenAI(
    base_url="https://api.groq.com/openai/v1",
    api_key=settings.GRSK,
)


# Load 1 lần khi khởi động
embed_model = SentenceTransformer("intfloat/multilingual-e5-small")

qdrant = QdrantClient(url=settings.QDRANT_URL, api_key=settings.QDRANT_API_KEY)


# ── Prompts ───────────────────────────────────────────────────────────────────

REWRITE_SYSTEM = """Bạn là trợ lý tối ưu hóa câu truy vấn cho hệ thống tìm kiếm FAQ.

Nhiệm vụ: Dựa vào lịch sử hội thoại và câu hỏi hiện tại, hãy viết lại câu hỏi thành
một câu truy vấn tìm kiếm ngữ nghĩa hoàn chỉnh, rõ ràng, độc lập (không cần context
hội thoại để hiểu).

Quy tắc:
- Giữ nguyên ngôn ngữ tiếng Việt
- Không thêm câu trả lời hay giải thích
- Giải quyết các đại từ mơ hồ ("cái đó", "nó", "vấn đề trên",...) thành nội dung cụ thể
- Nếu câu hỏi đã rõ ràng và độc lập → trả về nguyên văn
- Chỉ trả về câu truy vấn, không thêm gì khác"""

RAG_SYSTEM = """Bạn là trợ lý hỗ trợ khách hàng chuyên nghiệp, trả lời bằng tiếng Việt.

Nguyên tắc trả lời:
- Chỉ trả lời dựa trên CONTEXT được cung cấp bên dưới
- Nếu context không đủ thông tin → thành thật nói "Tôi chưa có thông tin về vấn đề này"
- Trả lời ngắn gọn, đúng trọng tâm, dễ hiểu
- Không bịa thêm thông tin ngoài context

CONTEXT:
{context}"""


# ── Step 1: Query Rewriting ───────────────────────────────────────────────────

def rewrite_query(query: str, chat_history: list[dict]) -> str:
    """
    Rewrite query dựa trên lịch sử hội thoại để tạo câu truy vấn độc lập.
    Chỉ lấy 6 message gần nhất để tránh token bloat.
    """
    # Chỉ lấy lịch sử gần đây, bỏ qua system message
    recent_history = [
        m for m in chat_history[-6:]
        if m["role"] in ("user", "assistant")
    ]

    # Nếu không có lịch sử → không cần rewrite
    if not recent_history:
        return query.strip()

    history_text = "\n".join(
        f"{'Người dùng' if m['role'] == 'user' else 'Trợ lý'}: {m['content']}"
        for m in recent_history
    )

    rewrite_prompt = f"""Lịch sử hội thoại:
{history_text}

Câu hỏi hiện tại: {query}

Viết lại câu hỏi thành truy vấn tìm kiếm độc lập:"""

    response = groq_client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": REWRITE_SYSTEM},
            {"role": "user",   "content": rewrite_prompt},
        ],
        temperature=0,      # cần deterministic cho rewriting
        max_tokens=150,
        stream=False,
    )

    rewritten = (response.choices[0].message.content or "").strip()
    print(f"[Rewrite] '{query}' → '{rewritten}'")  # debug, bỏ khi production
    return rewritten if rewritten else query.strip()



# ── Step 2: Embed + Retrieve từ Qdrant ───────────────────────────────────────

def embed_text(text: str) -> list[float]:
    """Tạo vector embedding cho query."""
    return embed_model.encode(
        f"query: {text}",
        normalize_embeddings=True
    ).tolist()


def retrieve_context(query_vector: list[float], top_k: int = TOP_K) -> list[str]:
    """Truy vấn Qdrant, trả về list các đoạn text liên quan."""
    results = qdrant.query_points(
        collection_name=COLLECTION,
        query=query_vector,
        limit=top_k,
        with_payload=True,
    ).points

    chunks = []
    for hit in results:
        if hit.payload:  # guard None check
            text = hit.payload.get("text") or hit.payload.get("content", "")
            if text:
                chunks.append(str(text))

    return chunks


# ── Step 3: Build RAG prompt + Call LLM ──────────────────────────────────────

def build_context(chunks: list[str]) -> str:
    """Ghép các chunk thành context block có đánh số."""
    if not chunks:
        return "Không tìm thấy thông tin liên quan."
    return "\n\n---\n\n".join(
        f"[{i+1}] {chunk}" for i, chunk in enumerate(chunks)
    )


def chat_with_rag(
    user_query: str,
    chat_history: list[dict],
    model: str = MODEL,
) -> tuple[str, list[dict]]:
    """
    Main function: nhận query + history → trả về (answer, updated_history).

    Args:
        user_query:   Câu hỏi mới nhất của user
        chat_history: Toàn bộ lịch sử dạng [{"role": "user/assistant", "content": "..."}]
        model:        Model Groq sẽ dùng

    Returns:
        (answer, updated_history) — cập nhật history để dùng cho lượt sau
    """
    # 1. Rewrite query dựa trên lịch sử
    rewritten_query = rewrite_query(user_query, chat_history)

    # 2. Embed query đã rewrite
    query_vector = embed_text(rewritten_query)

    # 3. Retrieve context từ Qdrant
    chunks = retrieve_context(query_vector)
    context = build_context(chunks)

    # 4. Build system prompt với context RAG
    system_msg = {
        "role": "system",
        "content": RAG_SYSTEM.format(context=context),
    }

    # 5. Gọi LLM với full history (để model hiểu mạch hội thoại khi trả lời)
    messages_to_send = cast(
        list[ChatCompletionMessageParam],
        [system_msg] + chat_history + [{"role": "user", "content": user_query}]
    )

    response = groq_client.chat.completions.create(
        model=model,
        messages=messages_to_send,
        temperature=0.3,
        stream=False,
    )
    answer = (response.choices[0].message.content or "Xin lỗi, tôi không thể trả lời câu hỏi này vào lúc này.").strip()

    # 6. Cập nhật history với cặp user-assistant mới nhất
    updated_history = chat_history + [
        {"role": "user",      "content": user_query},
        {"role": "assistant", "content": answer},
    ]

    return answer, updated_history


# ── CLI demo ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== RAG Chatbot (gõ 'exit' để thoát) ===\n")
    history: list[dict] = []

    while True:
        user_input = input("Bạn: ").strip()
        if user_input.lower() in ("exit", "quit", "thoát"):
            break
        if not user_input:
            continue

        answer, history = chat_with_rag(user_input, history)
        print(f"Bot: {answer}\n")