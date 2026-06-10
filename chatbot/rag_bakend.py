import sys
import os
from typing import Optional
from openai import OpenAI
from qdrant_client import QdrantClient
from qdrant_client.models import SearchRequest
from typing import cast
from openai.types.chat import ChatCompletionMessageParam

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from chatbot.common.config import settings

from sentence_transformers import SentenceTransformer

# ── Config ────────────────────────────────────────────────────────────────────
MODEL          = settings.MODEL
EMBED_MODEL    = "text-embedding-3-small"
COLLECTION     = settings.QDRANT_COLLECTION
QDRANT_API_KEY = settings.QDRANT_API_KEY
TOP_K          = 5
os.environ["HF_HUB_DISABLE_SYMLINKS_WARNING"] = "1"

groq_client = OpenAI(
    base_url="https://api.groq.com/openai/v1",
    api_key=settings.GRSK,
)

embed_model = SentenceTransformer("intfloat/multilingual-e5-base")

qdrant = QdrantClient(
    url=settings.QDRANT_URL,
    api_key=settings.QDRANT_API_KEY,
    check_compatibility=False,
)


# ── Prompts ───────────────────────────────────────────────────────────────────

REWRITE_SYSTEM = """Bạn là chuyên gia tối ưu hóa truy vấn cho hệ thống tìm kiếm bất động sản Việt Nam.

Nhiệm vụ: Dựa vào lịch sử hội thoại và câu hỏi hiện tại, viết lại thành câu truy vấn
tìm kiếm ngữ nghĩa hoàn chỉnh, độc lập, phù hợp với domain bất động sản.

Quy tắc:
- Giữ nguyên tiếng Việt
- Giải quyết đại từ mơ hồ ("chỗ đó", "căn đó", "giá kia",...) thành nội dung cụ thể
- Bổ sung từ khóa domain nếu ngữ cảnh rõ ràng:
    + Loại BĐS: phòng trọ, căn hộ, chung cư, nhà phố, mặt bằng, đất nền, villa,...
    + Giao dịch: cho thuê, mua bán, sang nhượng, đặt cọc,...
    + Thông số: diện tích (m²), giá (triệu/tháng, tỷ), nội thất, tầng, hướng,...
    + Vị trí: quận, huyện, phường, đường, khu vực,...
- Nếu câu hỏi đã đủ rõ → trả về nguyên văn
- Chỉ trả về câu truy vấn, không giải thích thêm"""

RAG_SYSTEM = """Bạn là PropAI — chuyên gia tư vấn bất động sản hàng đầu Việt Nam, \
có kiến thức sâu rộng về thị trường mua bán, cho thuê, đầu tư bất động sản.

## Phong cách tư vấn
- Chuyên nghiệp, thân thiện, dễ hiểu — như một môi giới BĐS giàu kinh nghiệm
- Trả lời có cấu trúc rõ ràng khi cần (dùng bullet, số liệu cụ thể)
- Chủ động gợi ý thêm thông tin hữu ích liên quan nếu có trong context
- Dùng đơn vị Việt Nam: triệu/tháng, tỷ đồng, m², sào, hecta,...

## Phạm vi tư vấn
Bạn có thể tư vấn về:
- 🏠 Thuê / Mua nhà & căn hộ — giá cả, vị trí, so sánh các lựa chọn
- 🏪 Mặt bằng kinh doanh — diện tích, mặt tiền, khu vực phù hợp
- 🏗️ Đầu tư BĐS — phân tích lợi nhuận, rủi ro, tiềm năng khu vực
- 📋 Pháp lý & thủ tục — sổ đỏ, hợp đồng, thuế phí, đặt cọc
- 📊 Thị trường — xu hướng giá, so sánh khu vực, phân khúc

## Nguyên tắc trả lời
- Ưu tiên thông tin từ CONTEXT bên dưới — đây là dữ liệu thực tế từ thị trường
- Khi context có listing phù hợp: trình bày rõ địa chỉ, giá, diện tích, đặc điểm nổi bật
- Khi context không đủ: thành thật nói "Hiện tôi chưa có dữ liệu phù hợp với yêu cầu này"
  và gợi ý người dùng cung cấp thêm thông tin (khu vực, ngân sách, diện tích mong muốn)
- Không bịa số liệu, không suy đoán giá ngoài context
- Cuối câu trả lời, nếu phù hợp, hãy hỏi thêm 1 câu để hiểu rõ hơn nhu cầu khách hàng

## CONTEXT (dữ liệu thị trường thực tế):
{context}"""


# ── Step 1: Query Rewriting ───────────────────────────────────────────────────

def rewrite_query(query: str, chat_history: list[dict]) -> str:
    recent_history = [
        m for m in chat_history[-6:]
        if m["role"] in ("user", "assistant")
    ]

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
        messages=cast(list[ChatCompletionMessageParam], [
            {"role": "system", "content": REWRITE_SYSTEM},
            {"role": "user",   "content": rewrite_prompt},
        ]),
        temperature=0,
        max_tokens=150,
        stream=False,
    )

    rewritten = (response.choices[0].message.content or "").strip()
    print(f"[Rewrite] '{query}' → '{rewritten}'")
    return rewritten if rewritten else query.strip()


# ── Step 2: Embed + Retrieve từ Qdrant ───────────────────────────────────────

def embed_text(text: str) -> list[float]:
    return embed_model.encode(
        f"query: {text}",
        normalize_embeddings=True
    ).tolist()


def retrieve_context(query_vector: list[float], top_k: int = TOP_K) -> list[str]:
    results = qdrant.query_points(
        collection_name=COLLECTION,
        query=query_vector,
        limit=top_k,
        with_payload=True,
    ).points

    chunks = []
    for hit in results:
        if hit.payload:
            text = hit.payload.get("text") or hit.payload.get("content", "")
            if text:
                chunks.append(str(text))

    return chunks


# ── Step 3: Build RAG prompt + Call LLM ──────────────────────────────────────

def build_context(chunks: list[str]) -> str:
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
    # 1. Rewrite query
    rewritten_query = rewrite_query(user_query, chat_history)

    # 2. Embed
    query_vector = embed_text(rewritten_query)

    # 3. Retrieve
    chunks = retrieve_context(query_vector)
    context = build_context(chunks)

    # 4. Build messages
    system_msg: ChatCompletionMessageParam = {
        "role": "system",
        "content": RAG_SYSTEM.format(context=context),
    }

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

    updated_history = chat_history + [
        {"role": "user",      "content": user_query},
        {"role": "assistant", "content": answer},
    ]

    return answer, updated_history


# ── CLI demo ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== PropAI – Tư Vấn Bất Động Sản (gõ 'exit' để thoát) ===\n")
    history: list[dict] = []

    while True:
        user_input = input("Bạn: ").strip()
        if user_input.lower() in ("exit", "quit", "thoát"):
            break
        if not user_input:
            continue

        answer, history = chat_with_rag(user_input, history)
        print(f"PropAI: {answer}\n")