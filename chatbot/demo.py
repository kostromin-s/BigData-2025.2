import streamlit as st
from rag_bakend import MODEL, chat_with_rag


st.set_page_config(
    page_title="PropAI – Tư Vấn Bất Động Sản",
    page_icon="🏙️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ==================== CUSTOM CSS =====================
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&family=Playfair+Display:wght@400;500&display=swap');

/* ===== BASE ===== */
html, body, [data-testid="stAppViewContainer"] {
    background: #0f1115 !important;
    font-family: 'Inter', sans-serif;
    color: #e6e8eb;
}

/* subtle gradient */
[data-testid="stAppViewContainer"]::before {
    content: '';
    position: fixed;
    inset: 0;
    background:
        radial-gradient(circle at 10% 10%, rgba(120,140,180,0.08), transparent 60%),
        radial-gradient(circle at 90% 90%, rgba(80,120,160,0.08), transparent 60%);
    z-index: 0;
    pointer-events: none;
}

/* hide streamlit UI */
#MainMenu, footer, header { display: none !important; }

/* ===== SIDEBAR ===== */
[data-testid="stSidebar"] {
    background: rgba(20,22,28,0.95) !important;
    border-right: 1px solid rgba(255,255,255,0.05);
}
[data-testid="stSidebar"] * {
    color: #cbd5e1 !important;
}

/* logo */
.sidebar-logo {
    font-family: 'Playfair Display', serif;
    font-size: 1.8rem;
    text-align: center;
    color: #93c5fd !important;
}

/* ===== MAIN ===== */
.main > .block-container {
    max-width: 900px !important;
}

/* ===== HEADER ===== */
.page-title {
    font-family: 'Playfair Display', serif;
    font-size: 2.4rem;
    text-align: center;
    color: #f1f5f9;
}
.page-title span {
    color: #60a5fa;
}
.page-subtitle {
    text-align: center;
    font-size: 0.75rem;
    color: #94a3b8;
}

/* ===== CHAT BOX ===== */
.chat-wrapper {
    background: rgba(25,28,36,0.7);
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 16px;
    padding: 1.2rem;
}

/* ===== MESSAGE ===== */
[data-testid="stChatMessage"] {
    background: transparent !important;
}

/* user */
.stChatMessage:has([data-testid="chatAvatarIcon-user"]) .stChatMessageContent {
    background: linear-gradient(135deg, #3b82f6, #2563eb);
    color: #ffffff !important;
    border-radius: 14px 6px 14px 14px !important;
}

/* assistant */
.stChatMessage:has([data-testid="chatAvatarIcon-assistant"]) .stChatMessageContent {
    background: rgba(40,45,60,0.85);
    color: #e2e8f0 !important;
    border-radius: 6px 14px 14px 14px !important;
}

/* ===== TEXT ===== */
p, li {
    color: #e2e8f0;
    line-height: 1.6;
}
strong {
    color: #f8fafc;
}
code {
    background: rgba(255,255,255,0.08);
    padding: 2px 6px;
    border-radius: 4px;
}

/* ===== INPUT ===== */
[data-testid="stChatInput"] {
    background: rgba(30,34,45,0.9) !important;
    border: 1px solid rgba(255,255,255,0.1);
    border-radius: 12px;
}
[data-testid="stChatInput"] textarea {
    color: #11151A !important;
}
[data-testid="stChatInput"] button {
    background: #3b82f6 !important;
    color: white !important;
}

/* ===== STATUS ===== */
.status-bar {
    text-align: right;
    font-size: 0.7rem;
    color: #64748b;
}

/* ===== THINKING DOTS ===== */
@keyframes pulse {
    0%,100%{opacity:0.3}
    50%{opacity:1}
}
.thinking-dots span {
    width:6px;height:6px;
    background:#60a5fa;
    border-radius:50%;
    display:inline-block;
    margin-right:4px;
    animation:pulse 1.2s infinite;
}
</style>
""", unsafe_allow_html=True)

# ===================== SIDEBAR =====================
with st.sidebar:
    st.markdown('<div class="sidebar-logo">PropAI</div>', unsafe_allow_html=True)
    st.markdown('<div class="sidebar-tagline">Real Estate Intelligence</div>', unsafe_allow_html=True)
    st.markdown('<hr class="sidebar-divider">', unsafe_allow_html=True)

    st.markdown("""
    <div class="sidebar-card">
        <span class="sidebar-card-icon">🏙️</span>
        <span class="sidebar-card-label">Giá Thị Trường</span>
        <div class="sidebar-card-desc">Cập nhật giá khu vực, phân khúc</div>
    </div>
    <div class="sidebar-card">
        <span class="sidebar-card-icon">📊</span>
        <span class="sidebar-card-label">Phân Tích Đầu Tư</span>
        <div class="sidebar-card-desc">ROI, dòng tiền, sinh lời</div>
    </div>
    <div class="sidebar-card">
        <span class="sidebar-card-icon">⚖️</span>
        <span class="sidebar-card-label">Pháp Lý & Thủ Tục</span>
        <div class="sidebar-card-desc">Sổ đỏ, hợp đồng, thuế phí</div>
    </div>
    <div class="sidebar-card">
        <span class="sidebar-card-icon">🗺️</span>
        <span class="sidebar-card-label">Chọn Khu Vực</span>
        <div class="sidebar-card-desc">So sánh vị trí, tiềm năng</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<hr class="sidebar-divider">', unsafe_allow_html=True)

    st.markdown("""
    <div class="sidebar-stats">
        <div class="stat-item">
            <div class="stat-num">10K+</div>
            <div class="stat-label">Dữ liệu</div>
        </div>
        <div class="stat-item">
            <div class="stat-num">63</div>
            <div class="stat-label">Tỉnh/TP</div>
        </div>
        <div class="stat-item">
            <div class="stat-num">24/7</div>
            <div class="stat-label">Hỗ trợ</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<hr class="sidebar-divider">', unsafe_allow_html=True)
    if st.button("🗑️  Xoá lịch sử chat", use_container_width=True):
        st.session_state.messages = [
            {"role": "assistant", "content": "Cuộc trò chuyện đã được làm mới ✨ Tôi có thể giúp gì cho bạn?"}
        ]
        st.rerun()
    st.markdown('<p style="text-align:center;font-size:0.65rem;color:rgba(198,160,96,0.3);margin-top:1rem;">© 2025 PropAI · Powered by AI</p>', unsafe_allow_html=True)


# ===================== MAIN AREA =====================
st.markdown("""
<div class="page-header">
    <div class="page-title">Tư Vấn <span>Bất Động Sản</span></div>
    <div class="page-subtitle">AI-Powered Real Estate Advisory</div>
</div>
""", unsafe_allow_html=True)

# Status bar
st.markdown("""
<div class="status-bar">
    <div class="status-dot"></div> AI đang hoạt động
</div>
""", unsafe_allow_html=True)

# Session state
if "messages" not in st.session_state:
    st.session_state.messages = [
        {
            "role": "assistant",
            "content": "Chào mừng bạn đến với **PropAI** 👋\n\nTôi là trợ lý bất động sản thông minh, sẵn sàng tư vấn về:\n- 🏡 **Mua / bán nhà** — Giá cả, khu vực, thương lượng\n- 📈 **Đầu tư** — ROI, dòng tiền, phân tích thị trường\n- ⚖️ **Pháp lý** — Sổ đỏ, hợp đồng, thuế phí\n\nBạn đang quan tâm điều gì?",
        }
    ]

# Chat messages
st.markdown('<div class="chat-wrapper">', unsafe_allow_html=True)
for m in st.session_state.messages:
    with st.chat_message(m["role"]):
        st.markdown(m["content"])
st.markdown('</div>', unsafe_allow_html=True)

# Quick prompts (only shown at start)
if len(st.session_state.messages) <= 1:
    st.markdown("""
    <div class="quick-prompts">
        <div class="quick-chip" onclick="document.querySelector('textarea').value='Giá nhà tại Hà Nội hiện tại ra sao?'; document.querySelector('textarea').dispatchEvent(new Event('input', {bubbles:true}))">🏙️ Giá nhà Hà Nội</div>
        <div class="quick-chip">📊 Nên đầu tư chung cư hay đất nền?</div>
        <div class="quick-chip">⚖️ Thủ tục mua nhà lần đầu</div>
        <div class="quick-chip">🗺️ Khu vực nào tiềm năng nhất 2025?</div>
        <div class="quick-chip">💰 Vay mua nhà cần điều kiện gì?</div>
    </div>
    """, unsafe_allow_html=True)

# Input
# ✅ Sửa lại đoạn xử lý input trong demo.py
if user_q := st.chat_input("Hỏi về giá nhà, khu vực, đầu tư, pháp lý…"):

    st.session_state.messages.append({"role": "user", "content": user_q})
    with st.chat_message("user"):
        st.markdown(user_q)

    with st.chat_message("assistant"):
        placeholder = st.empty()
        placeholder.markdown("""
        <div class="thinking-dots">
            <span></span><span></span><span></span>
        </div>
        """, unsafe_allow_html=True)

        try:
            # ✅ Unpack tuple (answer, updated_history)
            # Chỉ truyền history không gồm message user vừa thêm
            history_without_last = st.session_state.messages[:-1]
            answer, _ = chat_with_rag(
                user_query=user_q,
                chat_history=history_without_last,
            )
            if not answer:
                answer = "⚠️ Không nhận được phản hồi từ model. Vui lòng thử lại."
            placeholder.markdown(answer)
        except Exception as e:
            answer = f"❌ Đã xảy ra lỗi: `{e}`"
            placeholder.markdown(answer)

    st.session_state.messages.append({"role": "assistant", "content": answer})