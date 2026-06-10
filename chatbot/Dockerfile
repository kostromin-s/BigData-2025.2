# Image cho chatbot RAG (UI Streamlit) + indexer.
# Dùng chung cho cả Deployment (streamlit) lẫn Job build_index (override command).
# Build context = project root:
#   minikube image build -t chatbot:latest -f chatbot/Dockerfile .
FROM python:3.10-slim

# torch CPU-only (nhẹ hơn RẤT nhiều so với bản CUDA mặc định)
RUN pip install --no-cache-dir --timeout 600 \
    --index-url https://download.pytorch.org/whl/cpu \
    torch==2.2.2

# Các lib còn lại qua mirror Aliyun (nhanh & ổn định ở VN).
# Đường Groq KHÔNG cần langchain -> chỉ 4 lib chính.
RUN pip install --no-cache-dir --timeout 300 --retries 10 \
    --index-url https://mirrors.aliyun.com/pypi/simple/ \
    --trusted-host mirrors.aliyun.com \
    streamlit==1.29.0 \
    "openai>=1.30,<2" \
    "qdrant-client>=1.9,<2" \
    sentence-transformers==3.0.1

# Cache sẵn model embedding vào image -> runtime KHỎI tải lại (470MB)
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('intfloat/multilingual-e5-small')"

# Code + dữ liệu (giữ nguyên cấu trúc để import 'common.config' và đường dẫn data đúng)
COPY common/ /app/common/
COPY chatbot/ /app/chatbot/
COPY crawler/data/all_raw_data.json /app/crawler/data/all_raw_data.json

WORKDIR /app/chatbot
EXPOSE 8501

# Mặc định chạy UI (dark theme set qua flag để khỏi phụ thuộc .streamlit/config.toml).
# Job index sẽ override bằng: command: ["python","build_index.py"]
CMD ["streamlit", "run", "demo.py", \
     "--server.port=8501", "--server.address=0.0.0.0", \
     "--theme.base=dark", \
     "--theme.backgroundColor=#0f1115", \
     "--theme.secondaryBackgroundColor=#14161c", \
     "--theme.primaryColor=#60a5fa", \
     "--theme.textColor=#e6e8eb"]
