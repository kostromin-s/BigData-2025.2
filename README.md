# BigData-2025.2 — Nền tảng Phân tích Bất động sản Hà Nội (trên Kubernetes)

Hệ thống Big Data end-to-end: thu thập dữ liệu bất động sản từ Chợ Tốt → xử lý streaming →
lưu trữ phân tán → **dashboard phân tích** + **chatbot RAG tư vấn**. Toàn bộ chạy trên
**Kubernetes (Minikube)**.

---

## 1. Kiến trúc

```
                              ┌─────────────── KUBERNETES (namespace: bigdata) ───────────────┐
Chợ Tốt API                   │                                                               │
   │ crawler/crawl.py         │   Kafka ──► Spark Streaming ──► HDFS ──► Streamlit Dashboard  │
   ▼ (chạy ở host)            │   (KRaft)   (consumer)         (Parquet)   (cổng 30501)        │
crawler/data/all_raw_data.json│      ▲                                                         │
   │ kafka-loader (Job)  ─────┼──────┘                                                         │
   │                          │                                                               │
   └─ chatbot/build_index ───►│   Qdrant ◄── Chatbot RAG (PropAI) ──► Groq LLM (API ngoài)     │
      (Job, embed e5-small)   │  (vector)    (cổng 30502)                                      │
                              └───────────────────────────────────────────────────────────────┘
```

| Thành phần | Vai trò | Cổng (NodePort) |
|---|---|---|
| HDFS (NameNode/DataNode) | Lưu Parquet | 30870 (UI) |
| Kafka (KRaft) | Message queue | — |
| AKHQ | Kafka UI | 30080 |
| Spark Consumer | Kafka → HDFS | 30404 (UI) |
| Streamlit Dashboard | Phân tích, biểu đồ, bản đồ | 30501 |
| Qdrant | Vector DB cho chatbot | 30333 |
| Chatbot (PropAI) | RAG tư vấn BĐS | 30502 |

---

## 2. Yêu cầu cài đặt

- **Docker Desktop** (đang chạy, Linux containers) — cấp ≥ 6GB RAM trong Settings → Resources
- **Minikube**: `winget install Kubernetes.minikube`
- **kubectl**: `winget install Kubernetes.kubectl`
- **Python 3.10+** (để chạy crawler) + `pip install requests`
- **(Chatbot)** Một **Groq API key** miễn phí: https://console.groq.com

---

## 3. Chạy nhanh — 1 lệnh

Từ thư mục gốc của repo (PowerShell):

```powershell
# Dựng TRỌN BỘ (hạ tầng + dashboard + crawl data + chatbot)
.\k8s\deploy.ps1 -GroqKey gsk_xxxxxxxx
```

- **Không có Groq key?** Bỏ `-GroqKey` → script dựng mọi thứ trừ chatbot:
  ```powershell
  .\k8s\deploy.ps1
  ```
- Lần đầu sẽ **lâu** (tải image + torch cho chatbot). Các lần sau nhanh hơn nhờ cache.

`deploy.ps1` tự động làm:
1. Kiểm tra Docker/Minikube/kubectl, khởi động Minikube
2. Build + nạp các image (spark-consumer, dashboard) + pre-load image hạ tầng
3. Deploy HDFS → Kafka → Qdrant → Spark → Dashboard
4. Tạo topic Kafka `real-estate-documents`
5. Crawl dữ liệu (nếu chưa có) + build loader + đẩy ~300 tin vào Kafka
6. (Nếu có key) build chatbot + tạo Secret + index Qdrant + deploy chatbot

**Tham số:**
| Tham số | Ý nghĩa |
|---|---|
| `-GroqKey <key>` | Key Groq cho chatbot (hoặc đặt biến môi trường `$env:GRSK`) |
| `-Reset` | Xóa namespace `bigdata` rồi deploy lại từ đầu |
| `-SkipData` | Bỏ qua crawl + đẩy data |
| `-SkipChatbot` | Bỏ qua chatbot |

---

## 4. Truy cập dịch vụ

Trên Windows + Docker driver, **dùng `minikube service`** (tạo tunnel, tự mở browser):

```powershell
minikube service streamlit-dashboard -n bigdata   # Dashboard
minikube service chatbot -n bigdata               # Chatbot PropAI
minikube service namenode-ui -n bigdata           # HDFS UI
minikube service akhq -n bigdata                  # Kafka UI
```

> URL hiện ra dạng `127.0.0.1:<port>` là **bình thường** — đó là đầu local của tunnel
> vào pod trong cluster, không phải app chạy trên máy.

Kiểm tra trạng thái: `kubectl get pods -n bigdata`

---

## 5. Lưu ý cho người mới pull repo về

Một số thứ **KHÔNG có trong git** (do `.gitignore`), bạn phải tự lo:

| Thứ | Vì sao | Cách lấy |
|---|---|---|
| **Docker images** | Git chỉ có Dockerfile | `deploy.ps1` tự build |
| **Dữ liệu crawl** (`crawler/data/*.json`) | `*.json` bị ignore | `deploy.ps1` tự chạy crawler (cần internet) |
| **Groq API key** | Không bao giờ commit key | Lấy free ở console.groq.com, truyền qua `-GroqKey` |
| **`common/config.py`** | Đang bị ignore (chứa cấu hình chatbot) | Đã env-driven, không có key — repo nên **bỏ ignore** để team có sẵn |

> ⚠️ **Bảo mật:** Không commit key Groq/Qdrant vào repo. Key chatbot được đưa vào qua
> K8s Secret (`chatbot-secret`), không nằm trong file nào.

---

## 6. Xử lý sự cố (Troubleshooting)

| Triệu chứng | Nguyên nhân & cách xử lý |
|---|---|
| Pod `ContainerCreating` rất lâu | Đang pull image lần đầu — chờ, hoặc `deploy.ps1` đã pre-load sẵn |
| `kafka-0` CrashLoopBackOff, exit 1 | Thiếu `enableServiceLinks: false` (đã có trong manifest) |
| `spark-consumer` CrashLoop, `UnknownTopicOrPartition` | Topic chưa tạo → `deploy.ps1` bước [6] đã lo; tạo tay nếu cần |
| Dashboard tiếng Việt thành `???` | Pipe PowerShell làm hỏng UTF-8 — `deploy.ps1` tạo ConfigMap trực tiếp (đã fix) |
| Sửa code nhưng Minikube chạy bản cũ | `minikube image load` không ghi đè tag trùng → dùng `minikube image build` (đã áp dụng) |
| Chatbot lỗi "wrong vector dimension" | Embedding index ≠ query — cả 2 phải là `e5-small` (384) |

---

## 7. Cấu trúc thư mục

```
crawler/        Thu thập dữ liệu Chợ Tốt (crawl.py)
kafka/          Producer + Dockerfile.loader (đẩy data vào Kafka)
spark/          Spark Streaming consumer (Kafka → HDFS) + Dockerfile
pyspark/        Dashboard Streamlit (dashboard.py) + Dockerfile
chatbot/        RAG chatbot: demo.py (UI), rag_bakend.py (Groq), build_index.py + Dockerfile
common/         config.py (cấu hình chatbot, đọc từ env)
k8s/            Toàn bộ manifest Kubernetes + deploy.ps1
  ├─ hdfs/ kafka/ qdrant/ spark/ dashboard/ loader/ chatbot/
  └─ deploy.ps1   ← script dựng trọn bộ
```

---

## 8. Công nghệ

Kafka 7.5 (KRaft) · Spark Structured Streaming · Hadoop HDFS (Parquet) · Qdrant ·
sentence-transformers (e5-small) · Groq (Llama 3.1) · Streamlit · Plotly · Kubernetes (Minikube)
