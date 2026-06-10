# deploy.ps1 — Script deploy toàn bộ BigData-2025.2 lên Minikube
# Chạy từ thư mục gốc: .\k8s\deploy.ps1

param(
    [switch]$Reset  # Dùng -Reset để xóa và deploy lại từ đầu
)

$ROOT = Split-Path -Parent $PSScriptRoot
Set-Location $ROOT

Write-Host "=== BigData-2025.2 Kubernetes Deployment ===" -ForegroundColor Cyan

# ── Kiểm tra prerequisites ────────────────────────────────────────────────────
function Assert-Command($name, $installHint) {
    if (-not (Get-Command $name -ErrorAction SilentlyContinue)) {
        Write-Host "[MISSING] '$name' chua duoc cai." -ForegroundColor Red
        Write-Host "  Cai bang: $installHint" -ForegroundColor Yellow
        exit 1
    }
}

Write-Host "`n[0/6] Kiem tra prerequisites..." -ForegroundColor Yellow
Assert-Command "minikube" "winget install Kubernetes.minikube"
Assert-Command "kubectl"  "winget install Kubernetes.kubectl"
Assert-Command "docker"   "Cai Docker Desktop: https://www.docker.com/products/docker-desktop"

# Kiểm tra Docker daemon đang chạy
& docker ps | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Docker Desktop chua chay. Hay khoi dong Docker Desktop roi thu lai." -ForegroundColor Red
    exit 1
}
Write-Host "  Docker: OK" -ForegroundColor Green

# ── Bước 0: Khởi động Minikube ───────────────────────────────────────────────
$status = & minikube status --format='{{.Host}}' 2>$null
if ($status -ne "Running") {
    # Dùng RAM của Docker Desktop (không phải RAM hệ thống)
    $dockerMemBytes = [long](& docker info --format '{{.MemTotal}}')
    $dockerMemMB = [int]($dockerMemBytes / 1MB)
    $minikubeMem = [int]($dockerMemMB * 0.80)
    Write-Host "  Docker Desktop RAM: ${dockerMemMB}MB -> Cap cho Minikube: ${minikubeMem}MB" -ForegroundColor Gray
    Write-Host "  Khoi dong Minikube (driver=docker)..." -ForegroundColor Yellow

    & minikube start --driver=docker --cpus=4 --memory=$minikubeMem --disk-size=30g
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ERROR] Minikube khong khoi dong duoc. Xem log o tren de biet nguyen nhan." -ForegroundColor Red
        exit 1
    }
} else {
    Write-Host "  Minikube: Running" -ForegroundColor Green
}

# ── Xóa namespace cũ nếu dùng -Reset ─────────────────────────────────────────
if ($Reset) {
    Write-Host "`n[Reset] Xoa namespace bigdata..." -ForegroundColor Red
    & kubectl delete namespace bigdata --ignore-not-found
    Start-Sleep -Seconds 10
}

# ── Bước 1: Build images trên Docker Desktop → load vào Minikube ─────────────
# Build trên Docker Desktop thường (network nhanh hơn Minikube daemon)
# Sau đó dùng minikube image load để đưa vào cluster
Write-Host "`n[1/6] Build Docker images..." -ForegroundColor Yellow

Write-Host "  Building spark-consumer:latest (Docker Desktop)..." -ForegroundColor Gray
docker build -t spark-consumer:latest -f spark/Dockerfile.consumer .
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Build spark-consumer that bai." -ForegroundColor Red; exit 1
}

Write-Host "  Building streamlit-dashboard:latest (Docker Desktop)..." -ForegroundColor Gray
docker build -t streamlit-dashboard:latest -f pyspark/Dockerfile.dashboard ./pyspark
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Build streamlit-dashboard that bai." -ForegroundColor Red; exit 1
}

Write-Host "  Loading spark-consumer vao Minikube..." -ForegroundColor Gray
& minikube image load spark-consumer:latest
Write-Host "  Loading streamlit-dashboard vao Minikube..." -ForegroundColor Gray
& minikube image load streamlit-dashboard:latest

# Pre-load image ha tang tu Docker Desktop local -> Minikube.
# Minikube co image store rieng, neu khong load thi no se pull lai tu Docker Hub (rat cham).
$infraImages = @(
    "bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8",
    "bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8",
    "confluentinc/cp-kafka:7.5.0",
    "qdrant/qdrant:latest",
    "tchiotludo/akhq:latest"
)
foreach ($img in $infraImages) {
    # Neu chua co trong Docker Desktop thi pull ve truoc
    & docker image inspect $img *> $null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "  Pulling $img (chua co local)..." -ForegroundColor Gray
        & docker pull $img
    }
    Write-Host "  Loading $img vao Minikube..." -ForegroundColor Gray
    & minikube image load $img
}
Write-Host "  Images OK" -ForegroundColor Green

# ── Bước 2: Tạo Namespace ─────────────────────────────────────────────────────
Write-Host "`n[2/6] Tao Namespace..." -ForegroundColor Yellow
& kubectl apply -f k8s/00-namespace.yaml

# ── Bước 3: Deploy HDFS ───────────────────────────────────────────────────────
Write-Host "`n[3/6] Deploy HDFS..." -ForegroundColor Yellow
& kubectl apply -f k8s/hdfs/
Write-Host "  Doi NameNode san sang (co the mat vai phut)..."
& kubectl wait --for=condition=ready pod -l app=namenode -n bigdata --timeout=600s

# ── Bước 4: Deploy Kafka ──────────────────────────────────────────────────────
Write-Host "`n[4/6] Deploy Kafka..." -ForegroundColor Yellow
& kubectl apply -f k8s/kafka/
Write-Host "  Doi Kafka san sang..."
& kubectl wait --for=condition=ready pod -l app=kafka -n bigdata --timeout=600s

# ── Bước 5: ConfigMap + Qdrant + Spark + Dashboard ───────────────────────────
Write-Host "`n[5/6] Deploy Qdrant, Spark Consumer, Dashboard..." -ForegroundColor Yellow

# ConfigMap phải tạo TRƯỚC khi dashboard deployment khởi động.
# KHÔNG dùng "... -o yaml | kubectl apply -f -": pipe của PowerShell re-encode luồng
# UTF-8 làm HỎNG tiếng Việt thành "?". Tạo trực tiếp (--from-file đọc bytes nguyên vẹn).
Write-Host "  Tao ConfigMap dashboard-source..."
& kubectl delete configmap dashboard-source -n bigdata --ignore-not-found
& kubectl create configmap dashboard-source `
    --from-file=dashboard.py=pyspark/dashboard.py `
    --from-file=config.py=pyspark/config.py `
    -n bigdata

& kubectl apply -f k8s/qdrant/
& kubectl apply -f k8s/spark/
& kubectl apply -f k8s/dashboard/

# ── Hiển thị kết quả ──────────────────────────────────────────────────────────
Write-Host "`n=== Trang thai cac Pod ===" -ForegroundColor Cyan
& kubectl get pods -n bigdata

Write-Host "`n=== URL truy cap (dung sau khi pod Ready) ===" -ForegroundColor Cyan
$IP = & minikube ip
Write-Host "  HDFS NameNode UI   : http://${IP}:30870"
Write-Host "  Kafka AKHQ UI      : http://${IP}:30080"
Write-Host "  Spark UI           : http://${IP}:30404"
Write-Host "  Streamlit Dashboard: http://${IP}:30501"
Write-Host "  Qdrant API         : http://${IP}:30333"

Write-Host "`n[Done] Deployment hoan tat!" -ForegroundColor Green
