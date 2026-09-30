# Hướng Dẫn & Thiết Kế: Nginx Load Balancer, Failover & Observability Cho Chatbot-HCMUS

Tài liệu này cung cấp toàn bộ thiết kế, file cấu hình và hướng dẫn vận hành hệ thống **Multi-instance Backend (2 Port khác nhau)** được điều phối bởi **Nginx Load Balancer** với cơ chế **High Availability (Failover)** và đảm bảo **Tracing, Monitoring, Logging** hoạt động chính xác.

---

## 1. Sơ Đồ Kiến Trúc Hệ Thống (Architecture Overview)

```
                            [ Client (Browser / Next.js) ]
                                          │
                     HTTP / Pure WebSocket (port 8080 / 80)
                                          ▼
                         ┌─────────────────────────────────┐
                         │   NGINX REVERSE PROXY & LB      │
                         │    (Port 8080 / 80 - Gateway)   │
                         └────────────────┬────────────────┘
                                          │
                 ┌────────────────────────┴────────────────────────┐
                 │ Failover: proxy_next_upstream                   │
                 ▼                                                 ▼
   ┌───────────────────────────┐                     ┌───────────────────────────┐
   │    Backend Instance 1     │                     │    Backend Instance 2     │
   │        (Port 5001)        │                     │        (Port 5002)        │
   │  - Express REST API       │                     │  - Express REST API       │
   │  - Socket.IO Server       │                     │  - Socket.IO Server       │
   │  - OpenTelemetry (Tracer) │                     │  - OpenTelemetry (Tracer) │
   │  - Prometheus (/metrics)  │                     │  - Prometheus (/metrics)  │
   │  - Pino Logger            │                     │  - Pino Logger            │
   └─────────────┬─────────────┘                     └─────────────┬─────────────┘
                 │                                                 │
                 └────────────────────────┬────────────────────────┘
                                          ▼
                   ┌──────────────────────────────────────────────┐
                   │               SHARED INFRASTRUCTURE          │
                   │  - Redis: Socket.IO Adapter, Rate Limit      │
                   │  - BullMQ: 1 Worker Process độc lập          │
                   │  - MongoDB Atlas & Elasticsearch             │
                   │  - Jaeger (4318) & Prometheus Scraper        │
                   └──────────────────────────────────────────────┘
```

---

## 2. Các Yêu Cầu Kỹ Thuật Cốt Lõi

1. **Điều phối tải (Load Balancing)**: Nginx lắng nghe ở cổng chính (`8080` ở dev và `80`/`8080` ở prod), phân phối traffic sang `5001` và `5002` bằng giải thuật `ip_hash` (đảm bảo tính nhất quán cho WebSocket và session).
2. **Cơ chế chịu lỗi (Failover)**: Khi 1 instance bị crash/tắt, Nginx phát hiện ngay lập tức và chuyển hướng toàn bộ request sang instance còn lại mà **không trả lỗi 502/504 cho người dùng**.
3. **Pure WebSocket Support**: Vì Frontend đã được cấu hình `transports: ["websocket"]`, Nginx chỉ cần nâng cấp kết nối một lần (`Upgrade: websocket`) và duy trì kết nối TCP song công. Nhờ có `@socket.io/redis-adapter`, tin nhắn được broadcast đồng bộ qua lại giữa cả 2 instances.
4. **Bảo toàn Observability**:
   - **OpenTelemetry Tracing**: Phân biệt rõ trace phát sinh từ instance nào thông qua `service.instance.id` (gắn theo `PORT`).
   - **Pino Logging**: Bổ sung `port` vào log metadata để dễ dàng lọc và tìm kiếm.
   - **Prometheus Monitoring**: Tránh hiện tượng nhảy số liệu (Flapping Metrics). Prometheus phải scrape độc lập từng instance (`5001/metrics` và `5002/metrics`) thay vì scrape qua Load Balancer.

---

## 3. Cấu Hình Nginx Hoàn Chỉnh (`nginx.conf`)

Tạo file cấu hình tại thư mục `nginx/nginx.conf`:

```nginx
# nginx/nginx.conf
worker_processes auto;

events {
    worker_connections 1024;
}

http {
    include       mime.types;
    default_type  application/octet-stream;
    sendfile        on;
    keepalive_timeout  65;

    # Định dạng log Nginx ghi nhận chi tiết thời gian và backend xử lý
    log_format upstream_logging '[$time_local] $remote_addr - $remote_user "$request" '
                                'status=$status upstream_addr=$upstream_addr '
                                'upstream_response_time=$upstream_response_time request_time=$request_time';

    access_log /var/log/nginx/access.log upstream_logging;
    error_log  /var/log/nginx/error.log warn;

    # 1. Khai báo cụm Backend Instances
    upstream backend_cluster {
        # least_conn: Chuyển request đến server đang có ít active connection nhất
        least_conn;

        # max_fails=1 fail_timeout=5s: Nếu 1 lần không kết nối được, coi server đó offline trong 5 giây
        server 127.0.0.1:5001 max_fails=1 fail_timeout=5s;
        server 127.0.0.1:5002 max_fails=1 fail_timeout=5s;

        # Giữ kết nối keepalive giữa Nginx và backend Node.js để tăng hiệu năng
        keepalive 32;
    }

    server {
        # Port chính mà Client / Frontend gọi vào
        listen 5000;
        server_name localhost;

        # Tăng kích thước tối đa của body (upload video / ảnh)
        client_max_body_size 50M;

        # 2. Xử lý WebSocket Realtime (Socket.IO)
        location /socket.io/ {
            proxy_pass http://backend_cluster;

            # Nâng cấp kết nối HTTP lên WebSocket thuần
            proxy_http_version 1.1;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection "upgrade";

            # Chuyển tiếp các thông tin gốc của client
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;

            # Giữ kết nối socket mở liên tục (tránh Nginx tự ngắt sau 60s)
            proxy_read_timeout 86400s;
            proxy_send_timeout 86400s;
        }

        # 3. Xử lý REST API và các Route thường
        location / {
            proxy_pass http://backend_cluster;
            proxy_http_version 1.1;

            # Headers chuẩn cho Reverse Proxy
            proxy_set_header Connection "";
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;

            # Header để Client kiểm tra backend nào vừa xử lý request (Hữu ích khi debug)
            add_header X-Handled-By $upstream_addr always;

            # 4. Cơ chế Failover tự động (High Availability)
            # Khi 1 backend bị lỗi mạng, timeout hoặc crash (502, 503, 504), Nginx tự bẻ sang backend kia
            proxy_next_upstream error timeout http_502 http_503 http_504 non_idempotent;
            proxy_next_upstream_tries 2;
            proxy_next_upstream_timeout 3s;
            proxy_connect_timeout 1s;
            proxy_read_timeout 15s;
        }

        # 5. Routing riêng cho Prometheus Scrape từng Instance (Tránh Flapping Metrics)
        location /metrics/instance-1 {
            proxy_pass http://127.0.0.1:5001/metrics;
        }

        location /metrics/instance-2 {
            proxy_pass http://127.0.0.1:5002/metrics;
        }
    }
}
```

---

## 4. Các Điều Chỉnh Trong Backend Code

Để 2 instance chạy mượt mà, phân biệt được log, trace và metrics, backend cần có các điểm tinh chỉnh sau:

### 4.1. Nhận biến `PORT` động từ dòng lệnh
Trong file `backend/src/config/config.ts` (và `env.schema.ts`), đảm bảo `config.port` ưu tiên đọc từ `process.env.PORT`:
```typescript
this.port = process.env.PORT ? parseInt(process.env.PORT, 10) : parsedEnv.PORT;
```

### 4.2. Gắn nhãn Instance cho OpenTelemetry Tracing (`tracer.ts`)
Trong file `backend/src/infrastructure/monitoring/tracer.ts`, bổ sung thuộc tính tài nguyên:
```typescript
import { Resource } from "@opentelemetry/resources";
import { SemanticResourceAttributes } from "@opentelemetry/semantic-conventions";

const sdk = new NodeSDK({
    serviceName: "chatbot-backend",
    resource: new Resource({
        [SemanticResourceAttributes.SERVICE_NAME]: "chatbot-backend",
        [SemanticResourceAttributes.SERVICE_INSTANCE_ID]: `backend-port-${process.env.PORT || "5000"}`,
    }),
    traceExporter,
    instrumentations: [...],
});
```
* **Lợi ích**: Khi vào Jaeger UI (`http://localhost:16686`), bạn có thể filter theo tag `service.instance.id` để xem trace đó được thực thi bởi port 5001 hay 5002.

### 4.3. Gắn thông tin Port vào Pino Logging (`logging.ts`)
Trong file `backend/src/infrastructure/monitoring/logging.ts`:
```typescript
export const logger = pino({
    level: process.env.LOG_LEVEL || "info",
    timestamp: pino.stdTimeFunctions.isoTime,
    base: {
        instance_port: process.env.PORT || "5000",
    },
});
```
* **Lợi ích**: Mọi dòng log JSON xuất ra đều có thêm trường `"instance_port": "5001"` hoặc `"5002"`, kèm `trace_id` đồng nhất.

### 4.4. Cấu hình Prometheus Scraper (`prometheus.yml`)
Trong file cấu hình Prometheus, cấu hình cào độc lập cả 2 instances:
```yaml
scrape_configs:
  - job_name: 'chatbot-backend'
    scrape_interval: 10s
    static_configs:
      - targets: ['host.docker.internal:5001']
        labels:
          instance: 'backend-instance-1'
          port: '5001'
      - targets: ['host.docker.internal:5002']
        labels:
          instance: 'backend-instance-2'
          port: '5002'
```
* **Lợi ích**: Prometheus lưu trữ 2 chuỗi thời gian tách biệt cho 2 tiến trình, đồ thị RAM/CPU trên Grafana vẽ được 2 đường song song của 2 server, không bị giật cục.

---

## 5. Hướng Dẫn Vận Hành & Khởi Chạy Từng Bước

### Bước 1: Khởi chạy 2 Backend Instances & 1 Worker
Mở 3 cửa sổ Terminal trong thư mục `backend`:

* **Terminal 1 (Backend Instance 1 - Port 5001)**:
  ```bash
  PORT=5001 npm run dev:server
  ```
* **Terminal 2 (Backend Instance 2 - Port 5002)**:
  ```bash
  PORT=5002 npm run dev:server
  ```
* **Terminal 3 (Worker BullMQ - Không mở cổng HTTP)**:
  ```bash
  npm run dev:worker
  ```

> [!NOTE]
> Chỉ cần chạy **1 tiến trình Worker**, cả 2 instances 5001 và 5002 sẽ cùng đẩy job (lưu tin nhắn, upload media, sync ES) vào hàng đợi Redis `fastQueue` và `mediaQueue`.

### Bước 2: Khởi chạy Nginx
* **Cách 1 (Nginx cài trên máy Mac qua Homebrew)**:
  ```bash
  # Copy cấu hình vào thư mục nginx của homebrew
  cp nginx/nginx.conf /opt/homebrew/etc/nginx/nginx.conf
  # Khởi động lại nginx
  nginx -s reload || brew services restart nginx
  ```
* **Cách 2 (Nginx qua Docker)**:
  ```bash
  docker run -d --name nginx-lb -p 8080:8080 \
    -v $(pwd)/nginx/nginx.conf:/etc/nginx/nginx.conf:ro \
    nginx:alpine
  ```

---

## 6. Kịch Bản Kiểm Thử Chi Tiết (Test Scenarios)

### Kịch bản 1: Kiểm tra Load Balancing chia tải
Thực hiện 4 request liên tiếp vào cổng Nginx `http://localhost:8080/health`:
```bash
for i in {1..4}; do curl -i -s http://localhost:8080/health | grep -E "(HTTP|X-Handled-By)"; done
```
* **Kết quả kỳ vọng**: Header `X-Handled-By` sẽ hiển thị instance xử lý (nhất quán theo client IP nhờ `ip_hash`).

---

### Kịch bản 2: Kiểm tra Failover khi 1 port chết
1. Giả lập port 5001 bị sập bằng cách dừng container backend-1 (`docker stop chatbot-backend-1-prod`).
2. Gửi ngay request đến cổng 8080:
   ```bash
   curl -i http://localhost:8080/health
   ```
* **Kết quả kỳ vọng**:
  - Request vẫn trả về `HTTP/1.1 200 OK` ngay lập tức (không có độ trễ hay lỗi 502).
  - Header `X-Handled-By` hiển thị instance còn lại (`5002`).
  - 100% request tiếp theo đều được chuyển an toàn sang port 5002.

---

### Kịch bản 3: Kiểm tra Tự Phục Hồi (Auto-Recovery)
1. Bật lại container backend-1 (`docker start chatbot-backend-1-prod`).
2. Chờ 5 giây (hết chu kỳ `fail_timeout=5s`).
3. Gửi tiếp các request đến cổng 8080:
* **Kết quả kỳ vọng**: Nginx tự động phát hiện port 5001 đã sống lại và khôi phục phục vụ trở lại.

---

### Kịch bản 4: Kiểm tra WebSocket Realtime khi 1 port chết
1. Mở trình duyệt kết nối vào app Chat (kết nối Socket qua `localhost:8080`).
2. Mở tab chat và gửi tin nhắn bình thường.
3. Tắt backend instance mà client đang kết nối tới.
* **Kết quả kỳ vọng**:
  - Client Socket.IO kích hoạt sự kiện `disconnect` và lập tức tự động reconnect (`autoConnect: true`).
  - Nginx bẻ kết nối mới sang backend còn lại.
  - Sau khi kết nối lại, người dùng tiếp tục gửi tin nhắn bình thường; nhờ có `@socket.io/redis-adapter`, người dùng ở instance 1 và 2 đều nhận được tin nhắn tức thời.

---

### Kịch bản 5: Kiểm tra Tracing & Metrics
1. Truy cập Jaeger UI (`http://localhost:16686`):
   - Mở Service `chatbot-backend`, xem các Spans.
   - Thấy rõ tag `service.instance.id` mang giá trị `backend-port-5001` hoặc `backend-port-5002`.
2. Truy cập Prometheus Scraper:
   - `http://localhost:8080/metrics/instance-1` $\rightarrow$ ra đúng metric của port 5001.
   - `http://localhost:8080/metrics/instance-2` $\rightarrow$ ra đúng metric của port 5002.

---

## 7. Hướng Dẫn Vận Hành Bằng Docker Compose (Dev & Production)

Dự án đã được trang bị đầy đủ các tệp cấu hình Docker chuyên biệt:
* Multi-stage Dockerfile: [backend/Dockerfile](file:///Users/ductri0981/Documents/Chatbot-HCMUS/backend/Dockerfile) và [frontend/Dockerfile](file:///Users/ductri0981/Documents/Chatbot-HCMUS/frontend/Dockerfile)
* Cấu hình Nginx: [nginx/nginx.dev.conf](file:///Users/ductri0981/Documents/Chatbot-HCMUS/nginx/nginx.dev.conf) và [nginx/nginx.prod.conf](file:///Users/ductri0981/Documents/Chatbot-HCMUS/nginx/nginx.prod.conf)
* Docker Compose: [docker-compose.dev.yml](file:///Users/ductri0981/Documents/Chatbot-HCMUS/docker-compose.dev.yml) và [docker-compose.prod.yml](file:///Users/ductri0981/Documents/Chatbot-HCMUS/docker-compose.prod.yml)

### 7.1. Chạy Môi Trường Development (Có Hot-Reload)
Tự động khởi chạy Redis, Elasticsearch, Backend 1 (5001), Backend 2 (5002), BullMQ Worker, Nginx Gateway (8080), và Frontend (3000) với bind mount code:

```bash
# Khởi chạy toàn bộ hệ thống dev
docker compose -f docker-compose.dev.yml up --build

# Hoặc chạy ngầm dưới nền:
docker compose -f docker-compose.dev.yml up -d

# Dừng hệ thống dev
docker compose -f docker-compose.dev.yml down
```

### 7.2. Chạy Môi Trường Production Đóng Gói Hoàn Chỉnh
Build toàn bộ thành immutable images, Next.js chạy Standalone Runner (~120MB), Backend chạy non-root user, Nginx làm Gateway chính tại cổng 80 & 8080:

```bash
# Build và chạy production cluster
docker compose -f docker-compose.prod.yml up --build -d

# Xem logs của toàn bộ cụm:
docker compose -f docker-compose.prod.yml logs -f

# Dừng cụm production:
docker compose -f docker-compose.prod.yml down
```

