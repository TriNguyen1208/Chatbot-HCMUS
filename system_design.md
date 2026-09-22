# TÀI LIỆU THIẾT KẾ HỆ THỐNG CHUẨN PRODUCTION (SYSTEM DESIGN DOCUMENT)
Dự án: **Chatbot-HCMUS (High-Scale Realtime Chat System)**  
Môi trường: **Production Ready & High Availability**  
Tác giả: Architecture Team

---

## MỤC LỤC
1. [Tổng quan Kiến trúc Hiện tại & Baseline](#1-tổng-quan-kiến-trúc-hiện-tại--baseline)
2. [Đánh giá Bộ tính năng & Định hướng Tập trung](#2-đánh-giá-bộ-tính-năng--định-hướng-tập-trung)
3. [Sơ đồ Kiến trúc Tổng thể (High-Level System Architecture)](#3-sơ-đồ-kiến-trúc-tổng-thể-high-level-system-architecture)
4. [Quản lý Đa luồng & Phân tách Tiến trình (PM2 Cluster & Fork Mode)](#4-quản-lý-đa-luồng--phân-tách-tiến-trình-pm2-cluster--fork-mode)
5. [Kiến trúc Tính bất biến & Chống trùng lặp (Distributed Idempotency)](#5-kiến-trúc-tính-bất-biến--chống-trùng-lặp-distributed-idempotency)
6. [Bảo toàn Thứ tự Tin nhắn & Bù gói tin (Message Ordering & Gap Detection)](#6-bảo-toàn-thứ-tự-tin-nhắn--bù-gói-tin-message-ordering--gap-detection)
7. [Quản lý Vòng đời & Triển khai Không Gián đoạn (Graceful Shutdown)](#7-quản-lý-vòng-đời--triển-khai-không-gián-đoạn-graceful-shutdown)
8. [Kiểm soát Tải & Chống Spam Phân tán (Distributed Rate Limiting)](#8-kiểm-soát-tải--chống-spam-phân-tán-distributed-rate-limiting)
9. [Hệ thống Giám sát & Truy vết (Observability & Distributed Tracing)](#9-hệ-thống-giám-sát--truy-vết-observability--distributed-tracing)
10. [Đóng gói Hạ tầng & Môi trường Chạy (Containerization & Deployment)](#10-đóng-gói-hạ-tầng--môi-trường-chạy-containerization--deployment)
11. [Lộ trình Triển khai Chi tiết (Implementation Roadmap)](#11-lộ-trình-triển-khai-chi-tiết-implementation-roadmap)

---

## 1. Tổng quan Kiến trúc Hiện tại & Baseline

### 1.1. Công nghệ Lõi (Core Tech Stack)
* **Backend:** Node.js (v20+), Express.js (v5), TypeScript (ESM modules `#@/*`).
* **Realtime:** Socket.IO (v4.8) kết hợp `@socket.io/redis-adapter` hỗ trợ distributed WebSocket broadcasting.
* **Databases & Storage:**
  * **MongoDB Atlas:** Lưu trữ bền vững (Persistent Storage), Generic Repository Pattern.
  * **Redis (ioredis):** RAM Cache (Metadata, Recent messages, Presence, Watermarks), Pub/Sub Adapter.
  * **Elasticsearch (v8):** Full-text search tin nhắn, người dùng và cuộc trò chuyện.
  * **Cloudflare R2 (S3-compatible):** Lưu trữ media (Ảnh, Video chunked stream).
* **Background Queue:** **BullMQ (v5)** trên nền Redis, phân thành 3 queues chuyên biệt: `fastQueue`, `mediaQueue`, `cronQueue`.
* **Frontend:** Next.js 16 (App Router, Turbopack), React 19 (React Compiler enabled), Zustand 5, TanStack Query v5, Tailwind CSS v4, Singleton Socket Core.

### 1.2. Mẫu thiết kế đã chuẩn hóa (Established Patterns)
* **Backend:** Modular Monolith, Clean Architecture. Các module độc lập (`auth`, `user`, `conversation`, `message`, `media`, `search`), tương tác liên module bắt buộc qua `Facade` (`*.facade.ts`), Write-Behind Caching.
* **Frontend:** Fractal Component Tree (`[Component].tsx` là Presenter, `use[Component].ts` là Headless Hook/Controller).

---

## 2. Đánh giá Bộ tính năng & Định hướng Tập trung

### 2.1. Đánh giá Tính năng
Hệ thống hiện tại đã sở hữu đầy đủ 100% các tính năng của một ứng dụng nhắn tin thương mại hiện đại:
* **Nhắn tin đa phương tiện:** Text, Image, Video (kèm chuyển mã HLS và tạo thumbnail ngầm), Link Preview.
* **Giao tiếp Realtime:** Direct Chat (1-1), Group Chat, Typing/Stop typing, Watermark (Đã nhận / Đã đọc), Online/Offline Presence.
* **Vòng đời tin nhắn:** Sửa tin nhắn (trong 1h + lưu `edit_history`), Thu hồi tin nhắn (Recall), Thả cảm xúc (Reactions), Chuyển tiếp (Forward).
* **Quản trị phòng chat:** Phân quyền Admin, Thêm/Kích thành viên, Rời nhóm, Chuyển quyền, Giải tán nhóm, Chặn/Bỏ chặn.
* **Tìm kiếm:** Full-text search tốc độ cao qua Elasticsearch cluster.

### 2.2. Quyết định Định hướng Kỹ thuật
> **DỪNG PHÁT TRIỂN TÍNH NĂNG MỚI. TẬP TRUNG 100% NGUỒN LỰC VÀO SYSTEM DESIGN VÀ TỐI ƯU HẠ TẦNG SẢN XUẤT (PRODUCTION READINESS).**

---

## 3. Sơ đồ Kiến trúc Tổng thể (High-Level System Architecture)

```
                                [ CLIENT APPLICATIONS ]
                        (Next.js Web / Mobile - Pure WebSocket)
                                           │
                                           ▼ (Sticky / TCP Passthrough)
                               [ REVERSE PROXY / NGINX ]
                                           │
         ┌─────────────────────────────────┴─────────────────────────────────┐
         ▼                                                                   ▼
┌─────────────────────────────────┐                         ┌─────────────────────────────────┐
│       SERVER NODE 1             │                         │       SERVER NODE 2             │
│  ┌───────────────────────────┐  │                         │  ┌───────────────────────────┐  │
│  │   PM2 Cluster Mode        │  │                         │  │   PM2 Cluster Mode        │  │
│  │ ┌───────────┐┌───────────┐│  │                         │  │ ┌───────────┐┌───────────┐│  │
│  │ │Worker (1) ││Worker (2) ││  │                         │  │ │Worker (1) ││Worker (2) ││  │
│  │ │(HTTP/WS)  ││(HTTP/WS)  ││  │                         │  │ │(HTTP/WS)  ││(HTTP/WS)  ││  │
│  │ └─────┬─────┘└─────┬─────┘│  │                         │  │ └─────┬─────┘└─────┬─────┘│  │
│  └───────┼────────────┼──────┘  │                         │  └───────┼────────────┼──────┘  │
│          └──────┬─────┘         │                         │          └──────┬─────┘         │
│                 ▼               │                         │                 ▼               │
│  ┌───────────────────────────┐  │                         │  ┌───────────────────────────┐  │
│  │   PM2 Fork Mode (Worker)  │  │                         │  │   PM2 Fork Mode (Worker)  │  │
│  │   chatbot-worker          │  │                         │  │   chatbot-worker          │  │
│  │   (FFmpeg / Cron / Media) │  │                         │  │   (FFmpeg / Cron / Media) │  │
│  └──────────────┬────────────┘  │                         │  └──────────────┬────────────┘  │
└─────────────────┼───────────────┘                         └─────────────────┼───────────────┘
                  │                                                           │
                  └─────────────────────────────┬─────────────────────────────┘
                                                ▼
         ┌────────────────────────────────────────────────────────────────────────┐
         │                    DISTRIBUTED INFRASTRUCTURE LAYER                    │
         ├────────────────────────────────────────────────────────────────────────┤
         │ • Redis Pub/Sub Adapter (Cross-cluster WebSocket broadcast)            │
         │ • Redis Atomic Lock (Idempotency Key: SET NX EX)                       │
         │ • Redis Atomic Sequence Generator (INCR conv:{id}:seq)                 │
         │ • Redis Distributed Rate Limiter (Token Bucket / Sliding Window)       │
         │ • BullMQ Queues (FAST, MEDIA, CRON with Isolated Redis Connections)   │
         │ • MongoDB Atlas (Compound Unique Indexes, Write-behind Persistence)    │
         │ • Elasticsearch Cluster (Full-text Search Sync Engine)                 │
         │ • Cloudflare R2 (Object Storage for Direct Media Uploads)              │
         └────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Quản lý Đa luồng & Phân tách Tiến trình (PM2 Cluster & Fork Mode)

### 4.1. Vấn đề của Kiến trúc Hiện tại
* Hiện tại `backend/src/index.ts` đang import trực tiếp toàn bộ BullMQ workers (`fastWorker`, `mediaWorker`, `cronWorker`) vào cùng một tiến trình chạy Express và Socket.IO.
* **Hậu quả:** Node.js chạy single-thread Event Loop. Khi `mediaWorker` thực hiện encode video bằng FFmpeg hoặc nén ảnh nặng bằng Sharp, CPU bị đẩy lên 100% $\rightarrow$ **Event Loop bị nghẽn (Blocked) $\rightarrow$ Socket.IO không thể gửi/nhận ping-pong heartbeat $\rightarrow$ Toàn bộ client bị rớt kết nối đồng loạt (Connection Flapping)**.

### 4.2. Giải pháp Kiến trúc: Tách Process theo Vai trò
Tách ứng dụng thành 2 entry point độc lập:
1. `src/index.ts`: Chỉ phục vụ HTTP API và Socket.IO Gateway.
2. `src/worker.ts`: Chuyên dụng tiêu thụ hàng đợi BullMQ (Fast, Media, Cron).

### 4.3. Cấu hình PM2 Ecosystem (`ecosystem.config.cjs`)
```javascript
module.exports = {
  apps: [
    // 1. API & REALTIME SERVER: Chạy Cluster Mode tận dụng toàn bộ nhân CPU
    {
      name: "chatbot-api",
      script: "./dist/index.js",
      instances: "max",           // Tự động scale theo số core CPU của server
      exec_mode: "cluster",
      wait_ready: true,           // Đợi process.send('ready') mới bắt đầu route request
      listen_timeout: 10000,
      kill_timeout: 8000,         // Dành 8 giây cho Graceful Shutdown
      env: {
        NODE_ENV: "production",
        PROCESS_TYPE: "web",
        PORT: 5000
      }
    },

    // 2. BACKGROUND QUEUE WORKER: Chạy Fork Mode cách ly hoàn toàn CPU
    {
      name: "chatbot-worker",
      script: "./dist/worker.js",
      instances: 1,               // Cấp phát 1 hoặc 2 worker tuỳ dung lượng CPU
      exec_mode: "fork",
      kill_timeout: 15000,        // Dành 15 giây cho worker hoàn thành nốt job nặng dở dang
      env: {
        NODE_ENV: "production",
        PROCESS_TYPE: "worker"
      }
    }
  ]
};
```

### 4.4. Quy tắc Sống còn cho Socket.IO trên PM2 Cluster
1. **Ép buộc Pure WebSocket Transport ở Client:**
   ```typescript
   // frontend/src/shared/services/socket.service.ts
   this.socket = io(apiUrl, {
       transports: ["websocket"], // Bắt buộc, loại bỏ hoàn toàn polling
       upgrade: false,
       withCredentials: true
   });
   ```
   *Lý do:* Nếu dùng HTTP Long-Polling mặc định, request handshake (GET) và request truyền dữ liệu (POST) có thể bị PM2 master phân bổ vào 2 worker khác nhau $\rightarrow$ sinh lỗi `400 Session ID Unknown`.
2. **Adapter đồng bộ trạng thái:** Toàn bộ các worker trong cluster trao đổi thông tin phòng chat và sự kiện thông qua `@socket.io/redis-adapter`.

---

## 5. Kiến trúc Tính bất biến & Chống trùng lặp (Distributed Idempotency)

Trong môi trường phân tán và mạng không ổn định (4G/Wifi chập chờn), Client timeout hoặc retry sẽ tạo ra các **Duplicate Requests**. Hệ thống phải áp dụng Idempotency 4 tầng:

```
[CLIENT]
   │ (Gửi tin kèm client_msg_id: UUIDv4)
   ▼
[TẦNG 1: REDIS ATOMIC LOCK]
   ├── SET idemp:msg:{client_msg_id} "PROCESSING" EX 120 NX
   │     ├── Nếu KEY ĐÃ TỒN TẠI ──> Trả ngay ACK cũ từ Redis cache (KHÔNG lưu DB, KHÔNG emit lại)
   │     └── Nếu KEY CHƯA TỒN TẠI ─> Tiếp tục xử lý
   ▼
[TẦNG 2: PERSISTENCE & CACHE]
   ├── Ghi Redis conv:{id}:recent
   ├── Emit Socket vào room O(1)
   └── Set msg:result:{client_msg_id} = messageData (EX 120s)
   ▼
[TẦNG 3: DATABASE SAFETY NET]
   └── MongoDB Compound Unique Index: { conversation_id: 1, client_msg_id: 1 } (E11000 Protection)
   ▼
[TẦNG 4: BULLMQ DEDUPLICATION]
   └── BullMQ JobId: deterministic jobId (e.g. `sync_wm:{convId}:{userId}`)
```

### 5.1. Tầng Client $\rightarrow$ Socket Server
* Frontend sinh mã định danh duy nhất `client_msg_id` (UUIDv4) ngay khi người dùng nhấn gửi tin nhắn.
* Client gửi `client_msg_id` trong payload socket `new_message`.
* Backend kiểm tra nguyên tử trên Redis bằng lệnh **`SET NX EX`**:
  ```typescript
  const idempotencyKey = `idemp:msg:${payload.client_msg_id}`;
  const isAcquired = await redis.set(idempotencyKey, "PROCESSING", "EX", 120, "NX");

  if (!isAcquired) {
      // Tin nhắn này đã được gửi hoặc đang xử lý dở do mạng chập chờn
      const cached = await redis.get(`msg:result:${payload.client_msg_id}`);
      if (cached) {
          return ack?.({ success: true, data: JSON.parse(cached) });
      }
      return ack?.({ success: true, message: "Message is being processed" });
  }
  ```

### 5.2. Tầng Database (Phòng tuyến cuối)
Khai báo Compound Unique Index trong MongoDB:
```typescript
// backend/src/infrastructure/database/init-models.ts
MessageSchema.index({ conversation_id: 1, client_msg_id: 1 }, { unique: true, sparse: true });
```
Nếu xảy ra race condition vượt qua tầng Redis, Database sẽ từ chối với mã lỗi `E11000` Duplicate Key thay vì lưu 2 bản ghi trùng nhau.

### 5.3. Tầng Background Queue (Job Deduplication)
Tránh việc sinh hàng nghìn job trùng lặp trong Redis Queue khi một sự kiện xảy ra liên tục (như đánh dấu đã đọc hoặc sync Elasticsearch):
```typescript
// Chỉ định deterministic jobId
await queueService.addJob('sync_watermark', payload, {
    jobId: `sync_wm:${conversationId}:${userId}` // BullMQ tự loại bỏ job trùng nếu job cũ chưa xong
});
```

### 5.4. Tầng State: Monotonic Watermark (Chống thoái lùi trạng thái Đã đọc)
* **Nguy cơ:** User đọc tin nhắn `#100`. Do độ trễ mạng, gói ACK của tin `#95` vô tình tới sau. Nếu ghi đè thẳng `#95`, trạng thái đọc bị thụt lùi.
* **Giải pháp:** Sử dụng MongoDB Conditional Update có điều kiện so sánh mốc thời gian:
  ```typescript
  await conversationModel.updateOne(
      {
          _id: conversationId,
          "watermarks.user_id": userId,
          "watermarks.read_message_time": { $lt: incomingMessageTime }
      },
      {
          $set: {
              "watermarks.$.read_message_id": messageId,
              "watermarks.$.read_message_time": incomingMessageTime
          }
      }
  );
  ```

### 5.5. Cơ chế Client Retry & Optimistic UI (Hàng đợi gửi lại ở Frontend)
* **Vấn đề hiện tại ở Frontend:** 
  * Khi bấm gửi, `useChatInput.ts` chạy `setContent("")` xóa sạch ô nhập.
  * Nếu quá 5s không nhận được ACK (timeout hoặc rớt mạng 4G), hàm `emitWithAck` ném lỗi nhưng `useChatInput.ts` chỉ `console.error` rồi bỏ qua.
  * Hậu quả: Tin nhắn biến mất khỏi màn hình, người dùng không biết tin đã gửi hay chưa, không có nút "Gửi lại" (Retry), buộc phải gõ lại từ đầu cả đoạn văn bản.
* **Giải pháp Kiến trúc Chuẩn (Optimistic UI + Retry Queue):**
  1. **Optimistic Rendering:** Khi ấn Gửi, sinh ngay `client_msg_id` (UUIDv4) và chèn tin nhắn vào danh sách chat ở trạng thái `status: 'sending'`.
  2. **Ack Timeout Handling (5s):**
     * Nếu Server trả về ACK thành công: Đổi `status: 'sent'`, gắn `id` thật của MongoDB vào tin nhắn.
     * Nếu Timeout hoặc Server trả lỗi: Đổi `status: 'failed'`, hiển thị **icon chấm than đỏ (Failed)** bên cạnh tin nhắn.
  3. **Cơ chế Thử lại (Retry Mechanism):**
     * **Auto-retry:** Khi WebSocket tự động kết nối lại (reconnect event), tự động quét các tin `failed` và thử gửi lại với Exponential Backoff (1s, 2s).
     * **Manual Retry:** Người dùng bấm vào nút "Gửi lại" cạnh tin nhắn bị lỗi $\rightarrow$ Frontend phát lại event `new_message` với **chính xác `client_msg_id` ban đầu**.
  4. **Bảo vệ bằng Idempotency:** Nhờ giữ nguyên `client_msg_id`, dù tin nhắn cũ thực chất đã tới Server (chỉ bị rớt ACK chiều về), Backend cũng không bao giờ lưu 2 tin trùng nhau.

---

## 6. Bảo toàn Thứ tự Tin nhắn & Bù gói tin (Message Ordering & Gap Detection)

### 6.1. Vấn đề Thứ tự trong Hệ thống Phân tán
Khi nhiều tin nhắn gửi lên đồng thời tới các Worker Node khác nhau, độ trễ mạng khiến timestamp (`created_at`) không đảm bảo tuyệt đối thứ tự tuần tự của chuỗi hội thoại.

### 6.2. Cơ chế Sequence Counter (Redis Atomic INCR)
* Mỗi cuộc trò chuyện gắn với một bộ đếm số nguyên nguyên tử trên Redis: `conv:{convId}:seq`.
* Mỗi tin nhắn mới được cấp một số `seq` tăng dần duy nhất:
  ```typescript
  const seq = await redis.incr(`conv:${convId}:seq`);
  messageData.seq = seq;
  ```

### 6.3. Gap Detection ở Client (Phát hiện và Kéo bù tin nhắn mất)
* Client lưu trữ giá trị `lastReceivedSeq` của cuộc trò chuyện hiện tại.
* Khi nhận tin nhắn qua Socket có `seq`:
  * Nếu `seq === lastReceivedSeq + 1`: Tin nhắn hoàn hảo, cập nhật `lastReceivedSeq = seq`.
  * Nếu `seq > lastReceivedSeq + 1`: **Phát hiện mất gói tin (Packet Loss)**.
  * **Hành động:** Client tự động kích hoạt truy vấn kéo bù khoảng trống:
    `GET /api/messages?conversation_id={id}&from_seq={lastReceivedSeq + 1}&to_seq={seq - 1}`.

---

## 7. Quản lý Vòng đời & Triển khai Không Gián đoạn (Graceful Shutdown)

Khi deploy phiên bản mới qua lệnh `pm2 reload chatbot-api`, hệ thống phải đạt chuẩn **Zero Packet Loss**.

### 7.1. Quy trình Tắt tuần tự (Sequential Shutdown Pipeline)

```
[Nhận tín hiệu SIGTERM / SIGINT]
               │
               ▼
[Bước 1]: Dừng nhận HTTP traffic mới (server.close())
               │
               ▼
[Bước 2]: Gửi thông báo & đóng an toàn các kết nối Socket.IO
          (socketManager.getIO().disconnectSockets(true))
               │
               ▼
[Bước 3]: Chờ toàn bộ BullMQ Workers hoàn tất các jobs đang xử lý dở
          (await Promise.all(workers.map(w => w.close())))
               │
               ▼
[Bước 4]: Đóng an toàn các kết nối Database & Storage Pools
          (await mongoDB.disconnect(), await redisClient.disconnect())
               │
               ▼
[Bước 5]: Thoát tiến trình an toàn (process.exit(0))
```

### 7.2. Deep Health Checks
Thay thế endpoint `/health` đơn giản bằng 2 probe chuyên biệt:
* **/health/live (Liveness Probe):** Xác nhận tiến trình Node.js còn hoạt động.
* **/health/ready (Readiness Probe):** Xác nhận hệ thống sẵn sàng phục vụ:
  * MongoDB: `readyState === 1`
  * Redis: `PING === PONG`
  * Elasticsearch: Cluster status !== `red`
  * Nếu một trong các thành phần sập $\rightarrow$ Trả về HTTP 503, Load Balancer tự động ngắt điều hướng traffic vào Node đó.

---

## 8. Kiểm soát Tải & Chống Spam Phân tán (Distributed Rate Limiting)

### 8.1. HTTP Rate Limiting (Redis-backed)
* Thay thế bộ nhớ RAM cục bộ của `express-rate-limit` bằng **`rate-limit-redis`**.
* Đảm bảo hit count được cộng dồn chính xác giữa tất cả các PM2 Workers và Physical Servers.

### 8.2. WebSocket Event Throttling
Kiểm soát tần suất gửi tin nhắn và các thao tác socket theo cơ chế Token Bucket / Sliding Window:
```typescript
const key = `ratelimit:ws:${userId}`;
const current = await redis.incr(key);
if (current === 1) {
    await redis.expire(key, 1); // Cửa sổ 1 giây
}
if (current > 10) { // Giới hạn 10 sự kiện / giây
    return ack?.({
        success: false,
        code: 429,
        message: "Bạn đang thao tác quá nhanh, vui lòng thử lại sau giây lát."
    });
}
```

---

## 9. Hệ thống Giám sát & Truy vết (Observability & Distributed Tracing)

Hệ thống triển khai đầy đủ **3 Trụ cột Giám sát (The 3 Pillars of Observability: Metrics - Logs - Traces)** chuẩn Enterprise bằng bộ tứ mã nguồn mở: **Prometheus + Loki + Jaeger + Grafana**.

```
                       ┌──────────────────────────────────────────────┐
                       │          GRAFANA (Bảng điều khiển)           │
                       │     (Màn hình duy nhất hiển thị tất cả)      │
                       └──────▲───────────────▲───────────────▲───────┘
                              │               │               │
            ┌─────────────────┴─┐       ┌─────┴──────────┐    └─────────────────┐
            │    PROMETHEUS     │       │     LOKI       │      │    JAEGER       │
            │    (METRICS)      │       │    (LOGS)      │      │   (TRACES)      │
            └─────────▲─────────┘       └───────▲────────┘      └───────▲─────────┘
                      │                         │                       │
      ┌───────────────┴─────────────────────────┴───────────────────────┴───────────────┐
      │                      HỆ THỐNG CHATBOT-HCMUS BACKEND                             │
      │  • Trả về /metrics              • Ghi log JSON (Pino)     • OpenTelemetry Spans │
      └─────────────────────────────────────────────────────────────────────────────────┘
```

### 9.1. Metrics (Chỉ số thời gian thực): Prometheus & `prom-client`
* **Cơ chế:** Backend xuất bản endpoint `/metrics` bằng thư viện `prom-client`. Prometheus server định kỳ kéo dữ liệu (scrape) mỗi 15 giây.
* **Các chỉ số quan trọng cần đo lường:**
  * `socket_active_connections`: Số lượng kết nối WebSocket trực tiếp đang duy trì.
  * `http_request_duration_seconds`: Histogram thời gian phản hồi API (p50, p95, p99).
  * `bullmq_jobs_waiting`, `bullmq_jobs_active`, `bullmq_jobs_failed`: Độ trễ và tình trạng tắc nghẽn của từng queue (`fastQueue`, `mediaQueue`, `cronQueue`).
  * `nodejs_eventloop_lag_seconds`: Độ trễ của Event Loop (cảnh báo sớm nguy cơ sập WebSocket).

### 9.2. Logs (Nhật ký có cấu trúc): Pino & Grafana Loki
* **Loại bỏ hoàn toàn `console.log` và `morgan`:** Thay bằng thư viện **Pino** với tốc độ xử lý nhanh nhất thế giới Node.js, không làm nghẽn Event Loop.
* **Log định dạng chuẩn JSON:** Tự động gắn nhãn: `level`, `time`, `reqId`, `traceId`, `userId`, `conversationId`, `error.stack`.
* **Gom log bằng Loki:**
  * Không dùng Elasticsearch để lưu log (nhằm tiết kiệm RAM).
  * Loki chỉ index metadata/labels (`app="chatbot-backend"`, `level="error"`), dữ liệu nén dạng chunks cực kỳ tiết kiệm bộ nhớ (~150MB RAM).
  * Promtail / Docker logging driver tự động chuyển log từ container vào Loki.

### 9.3. Traces (Truy vết phân tán): OpenTelemetry & Jaeger
* **Cơ chế:** Tích hợp `@opentelemetry/sdk-node` tự động tạo Trace Context và Trace ID (`trace_id`) xuyên suốt vòng đời của 1 request:
  * Client gửi tin $\rightarrow$ Socket Gateway $\rightarrow$ Redis check `sismember` $\rightarrow$ BullMQ producer $\rightarrow$ BullMQ worker $\rightarrow$ MongoDB insert $\rightarrow$ Sync Elasticsearch.
* **Hiển thị trên Jaeger (All-in-one):**
  * Vẽ biểu đồ thác nước (Waterfall diagram) chi tiết từng mili-giây của từng Span.
  * Giúp kỹ sư xác định chính xác mắt xích nào bị chậm (Slow Query DB hay nghẽn hàng đợi) chỉ trong vài giây.

### 9.4. Correlation (Liên kết dữ liệu) trên Grafana Dashboard
* **Metric $\rightarrow$ Log $\rightarrow$ Trace kết hợp chặt chẽ:**
  1. Khi đồ thị Prometheus trên Grafana báo có đỉnh nhọn (Spike) lỗi 500 hoặc latency tăng cao.
  2. Kỹ sư quét chọn vùng thời gian $\rightarrow$ Grafana lọc ngay các dòng Log lỗi trong Loki tại thời điểm đó.
  3. Từ dòng Log lỗi chứa `trace_id`, bấm 1 click nhảy thẳng sang giao diện Waterfall của Jaeger để xem chính xác dòng code bị timeout/crash.

### 9.5. Tài nguyên Tiêu thụ trên Hạ tầng (Capacity Planning)
Bộ tứ được đóng gói chạy bằng Docker trên máy chủ Oracle Cloud ARM (12GB RAM), tổng mức chiếm dụng bộ nhớ chỉ khoảng **~550MB RAM**:
* Prometheus: ~150 MB
* Loki: ~150 MB
* Jaeger (All-in-one): ~150 MB
* Grafana: ~100 MB

---

## 10. Đóng gói Hạ tầng & Môi trường Chạy (Containerization & Deployment)

### 10.1. Multi-stage Dockerfile cho Backend
* **Stage 1 (Builder):** Cài đặt full `devDependencies`, biên dịch TypeScript sang JavaScript (`dist/`).
* **Stage 2 (Runner):** Base image `node:20-alpine`, chỉ copy `dist/` và cài đặt `production` dependencies.
* Chạy dưới quyền người dùng không đặc quyền (`USER node`) để bảo mật.

### 10.2. Multi-stage Dockerfile cho Frontend (Next.js Standalone)
* Bật `output: "standalone"` trong `next.config.ts`.
* Image xuất xưởng chỉ bao gồm `.next/standalone` và thư mục `public/`, dung lượng giảm từ >1GB xuống còn ~150MB.

---

## 11. Lộ trình Triển khai Chi tiết (Implementation Roadmap)

```
┌────────────────────────────────────────────────────────────────────────┐
│ PHASE 1: CORE RELIABILITY & DECOUPLING (Ưu tiên P0)                   │
├────────────────────────────────────────────────────────────────────────┤
│ 1. Tách entry point: `src/index.ts` (API/WS) & `src/worker.ts` (Queue) │
│ 2. Thiết lập PM2 Ecosystem: `chatbot-api` (Cluster) + `chatbot-worker` │
│ 3. Triển khai Graceful Shutdown & Deep Health Checks (/health/ready)   │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ PHASE 2: DISTRIBUTED IDEMPOTENCY, RETRY & ORDERING (Ưu tiên P0)        │
├────────────────────────────────────────────────────────────────────────┤
│ 1. Frontend: Sinh `client_msg_id` (UUIDv4) + Optimistic UI (sending)   │
│ 2. Frontend: Hàng đợi Thử lại (Auto-retry & Nút Manual Retry khi lỗi)  │
│ 3. Backend: Redis Idempotency Key (`SET NX EX`) & Cache kết quả ACK    │
│ 4. Database: Compound Unique Index Mongo `{ conversation_id, msg_id }` │
│ 5. BullMQ: JobId Deduplication                                         │
│ 6. State: Monotonic Watermark Update Guard                             │
│ 7. Sequence Counter: (`conv:{id}:seq`) & Gap Detection                 │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ PHASE 3: OBSERVABILITY, RATE LIMITING & TELEMETRY (Ưu tiên P1)         │
├────────────────────────────────────────────────────────────────────────┤
│ 1. Structured Logging: Tích hợp Pino JSON + Grafana Loki               │
│ 2. Distributed Tracing: OpenTelemetry SDK + Jaeger (Waterfall traces)  │
│ 3. Metrics & Dashboard: `prom-client` (/metrics) + Prometheus + Grafana│
│ 4. Rate Limiting: Distributed Redis Token Bucket & Socket Throttling   │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│ PHASE 4: CONTAINERIZATION & CI/CD (Ưu tiên P2)                         │
├────────────────────────────────────────────────────────────────────────┤
│ 1. Multi-stage Dockerfile cho Backend & Frontend (Standalone)          │
│ 2. Docker Compose Production hoàn chỉnh (Nginx, App, Worker, Redis, ES)│
│ 3. Sửa deprecation warning middleware Next.js 16                       │
└────────────────────────────────────────────────────────────────────────┘
```

---

*Tài liệu này là cơ sở kỹ thuật ràng buộc (Ground Truth) để các kỹ sư và AI Coding Agent triển khai chính xác mã nguồn theo từng giai đoạn.*
