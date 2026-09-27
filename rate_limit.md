# Hướng Dẫn & Đề Xuất Thiết Kế: Rate Limiting Với Redis Cho Chatbot-HCMUS

Tài liệu này phân tích hiện trạng, so sánh các giải pháp và cung cấp hướng dẫn triển khai **Rate Limiting** dựa trên nền tảng **Redis** cho hệ thống Chatbot-HCMUS (Backend Express + TypeScript ESM + ioredis + Socket.IO).

---

## 1. Hiện Trạng & Tại Sao Cần Redis Rate Limit?

### 1.1. Vấn đề của In-Memory Rate Limit hiện tại
Hiện tại, trong file `backend/src/app.ts`:
```typescript
app.use(rateLimit(config.rateLimit))
```
Hệ thống đang dùng `express-rate-limit` với memory store mặc định (lưu trực tiếp trong RAM của process Node.js). Điều này dẫn đến các hạn chế nghiêm trọng trong môi trường thực tế:
1. **Không hỗ trợ Multi-instance / Clustering**: Hệ thống đã có cấu hình PM2 (`ecosystem.config.cjs`) và hỗ trợ worker tách rời. Khi chạy nhiều instance hoặc deploy container (Docker/K8s), memory giữa các tiến trình không được chia sẻ. Kẻ tấn công có thể phân tán request vào các tiến trình khác nhau để nhân số lượng request cho phép lên gấp $N$ lần (với $N$ là số core/instance).
2. **Mất trạng thái khi Restart**: Mỗi khi server reload hoặc worker restart, toàn bộ quota rate limit của người dùng bị reset về 0.
3. **Không áp dụng được cho WebSocket / Background Tasks**: Memory store của Express chỉ chạy ở tầng HTTP middleware, không thể tái sử dụng cho các luồng realtime như Socket.IO (chống spam message/typing).

### 1.2. Tại sao dùng Redis?
- **Tập trung & Đồng bộ (Centralized & Distributed)**: Toàn bộ API servers đọc/ghi cùng một nguồn chân lý (Single Source of Truth).
- **Tốc độ cực nhanh**: Độ trễ đọc/ghi bộ nhớ RAM trong Redis chỉ mất khoảng `0.1ms - 0.5ms`.
- **Hỗ trợ TTL tự động**: Redis tự động thu hồi bộ nhớ bằng cơ chế `EXPIRE` sau khi hết khung thời gian (Window).
- **Tính nguyên tử (Atomicity)**: Các lệnh như `INCR`, `EXPIRE` hoặc Redis Lua Scripts đảm bảo không bao giờ bị race condition khi có hàng nghìn request đồng thời.
- **Tận dụng hạ tầng sẵn có**: Dự án đã có sẵn kết nối `ioredis` tại `src/infrastructure/redis/redis.client.ts`.

---

## 2. So Sánh Các Thuật Toán Rate Limiting

| Thuật toán | Cơ chế hoạt động | Ưu điểm | Nhược điểm | Đánh giá cho Chatbot-HCMUS |
| :--- | :--- | :--- | :--- | :--- |
| **Fixed Window Counter** | Đếm số request trong chu kỳ cố định (VD: 00:00 - 00:15). Lệnh: `INCR` + `EXPIRE`. | Cực kỳ nhẹ, tốn ít bộ nhớ RAM Redis nhất. | **Lỗ hổng giáp ranh (Traffic Burst at boundary)**: User gửi full quota ở cuối phút thứ 14 và tiếp tục gửi full quota ở đầu phút thứ 15 $\rightarrow$ gấp đôi lưu lượng cho phép. | Phù hợp cho Global Anti-DDoS cơ bản. |
| **Sliding Window Log** | Lưu timestamp của từng request vào Redis `ZSET`. Xóa các phần tử cũ hơn `now - window`, đếm `ZCARD`. | Độ chính xác tuyệt đối 100%, không bị bùng nổ ở ranh giới. | Tốn bộ nhớ RAM Redis vì lưu từng timestamp của từng request. | Rất tốt cho các route nhạy cảm (Auth, Forgot Password, R2 Upload Presigned URL). |
| **Sliding Window Counter** | Kết hợp giữa Fixed Window hiện tại và Fixed Window trước đó theo trọng số thời gian trôi qua. | Cân bằng hoàn hảo giữa độ chính xác và mức tiêu hao RAM (chỉ lưu 2 counter). | Xấp xỉ tương đối (sai số ~0.05%), nhưng hoàn toàn chấp nhận được trong thực tế. | **Khuyên dùng** cho phần lớn REST API thông thường. |
| **Token Bucket** | Thùng chứa token được nạp đầy theo chu kỳ. Mỗi request tiêu tốn 1 hoặc nhiều token. Khi hết token sẽ bị chặn. | Cho phép xử lý burst traffic hợp lệ trong thời gian ngắn mà vẫn đảm bảo tốc độ trung bình ổn định. | Cần Redis Lua Script để đảm bảo atomic giữa tính token và trừ token. | **Rất phù hợp cho WebSocket Chat & Upload API** (ví dụ user gửi 1 loạt ảnh/tin nhắn ngắn). |
| **Leaky Bucket** | Request đi vào hàng đợi và được xử lý theo tốc độ cố định, nếu tràn hàng đợi thì từ chối. | Lưu lượng ra luôn mượt mà, chống spike tuyệt đối. | Có thể làm tăng độ trễ của request khi hàng đợi đầy. | Thường dùng cho background queue hơn là realtime API. |

---

## 3. Các Phương Án Kỹ Thuật Có Thể Triển Khai

### Phương án 1: Sử dụng `rate-limit-redis` kết hợp `express-rate-limit` (Khuyên Dùng cho REST API)
- **Mô tả**: Tích hợp thư viện chính thức `rate-limit-redis` vào middleware `express-rate-limit` hiện có, sử dụng client `ioredis` đang chạy trong dự án.
- **Ưu điểm**:
  - Tận dụng cấu hình sẵn có của `express-rate-limit`.
  - Hỗ trợ đầy đủ chuẩn HTTP Header IETF (`RateLimit-Limit`, `RateLimit-Remaining`, `RateLimit-Reset`, `Retry-After`).
  - Hỗ trợ thuật toán **Sliding Window Counter** hoặc Hash-based store, chống race condition.
  - Tự động fallback fail-open khi Redis gặp sự cố (tùy cấu hình).
  - Code gọn, chuẩn hóa theo Clean Architecture.
- **Nhược điểm**: Chỉ áp dụng cho tầng Express HTTP, không áp dụng trực tiếp cho Socket.IO.

### Phương án 2: Tự xây dựng Custom Redis Rate Limiter Middleware (Dùng Lua Script)
- **Mô tả**: Tự viết Middleware sử dụng `redisClient` chạy script Lua (Token Bucket hoặc Sliding Window Log bằng `ZSET`).
- **Ưu điểm**:
  - Không cần cài thêm thư viện phụ thuộc ngoài `ioredis`.
  - Có thể tái sử dụng 100% cùng 1 core logic cho cả **Express Route** và **Socket.IO Event Handlers** (`message:send`, `conversation:join`...).
  - Tùy biến linh hoạt payload lỗi trả về theo chuẩn `ApiResponse` của dự án.
- **Nhược điểm**: Cần tự quản lý và kiểm thử chặt chẽ logic tính toán thời gian, headers HTTP và xử lý lỗi kết nối.

### Phương án 3: Kết hợp Hybrid (Toàn diện nhất)
- Dùng **Phương án 1** cho các tầng API HTTP (Global, Auth, Upload).
- Dùng **Custom Redis Token Bucket / Sliding Log** cho tầng Socket.IO Realtime.

---

## 4. Thiết Kế Phân Tầng (Tiered Rate Limiting) Cho Dự Án

| Tầng bảo vệ | Target Key | Giới hạn gợi ý | Mục đích |
| :--- | :--- | :--- | :--- |
| **1. Global API Limiter** | `rl:global:<ip>` | 300 requests / 15 phút | Bảo vệ hạ tầng chung, ngăn chặn crawler / DoS thô sơ. |
| **2. Auth Sensitive Limiter** | `rl:auth:<ip>:<email>` | 5 - 10 requests / 15 phút | Ngăn chặn Brute Force mật khẩu, spam Google OAuth, spam Refresh Token. |
| **3. Media / Upload Limiter** | `rl:media:<userId>` | 30 requests / 5 phút | Bảo vệ chi phí Cloudflare R2 presigned URL và tài nguyên xử lý video ffmpeg. |
| **4. Realtime Socket Limiter** | `rl:socket:<userId>` | 5 messages / giây (burst 10) | Chống spam tin nhắn phòng chat, chống DDoS Socket.IO server. |

---

## 5. Hướng Dẫn Triển Khai Chi Tiết (Theo Phương Án Khuyên Dùng)

### Bước 1: Cài đặt package store Redis
Cài đặt thư viện kết nối chính thức giữa `express-rate-limit` và `ioredis`:
```bash
npm install rate-limit-redis
```

### Bước 2: Tạo Rate Limit Middleware Module
Tạo file `backend/src/shared/middlewares/rate-limit.middleware.ts`:

```typescript
import rateLimit, { Options } from "express-rate-limit";
import { RedisStore } from "rate-limit-redis";
import { redisClient } from "#@/infrastructure/redis/redis.client.js";
import { config } from "#@/config/config.js";
import type { Request, Response } from "express";

// Khởi tạo Redis Store dùng chung ioredis client hiện tại
const createRedisStore = (prefix: string) => {
    return new RedisStore({
        sendCommand: (...args: string[]) => {
            const client = redisClient.getClient();
            // ioredis sendCommand wrapper
            return client.call(args[0], ...args.slice(1)) as any;
        },
        prefix: `rl:${prefix}:`,
    });
};

// Response format chuẩn theo ApiResponse của dự án
const defaultHandler = (req: Request, res: Response) => {
    res.status(429).json({
        success: false,
        message: "Bạn đã gửi quá nhiều yêu cầu. Vui lòng thử lại sau.",
        data: null,
    });
};

// 1. Global Limiter cho toàn bộ /api
export const globalRateLimiter = rateLimit({
    windowMs: config.rateLimit.windowMs, // 15 phút
    limit: config.rateLimit.limit, // vd: 300 requests
    standardHeaders: "draft-7", // Trả về headers RateLimit-*
    legacyHeaders: false,
    store: createRedisStore("global"),
    handler: defaultHandler,
});

// 2. Strict Limiter cho các API Auth (Login, Register, Refresh)
export const authRateLimiter = rateLimit({
    windowMs: 15 * 60 * 1000, // 15 phút
    limit: 10, // Tối đa 10 lần thử
    standardHeaders: "draft-7",
    legacyHeaders: false,
    store: createRedisStore("auth"),
    keyGenerator: (req: Request) => {
        // Kết hợp IP và email/account để tránh khóa nhầm toàn bộ IP trường (HCMUS NAT IP)
        const email = req.body?.email || "";
        return `${req.ip}_${email}`;
    },
    handler: (req: Request, res: Response) => {
        res.status(429).json({
            success: false,
            message: "Quá nhiều lần đăng nhập không thành công. Vui lòng thử lại sau 15 phút.",
            data: null,
        });
    },
});

// 3. Media Upload Limiter
export const mediaRateLimiter = rateLimit({
    windowMs: 5 * 60 * 1000, // 5 phút
    limit: 25,
    standardHeaders: "draft-7",
    legacyHeaders: false,
    store: createRedisStore("media"),
    keyGenerator: (req: any) => {
        // Ưu tiên theo user_id nếu đã qua authMiddleware, nếu không thì dùng IP
        return req.user?.id || req.ip;
    },
    handler: defaultHandler,
});
```

### Bước 3: Đăng ký Middleware vào `backend/src/app.ts` & các Route Cụ Thể

Trong `backend/src/app.ts`:
```typescript
import { globalRateLimiter } from "#@/shared/middlewares/rate-limit.middleware.js";

// Đảm bảo Express tin tưởng reverse proxy (Nginx / Cloudflare) để lấy đúng IP người dùng
app.set("trust proxy", 1);

// Áp dụng Global Rate Limit cho toàn bộ API
app.use("/api", globalRateLimiter);
```

Trong `backend/src/modules/auth/auth.route.ts`:
```typescript
import { authRateLimiter } from "#@/shared/middlewares/rate-limit.middleware.js";

// Bọc vào các route nhạy cảm
router.post("/login", authRateLimiter, authController.login);
router.post("/google", authRateLimiter, authController.googleLogin);
```

---

## 6. Thiết Kế Generic Socket Rate Limiter Dùng Chung Cho Toàn Bộ Socket.IO

### 6.1. Tại sao không nên viết logic rate limit cứng trong `message.socket.ts`?
1. **Vi phạm nguyên tắc DRY (Don't Repeat Yourself)**: Không chỉ gửi tin nhắn, các sự kiện socket khác như **tạo cuộc trò chuyện (`CREATE_CONVERSATION`)**, **cập nhật thông tin nhóm (`UPDATE_CONVERSATION`)**, **thêm thành viên**, hay **thả cảm xúc (reaction)** đều có nguy cơ bị spam/bot flood làm nghẽn MongoDB và Redis.
2. **Tuân thủ Clean Architecture**: Các file `*.socket.ts` ở từng domain module (`modules/conversation/`, `modules/message/`) chỉ chịu trách nhiệm validate payload và chuyển giao cho Service/Facade.
3. **Giải pháp chuẩn**: Xây dựng một **Generic Socket Rate Limiter Helper** đặt tại tầng hạ tầng WebSocket:
   `backend/src/infrastructure/websocket/socket-rate-limit.ts`.

---

### 6.2. Mã nguồn Generic Helper (`socket-rate-limit.ts`)

```typescript
// backend/src/infrastructure/websocket/socket-rate-limit.ts
import { redisClient } from "#@/infrastructure/redis/redis.client.js";

export interface SocketRateLimitOptions {
    /** Key định danh đối tượng (Ví dụ: `conv:create:${userId}`, `msg:send:${userId}`) */
    key: string;
    /** Số lượng request tối đa trong cửa sổ thời gian */
    limit: number;
    /** Thời gian chu kỳ tính bằng giây */
    windowSeconds: number;
}

export interface SocketRateLimitResult {
    /** true nếu được phép đi tiếp, false nếu bị chặn */
    allowed: boolean;
    /** Số lượt còn lại trong chu kỳ */
    remaining: number;
    /** Số giây cần chờ trước khi được gửi tiếp */
    retryAfterSeconds: number;
}

/**
 * Kiểm tra Rate Limit cho các sự kiện Socket.IO sử dụng Redis Atomic Counter
 */
export async function checkSocketRateLimit(
    options: SocketRateLimitOptions
): Promise<SocketRateLimitResult> {
    const { key, limit, windowSeconds } = options;
    const client = redisClient.getClient();
    const redisKey = `rl:ws:${key}`;

    try {
        // Tăng bộ đếm nguyên tử
        const current = await client.incr(redisKey);

        // Nếu là request đầu tiên trong window -> thiết lập TTL
        if (current === 1) {
            await client.expire(redisKey, windowSeconds);
        }

        // Lấy thời gian sống còn lại (TTL) để phản hồi cho client
        let ttl = await client.ttl(redisKey);
        if (ttl < 0) {
            // Đề phòng trường hợp hiếm hoi key bị thiếu TTL
            await client.expire(redisKey, windowSeconds);
            ttl = windowSeconds;
        }

        const allowed = current <= limit;
        const remaining = Math.max(0, limit - current);

        return {
            allowed,
            remaining,
            retryAfterSeconds: allowed ? 0 : ttl,
        };
    } catch (error) {
        console.error(`[SocketRateLimit] Lỗi Redis trên key ${redisKey}:`, error);
        // Fail-Open: Nếu Redis gián đoạn, vẫn cho phép người dùng thực hiện để đảm bảo trải nghiệm
        return {
            allowed: true,
            remaining: 1,
            retryAfterSeconds: 0,
        };
    }
}
```

---

### 6.3. Ứng dụng thực tế vào các Socket Modules

#### 1. Áp dụng trong `conversation.socket.ts`
Chống spam tạo nhóm và cập nhật thông tin nhóm:

```typescript
// backend/src/modules/conversation/conversation.socket.ts
import { checkSocketRateLimit } from "#@/infrastructure/websocket/socket-rate-limit.js";

// 1. Tạo cuộc trò chuyện mới (Giới hạn: tối đa 5 nhóm / 60 giây)
socket.on(SocketEvents.CREATE_CONVERSATION, async (rawData: unknown, ack?: (res: SocketAckResponse<Conversation>) => void) => {
    try {
        const rateLimit = await checkSocketRateLimit({
            key: `conv:create:${userId}`,
            limit: 5,
            windowSeconds: 60,
        });

        if (!rateLimit.allowed) {
            return ack?.({
                success: false,
                code: 429,
                message: `Bạn đang tạo nhóm quá nhanh. Vui lòng thử lại sau ${rateLimit.retryAfterSeconds}s!`,
            });
        }

        const data = validateSocketPayload(CreateConversationSocketSchema, rawData, ack);
        if (!data) return;

        const conversation = await conversationContainer.conversationService.createConversation(userId, data);
        ack?.({ success: true, data: conversation });
    } catch (error: any) {
        ack?.({ success: false, code: error.status || 500, message: error.message });
    }
});

// 2. Cập nhật thông tin nhóm (Tên, Avatar) (Giới hạn: tối đa 10 lần / 60 giây)
socket.on(SocketEvents.UPDATE_CONVERSATION, async (rawData: unknown, ack?: (res: SocketAckResponse<Conversation>) => void) => {
    try {
        const rateLimit = await checkSocketRateLimit({
            key: `conv:update:${userId}`,
            limit: 10,
            windowSeconds: 60,
        });

        if (!rateLimit.allowed) {
            return ack?.({
                success: false,
                code: 429,
                message: `Thao tác đổi thông tin nhóm quá thường xuyên. Vui lòng chờ ${rateLimit.retryAfterSeconds}s!`,
            });
        }

        const data = validateSocketPayload(UpdateConversationSocketSchema, rawData, ack);
        if (!data) return;

        const updated = await conversationContainer.conversationService.updateConversation(userId, data.conversation_id, {
            name: data.name,
            avatar_url: data.avatar_url,
            primary_icon: data.primary_icon,
        });
        ack?.({ success: true, data: updated || undefined });
    } catch (error: any) {
        ack?.({ success: false, code: error.status || 500, message: error.message });
    }
});
```

#### 2. Áp dụng trong `message.socket.ts`
Chống flood tin nhắn và spam thả reaction:

```typescript
// backend/src/modules/message/message.socket.ts
import { checkSocketRateLimit } from "#@/infrastructure/websocket/socket-rate-limit.js";

// 1. Gửi tin nhắn (Giới hạn: tối đa 10 tin nhắn / 3 giây)
socket.on(SocketEvents.MESSAGE_SEND, async (rawData: unknown, ack?: (res: SocketAckResponse<Message>) => void) => {
    try {
        const rateLimit = await checkSocketRateLimit({
            key: `msg:send:${userId}`,
            limit: 10,
            windowSeconds: 3,
        });

        if (!rateLimit.allowed) {
            return ack?.({
                success: false,
                code: 429,
                message: `Bạn đang gửi tin nhắn quá nhanh. Vui lòng chờ ${rateLimit.retryAfterSeconds}s!`,
            });
        }

        // Xử lý gửi tin nhắn...
    } catch (error: any) {
        ack?.({ success: false, code: error.status || 500, message: error.message });
    }
});
```

---

## 7. Các Lưu Ý Kỹ Thuật Quan Trọng (Best Practices & Edge Cases)

1. **Vấn đề Chung IP trường HCMUS (NAT IP Issue)**:
   - Các sinh viên truy cập wifi trường HCMUS có thể có cùng 1 địa chỉ Public IP.
   - Nếu chỉ Rate Limit theo `req.ip` ở Global thì có thể một nhóm sinh viên vô tình làm cạn kiệt quota của nhau.
   - **Giải pháp**:
     - Global limiter đặt mức vừa phải (vd: 300 - 500 req / 15 phút).
     - Với các route đã đăng nhập, luôn ưu tiên rate limit theo `user_id` thay vì IP: `keyGenerator: (req) => req.user?.id ?? req.ip`.
2. **Reverse Proxy & Header IP (`trust proxy`)**:
   - Dự án đã cấu hình `app.set("trust proxy", 1);` trong `app.ts`. Điều này đảm bảo `req.ip` lấy đúng từ header `X-Forwarded-For` do Nginx/Cloudflare truyền vào.
3. **Fail-Open hay Fail-Close khi Redis gặp sự cố?**
   - Mặc định, nếu Redis bị sập tạm thời hoặc đứt mạng, `rate-limit-redis` sẽ log lỗi và **Fail-Open** (cho phép request đi qua thay vì làm sập toàn bộ ứng dụng của người dùng). Đây là best practice cho trải nghiệm người dùng (UX).
4. **Header chuẩn IETF**:
   - Sử dụng `standardHeaders: "draft-7"` để client (Frontend Next.js) có thể nhận các header:
     - `RateLimit-Limit`: Tổng số request cho phép trong window.
     - `RateLimit-Remaining`: Số request còn lại.
     - `RateLimit-Reset`: Thời gian (giây) còn lại trước khi reset window.

---

## 8. Các Câu Hỏi & Quyết Định Cần Thống Nhất

Trước khi tiến hành code và cài đặt, bạn vui lòng cho ý kiến về các điểm kỹ thuật sau:
1. **Lựa chọn thư viện**: Bạn muốn dùng **Phương án 1** (`rate-limit-redis` tích hợp vào `express-rate-limit` hiện có) hay tự viết **Custom Redis Lua Script**?
2. **Phạm vi bảo vệ**: Bạn có muốn áp dụng Rate Limit cho cả **Socket.IO Realtime Chat** không, hay chỉ ưu tiên cho tầng **HTTP REST API** trước?
3. **Mức giới hạn (Quota)**: Các ngưỡng đề xuất (Global: 300 req/15m; Auth: 10 req/15m; Socket: 5 msg/2s) đã phù hợp với use-case của bạn chưa hay cần điều chỉnh?
