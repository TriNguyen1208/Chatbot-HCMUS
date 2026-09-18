# 📚 TỔNG QUAN VÀ HƯỚNG DẪN HỌC NHANH FRONTEND (CHATBOT-HCMUS)

Tài liệu này được biên soạn để giúp bạn nắm bắt toàn bộ kiến trúc, luồng dữ liệu, cách tổ chức mã nguồn và các patterns cốt lõi của thư mục `frontend` trong thời gian ngắn nhất.

---

## 1. Stack Công Nghệ & Tổng Quan

* **Framework:** **Next.js 16 (App Router)** + **React 19** (hỗ trợ React Compiler).
* **Styling:** **Tailwind CSS v4** (CSS variables, Dark mode qua `next-themes`).
* **State Management:**
  * **TanStack Query v5 (@tanstack/react-query):** Quản lý Server State (danh sách tin nhắn phân trang, danh sách cuộc trò chuyện, cache sync).
  * **Zustand 5:** Quản lý Client State (active conversation, modal open/close, typing status, user dataloader cache).
* **Realtime Communication:** **Socket.IO Client** (kết nối WebSockets full-duplex với backend).
* **Kiến trúc cốt lõi:**
  1. **Fractal Component Architecture:** Tách rời hoàn toàn giao diện (`.tsx`) và nghiệp vụ (`use*.ts`). Subcomponents chỉ dùng riêng cho cha thì nằm bên trong cha.
  2. **Single Realtime Core:** Một kết nối Socket duy nhất được quản lý qua `SocketProvider`, các module tự đăng ký và hủy sự kiện (`cleanup`).
  3. **DataLoader Batching Pattern:** Gom nhóm các request lấy thông tin user để triệt tiêu vấn đề $N+1$ query.

---

## 2. Sơ Đồ Cấu Trúc Thư Mục (`frontend/src`)

```text
frontend/src/
├── app/                              # Next.js App Router (Routing & Layouts)
│   ├── (main)/                       # Route group có bảo vệ (Auth Guard)
│   │   ├── (chat)/                   # Giao diện Chat chính
│   │   │   ├── chat/page.tsx         # /chat (Tất cả hội thoại)
│   │   │   ├── direct-chat/page.tsx  # /direct-chat (Chỉ chat 1-1)
│   │   │   ├── group-chat/page.tsx   # /group-chat (Chỉ chat nhóm)
│   │   │   └── layout.tsx            # Resizable Sidebar + mount useChatSocket
│   │   ├── home/page.tsx             # Trang chủ / Dashboard cá nhân
│   │   ├── profile/page.tsx          # Trang quản lý hồ sơ cá nhân
│   │   └── layout.tsx                # Mount SocketProvider duy nhất cho toàn bộ app
│   ├── layout.tsx                    # Root Layout: ThemeProvider, GoogleOAuth, Fonts
│   ├── page.tsx                      # Trang Login (/)
│   └── providers.tsx                 # Bọc toàn bộ Global Context Providers
│
├── config/                           # Cấu hình môi trường (Zod validation)
│   ├── env.ts                        # Fallback & export env (apiUrl, googleClientId)
│   └── env.schema.ts
│
├── features/                         # DOMAIN-DRIVEN FEATURES (Trung tâm ứng dụng)
│   ├── auth/                         # Xác thực: Google OAuth, Token, AuthStore
│   │   ├── api/authApi.ts
│   │   └── stores/authStore.ts
│   │
│   └── chat/                         # TÍNH NĂNG CHAT (Trọng tâm hệ thống)
│       ├── api/                      # Các HTTP requests (conversation, message, media, user, search)
│       ├── socket/                   # Xử lý Realtime Socket.IO
│       │   ├── handlers/             # Các handlers phân tách theo domain (message, conv, watermark...)
│       │   └── useChatSocket.ts      # Hook điều phối đăng ký & giải phóng listeners
│       ├── stores/                   # Zustand Stores (chatStore, userStore, modalStore, searchStore)
│       ├── utils/
│       │   └── chat-cache.util.ts    # Pure functions cập nhật TanStack Query cache tức thì
│       └── components/               # CÂY FRACTAL COMPONENT
│           ├── Sidebar/              # Cột trái (Tabs, Danh sách chat, Profile, FriendBar)
│           ├── ChatArea/             # Khung chat chính (Header, Input, Drawer thông tin)
│           ├── Messages/             # Danh sách tin nhắn & từng bong bóng tin nhắn
│           ├── ChatCard/             # Thẻ hiển thị cuộc trò chuyện ở Sidebar
│           └── Modals/               # Chỉ chứa 3 Global Modals (UserProfile, CreateGroup, Forward)
│
├── lib/                              # Thư viện hạ tầng & HTTP Client
│   └── api.ts                        # Axios instance + Interceptor tự refresh token + unwrap data
│
├── providers/                        # React Context Providers
│   ├── QueryProvider.tsx             # Cấu hình TanStack Query Client
│   └── SocketProvider.tsx            # Singleton Socket Provider (chống rò rỉ kết nối)
│
├── shared/                           # Dùng chung toàn hệ thống
│   └── services/
│       ├── socket.service.ts         # Singleton Socket.IO Client instance
│       └── upload.service.ts         # Upload video/ảnh S3 Multipart Chunk 5MB
│
├── types/                            # Type Definitions tập trung
│   ├── api.types.ts                  # ApiResponse<T>, Pagination
│   ├── chat.types.ts                 # Conversation, Message, Watermark, Reaction
│   └── user.types.ts                 # User (thống nhất dùng student_id)
│
└── middleware.ts                     # Edge Middleware: Kiểm tra cookie accessToken/refreshToken
```

---

## 3. Luồng Xác Thực & Điều Hướng (Auth Flow & Routing)

### 3.1. Next.js Edge Middleware (`src/middleware.ts`)
* Kiểm tra cookie `accessToken` hoặc `refreshToken`.
* **Quy tắc chuyển hướng:**
  1. Chưa đăng nhập mà vào route `/chat`, `/home`, `/profile` $\rightarrow$ Đẩy về `/` (Login).
  2. Đã có token mà cố truy cập lại trang `/` (Login) $\rightarrow$ Tự động chuyển thẳng vào `/chat`.

### 3.2. Chuẩn hóa Axios Client & Tự Động Refresh Token (`src/lib/api.ts`)
* **Tự động Unwrap dữ liệu:** Backend luôn trả về `{ status, message, data: T }`. Helper `http.get`, `http.post` tự động trả về `res.data.data`, code ở component/hook không cần viết `(res as any).data.data`.
* **Cơ chế Refresh Token hàng đợi (`failedQueue`):**
  * Khi nhận mã lỗi HTTP `401 Unauthorized`:
  * Nếu đang có request khác đi xin token mới (`isRefreshing === true`), các request sau được đưa vào một mảng hàng đợi `failedQueue` chờ đợi.
  * Gửi request `/api/auth/refresh-token` (trình duyệt tự đính kèm `refreshToken` HttpOnly cookie).
  * Thành công: Giải phóng `failedQueue` và replay lại toàn bộ request ban đầu.
  * Thất bại: Gọi `handleForceLogout()` để xoá state và chuyển về trang Login.

### 3.3. Khởi tạo phiên đăng nhập (`src/app/providers.tsx`)
* Khi ứng dụng load lần đầu, hook `useEffect` gọi `authApi.getMe()` để kiểm tra thông tin user hiện tại và lưu vào `useAuthStore`.

---

## 4. Kiến Trúc Realtime Core (Socket.IO)

Hệ thống realtime được thiết kế để giải quyết triệt để lỗi **Dual Socket Leak** (rò rỉ nhiều kết nối socket chạy ngầm cùng lúc).

### 4.1. Singleton Socket Service (`src/shared/services/socket.service.ts`)
* Đảm bảo chỉ có **duy nhất 1 instance** `Socket` được tạo trong toàn bộ vòng đời ứng dụng.
* Sử dụng WebSocket transport (`transports: ["websocket"]`) để đạt hiệu năng cao nhất và độ trễ thấp nhất.

### 4.2. Quản lý vòng đời qua `SocketProvider` (`src/providers/SocketProvider.tsx`)
* Đặt tại `src/app/(main)/layout.tsx`.
* **Khi user đăng nhập:** Gọi `socketService.connect()`.
* **Khi user đăng xuất:** Tự động gọi `socketService.disconnect()`.
* Các component muốn dùng Socket chỉ cần gọi `useSocketContext()`.

### 4.3. Đăng ký & Giải phóng Sự kiện Đối xứng (`src/features/chat/socket/`)
Hook `useChatSocket` tại `src/features/chat/socket/useChatSocket.ts` điều phối toàn bộ các handlers:
```typescript
const cleanups = [
  registerMessageHandlers(socket, queryClient),
  registerConversationHandlers(socket, queryClient, router),
  registerWatermarkHandlers(socket, queryClient),
  registerPresenceHandlers(socket),
  registerTypingHandlers(socket),
];
return () => cleanups.forEach((cleanup) => cleanup()); // Tránh rò rỉ bộ nhớ
```

* **`message.handler.ts`:**
  * Lắng nghe `message:new`: Dùng `chatCache.appendNewMessage` chèn tin nhắn vào React Query cache, đồng thời đưa hội thoại lên đầu danh sách (`bumpConversationLastMessage`).
  * Lắng nghe `message:reaction`, `message:recalled`, `message:edited`.
* **`conversation.handler.ts`:**
  * Lắng nghe `conversation:new`, `conversation:updated`, `conversation:disbanded`.
  * Xử lý trường hợp bị kick khỏi nhóm: Nếu đang đứng ở phòng chat đó thì tự động điều hướng về `/chat`.
* **`watermark.handler.ts`:**
  * Lắng nghe `watermark:update`: Cập nhật trạng thái "Đã xem / Đã nhận" cho các thành viên trong hội thoại.
* **`presence.handler.ts`:**
  * Lắng nghe `user:presence`: Đồng bộ trạng thái online/offline vào `userStore`.
* **`typing.handler.ts`:**
  * Lắng nghe `user:typing`: Hiển thị animation "...đang soạn tin" theo thời gian thực.

---

## 5. Quản Lý State (State Management Architecture)

Hệ thống phân chia ranh giới rõ ràng giữa **Server State** và **Client State**:

### 5.1. Server State: TanStack Query v5
Chịu trách nhiệm lưu trữ và đồng bộ dữ liệu từ server:
* `['conversations']`: Danh sách các cuộc trò chuyện (Infinite Query, phân trang theo con trỏ `limit=20`).
* `['messages', conversationId]`: Danh sách tin nhắn của hội thoại hiện tại (Infinite Query).
* `['conversation', conversationId]`: Chi tiết cấu hình hội thoại.

#### 🌟 Điểm nổi bật: `chat-cache.util.ts`
Thay vì gọi `queryClient.invalidateQueries()` (sẽ tạo request HTTP tải lại toàn bộ danh sách, gây giật lag và tốn băng thông), dự án sử dụng file `src/features/chat/utils/chat-cache.util.ts` chứa các hàm pure mutation trực tiếp lên cache RAM của TanStack Query:
* `appendNewMessage`: Chèn tin nhắn mới vào đầu page 0 tức thì.
* `bumpConversationLastMessage`: Đưa box chat vừa có tin nhắn mới lên đầu danh sách và cập nhật `last_message`.
* `updateMessageReaction`, `updateMessageRecalled`, `updateWatermarkInCache`...

### 5.2. Client State: Zustand 5 Stores
* **`useChatStore` (`src/features/chat/stores/chatStore.ts`):**
  * `activeConversation`: Cuộc trò chuyện đang được chọn mở trên màn hình.
  * `showInfoPanel`: Trạng thái đóng/mở thanh thông tin hội thoại bên phải.
  * `typingUsers`: Danh sách những người đang gõ trong từng conversation.
  * `editingMessage`: Tin nhắn đang được chọn để chỉnh sửa.
* **`useUserStore` (`src/features/chat/stores/userStore.ts`) — [DataLoader Pattern]:**
  * **Vấn đề giải quyết:** Trong chat nhóm có 50 tin nhắn của 10 người khác nhau, nếu mỗi avatar/tên lại gọi 1 request API lấy thông tin user thì sẽ xảy ra vấn đề $N+1$ request làm nghẽn server.
  * **Giải pháp:** Khi một component cần user info, nó gọi `requestUser(userId)`. Store sẽ gom các ID này vào một hàng đợi và hẹn giờ **50ms debounce**. Sau 50ms, nó gom tất cả ID và chỉ gửi **duy nhất 1 request** `POST /api/user/batch`.
* **`useModalStore` (`src/features/chat/stores/modalStore.ts`):**
  * Quản lý bật/tắt 3 Global Modals: `UserProfileModal`, `CreateGroupModal`, `ForwardModal`.
* **`useSearchStore`:** Quản lý input và debounce tìm kiếm toàn cục.

---

## 6. Mô Hình Fractal Component Architecture

Frontend tuân thủ chặt chẽ nguyên lý **Fractal Component**:
* Không đặt các file code dài hàng nghìn dòng ("God Component").
* Mỗi component là 1 thư mục gồm cặp đôi:
  1. **`[ComponentName].tsx` (Presenter):** Chỉ thuần túy chứa JSX, Tailwind CSS classes, gọi hook. Tuyệt đối không chứa logic phức tạp.
  2. **`use[ComponentName].ts` (Controller / Headless Hook):** Chứa toàn bộ state, API calls, event handlers, validation.
* **Component con dùng riêng** chỉ được nằm trong thư mục `components/` của component cha, không để lộ ra ngoài.

### 6.1. Cấu trúc Cây Component Chat

```text
ChatArea/
├── ChatArea.tsx & useChatArea.ts
└── components/
    ├── ChatHeader/               # Tên người/nhóm, avatar, trạng thái online, nút toggle info
    │   ├── ChatHeader.tsx
    │   └── useChatHeader.ts
    ├── ChatInput/                # Ô gõ tin nhắn, gửi ảnh/video, emoji picker, ghi âm
    │   ├── ChatInput.tsx
    │   └── useChatInput.ts
    └── ConversationInfo/         # Thanh drawer thông tin cuộc trò chuyện bên phải
        ├── ConversationInfo.tsx
        ├── useConversationInfo.ts
        └── components/           # Subcomponents & Private Modals riêng của ConversationInfo
            ├── InfoHeader/
            ├── MemberList/
            ├── MediaGallery/
            ├── DangerActions/
            ├── BlockUserModal/
            ├── DisbandGroupModal/
            ├── EditGroupModal/
            └── MediaViewerModal/
```

### 6.2. Phân biệt Private Modals vs Global Modals
* **Private Modals (Modal cục bộ):** Chỉ xuất hiện khi thao tác trong 1 component cụ thể (ví dụ: `BlockUserModal`, `EditGroupModal`, `ReactionModal` trong `MessageItem`). Các modal này được đặt ngay trong thư mục `components/` của chính component đó.
* **Global Modals (Modal toàn cục):** Được kích hoạt từ nhiều nơi khác nhau (Sidebar, Menu, Header...). Chỉ có **3 modal** này được đặt tại `features/chat/components/Modals/` và mount tại `app/(main)/(chat)/layout.tsx`:
  1. `UserProfileModal`: Xem thông tin chi tiết sinh viên khi click vào avatar bất kỳ đâu.
  2. `CreateGroupModal`: Tạo nhóm chat mới.
  3. `ForwardModal`: Chuyển tiếp tin nhắn sang cuộc hội thoại khác.

---

## 7. Các Tính Năng Nghiệp Vụ Quan Trọng

### 7.1. Gửi Tin Nhắn & Upload Media Đa Phương Tiện
* **Văn bản & Emoji:** Hỗ trợ Emoji Mart, tự động parse liên kết URL qua `linkifyjs` và hiển thị xem trước qua `LinkPreview`.
* **Upload Ảnh/Video (`src/shared/services/upload.service.ts`):**
  * Với video dung lượng lớn: Tách file thành từng chunk nhỏ **5MB**, xin presigned URL từ Cloudflare R2 / S3, sau đó hoàn tất multipart upload.
  * Hiển thị thanh tiến trình tải lên trực tiếp trên ô chat.

### 7.2. Đồng bộ "Đã xem" & "Đã nhận" (Watermarks)
* Khi `MessageList` cuộn đến tin nhắn mới nhất, hook gửi sự kiện socket `watermark:read`.
* Server ghi nhận watermark và phát socket cho các thành viên khác để hiển thị avatar nhỏ của người đã đọc bên cạnh tin nhắn.

### 7.3. Tìm Kiếm Toàn Cục (Elasticsearch & MongoDB)
* Thanh tìm kiếm tại Sidebar kết nối với `searchStore`.
* Tự động debounce 300ms, gọi API `/api/search/global` để tìm kiếm đồng thời: Người dùng, Nhóm chat và Tin nhắn văn bản.

---

## 8. Hướng Dẫn Nhanh Khi Phát Triển Code Mới (Cheat Sheet)

Khi bạn cần thêm tính năng mới hoặc sửa đổi code trong thư mục `frontend`:

| Tình Huống | Cách Làm Đúng Chuẩn | Điều CẤM LÀM |
| :--- | :--- | :--- |
| **Gọi API mới** | Viết function trong `features/chat/api/*.api.ts` sử dụng `http.get<T>()` / `http.post<T>()`. Trả về trực tiếp `Promise<T>`. | ❌ Không dùng `axios.get` trực tiếp rồi tự ép kiểu `res.data.data`. |
| **Dùng Socket** | Lấy socket từ hook `useSocketContext()`. | ❌ Tuyệt đối **CẤM** gọi `io(...)` hay `socketService.connect()` trong component. |
| **Tạo Component mới** | Tạo folder `[Name]/` với 2 file: `[Name].tsx` (chỉ render giao diện) và `use[Name].ts` (chứa toàn bộ logic/state). | ❌ Không viết cả trăm dòng logic, useEffect, useState gộp chung vào 1 file `.tsx`. |
| **Thêm Modal mới** | Nếu modal chỉ dùng cho 1 màn hình/nút bấm, đặt nó vào thư mục `components/` của màn hình đó (Private Modal). | ❌ Không ném bừa bãi vào thư mục `features/chat/components/Modals/`. |
| **Cập nhật tin nhắn realtime** | Sử dụng các helper functions trong `src/features/chat/utils/chat-cache.util.ts`. | ❌ Không gọi `queryClient.invalidateQueries()` bừa bãi khi nhận tin nhắn mới. |
| **Lấy thông tin User** | Gọi `requestUser(id)` từ `useUserStore` để được tự động batching request. | ❌ Không gọi API get user đơn lẻ lặp đi lặp lại trong vòng lặp tin nhắn. |
| **Kiểu dữ liệu sinh viên** | Luôn dùng trường `student_id` (được định nghĩa trong `src/types/user.types.ts`). | ❌ Không dùng lại biến thể cũ `studentID`. |

---
*Tài liệu được tổng hợp cho dự án Chatbot-HCMUS Frontend. Mọi thắc mắc về kiến trúc hãy tham khảo thêm `optimized_plan.md`.*
