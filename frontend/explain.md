# HCMUS Chatbot - Tài Liệu Giải Thích Chi Tiết Toàn Bộ Mã Nguồn Frontend

Tài liệu này cung cấp cái nhìn toàn diện, tường minh và chuyên sâu về cấu trúc kiến trúc, ý nghĩa của từng module, từng file, từng hook, từng hàm xử lý và luồng dữ liệu cốt lõi trong toàn bộ hệ thống Frontend của **Chatbot HCMUS**.

---

## MỤC LỤC

1. [Tổng Quan Kiến Trúc & Stack Công Nghệ](#1-tổng-quan-kiến-trúc--stack-công-nghệ)
2. [Cấu Hình & Khởi Tạo Ứng Dụng (Config & Root Setup)](#2-cấu-hình--khởi-tạo-ứng-dụng-config--root-setup)
   - `src/config/env.schema.ts` & `src/config/env.ts`
   - `src/config/constants.ts`
   - `src/app/layout.tsx`
   - `src/app/providers.tsx`
   - `src/providers/SocketProvider.tsx`
   - `src/providers/QueryProvider.tsx`
3. [Tầng Hạ Tầng Mạng & Tiện Ích Dùng Chung (Network & Shared Services)](#3-tầng-hạ-tầng-mạng--tiện-ích-dùng-chung-network--shared-services)
   - `src/lib/api.ts` (Axios Interceptors, Silent Token Refresh, Http Helper)
   - `src/shared/services/socket.service.ts` (Singleton Socket, ACK Mechanism)
   - `src/shared/services/upload.service.ts` (S3 Multipart Chunk Upload)
   - `src/utils/cn.ts` & `src/utils/formatTime.ts`
4. [Tầng Quản Lý Trạng Thái (State Management & Stores)](#4-tầng-quản-lý-trạng-thái-state-management--stores)
   - `src/features/auth/stores/authStore.ts`
   - `src/features/chat/stores/chatStore.ts`
   - `src/features/chat/stores/userStore.ts` (Batch DataLoader Pattern)
   - `src/features/chat/stores/modalStore.ts`
   - `src/features/chat/stores/searchStore.ts`
5. [Tầng Giao Tiếp API (API Layer)](#5-tầng-giao-tiếp-api-api-layer)
   - `src/features/auth/api/authApi.ts`
   - `src/features/chat/api/conversation.api.ts`
   - `src/features/chat/api/message.api.ts`
   - `src/features/chat/api/media.api.ts`
   - `src/features/chat/api/search.api.ts`
   - `src/features/chat/api/user.api.ts`
   - `src/features/profile/api/profileApi.ts`
6. [Tầng Realtime WebSocket Handlers & Lifecycle](#6-tầng-realtime-websocket-handlers--lifecycle)
   - `src/features/chat/socket/useChatSocket.ts`
   - `src/features/chat/socket/handlers/message.handler.ts`
   - `src/features/chat/socket/handlers/conversation.handler.ts`
   - `src/features/chat/socket/handlers/watermark.handler.ts`
   - `src/features/chat/socket/handlers/presence.handler.ts`
   - `src/features/chat/socket/handlers/typing.handler.ts`
7. [Engine Cập Nhật Cache: `chat-cache.util.ts`](#7-engine-cập-nhật-cache-chat-cacheutilts)
8. [Tầng Giao Diện Người Dùng (Fractal Components & Headless Hooks)](#8-tầng-giao-diện-người-dùng-fractal-components--headless-hooks)
   - `Sidebar` (Header, TabButtonList, FriendBar, FriendMessageList, SidebarProfile)
   - `ChatArea` (ChatHeader, MessageList, MessageItem, ChatInput, ConversationInfo)
   - Hệ thống Modals (Global Modals vs Private Modals)
9. [Feature: Xác Thực (Auth)](#9-feature-xác-thực-auth)
10. [Feature: Dashboard & Profile](#10-feature-dashboard--profile)
11. [Sơ Đồ Các Luồng Nghiệp Vụ Cốt Lõi (Core Data Flows)](#11-sơ-đồ-các-luồng-nghiệp-vụ-cốt-lõi-core-data-flows)

---

## 1. TỔNG QUAN KIẾN TRÚC & STACK CÔNG NGHỆ

Hệ thống Frontend được xây dựng dựa trên các tiêu chuẩn kỹ thuật hiện đại:
* **Framework:** **Next.js 16 (App Router)** + **React 19** (hỗ trợ React Compiler tự động tối ưu memoization).
* **Styling:** **Tailwind CSS v4** kết hợp CSS Variables và Dark Mode (`next-themes`).
* **Kiến trúc Component:** **Fractal Component Pattern** kết hợp mô hình **Presenter + Headless Hook**:
  - `[Name].tsx`: Presenter — Chịu trách nhiệm render JSX thuần túy, Tailwind class, gắn sự kiện giao diện.
  - `use[Name].ts`: Headless Hook — Chứa toàn bộ nghiệp vụ, React state, effect, validation, gọi API hoặc socket.
  - Các component con dùng riêng (private) được đặt ngay trong thư mục con `components/` của component cha.
* **Quản lý trạng thái (State Management):**
  - **Zustand 5**: Quản trị Client State nhẹ nhàng, linh hoạt, không gây re-render dư thừa (`authStore`, `chatStore`, `userStore`, `modalStore`, `searchStore`).
  - **TanStack Query v5 (React Query)**: Quản trị Server State, lưu trữ bộ đệm (cache) phân trang vô hạn (Infinite Query), đồng bộ danh sách hội thoại và tin nhắn.
* **Realtime Core (WebSocket):**
  - Sử dụng kiến trúc **Single Realtime Core (Singleton Socket)** thông qua `socketService`.
  - Toàn bộ hành động Realtime (tạo nhóm, gửi tin, sửa tin, reaction, watermark...) đều sử dụng cơ chế **Socket.IO ACK Callback** (`emitWithAck`) có bảo vệ Timeout và unwrap dữ liệu chuẩn hoá.
* **Data Layering:**
  - Tự động bóc tách envelope phản hồi chuẩn `ApiResponse<T> = { message, data: T }` ngay tại tầng HTTP Client `http.get`, `http.post`, v.v.

---

## 2. CẤU HÌNH & KHỞI TẠO ỨNG DỤNG (CONFIG & ROOT SETUP)

### `src/config/env.schema.ts` & `src/config/env.ts`
* **Ý nghĩa:** Đảm bảo biến môi trường (`process.env`) được kiểm tra kiểu dữ liệu nghiêm ngặt ngay khi ứng dụng khởi chạy.
* **Chi tiết kỹ thuật:**
  - Sử dụng thư viện `zod` để định nghĩa `envSchema`:
    - `NEXT_PUBLIC_API_URL`: URL của backend Node.js (mặc định fallback về `http://localhost:5000`).
    - `NEXT_PUBLIC_GOOGLE_CLIENT_ID`: Khóa OAuth 2.0 xác thực sinh viên qua Google Account.
    - `NEXT_PUBLIC_MICROSOFT_CLIENT_ID` & `NEXT_PUBLIC_MICROSOFT_TENANT_ID`: Cấu hình xác thực MSAL Azure AD cho tài khoản trường `@student.hcmus.edu.vn`.
  - `env.ts` thực thi hàm `envSchema.safeParse(...)`. Nếu biến môi trường thiếu hoặc sai cú pháp, ứng dụng sẽ log chi tiết lỗi ra console giúp lập trình viên phát hiện ngay lập tức thay vì crash ngầm ở runtime.

### `src/config/constants.ts`
* **Ý nghĩa:** Nơi lưu trữ tập trung các giá trị hằng số toàn cục phía giao diện:
  - `DEFAULT_AVATAR`: Đường dẫn ảnh đại diện mặc định cho người dùng hoặc nhóm chưa có ảnh.
  - `ALLOWED_DOMAINS`: Danh sách các domain email được phép đăng nhập (`student.hcmus.edu.vn`, `hcmus.edu.vn`).
  - Cấu hình phân trang: `PAGE_SIZE = 20`, thời gian debounce.

### `src/app/layout.tsx` (Root Layout)
* **Ý nghĩa:** Khung giao diện gốc bọc toàn bộ ứng dụng Next.js.
* **Chi tiết mã nguồn:**
  - Tải Google Font `Inter` với tập ký tự Latinh và Tiếng Việt (`subsets: ["latin", "vietnamese"]`).
  - Gán thẻ `Metadata` chuẩn hóa SEO và hiển thị tab trình duyệt: `"HCMUS Chatbot - Nền tảng nhắn tin & hỗ trợ sinh viên"`.
  - Thẻ `<body>` được gắn class Tailwind chuẩn màu nền và chuyển màu mượt mà: `min-h-screen bg-background text-foreground antialiased transition-colors duration-300`.
  - Render `<Providers>{children}</Providers>` để tiêm tất cả ngữ cảnh toàn cục cho cây component con.

### `src/app/providers.tsx` (Global Providers Wrapper)
* **Ý nghĩa:** Bộ điều phối các Providers tầng cao nhất và kiểm soát phân quyền / điều hướng route ban đầu.
* **Chi tiết các hàm và logic từng khối:**
  - `mounted` state: Kiểm tra component đã mount trên Client chưa nhằm tránh lỗi Hydration Mismatch đặc thù của Next.js SSR.
  - **Khối Effect 1 - Khởi tạo xác thực (`initAuth`):**
    - Khi người dùng vừa mở trang hoặc F5, gọi `authApi.getMe()` để kiểm tra cookie phiên (`accessToken`/`refreshToken`).
    - Nếu thành công: gọi `setUser(user)` nạp thông tin người dùng vào `authStore`.
    - Nếu thất bại: gọi `clearUser()` đưa trạng thái về chưa đăng nhập.
    - Kết thúc: set `isCheckingAuth(false)`.
  - **Khối Effect 2 - Điều hướng bảo vệ Route:**
    - Lắng nghe `isAuthenticated`, `isCheckingAuth` và `pathname`.
    - Khi `isCheckingAuth` kết thúc:
      - Nếu đã đăng nhập (`isAuthenticated = true`) mà đang ở trang login (`/`), tự động chuyển hướng vào màn hình chat (`/chat`).
      - Nếu chưa đăng nhập (`isAuthenticated = false`) mà cố truy cập các trang nội bộ (khác `/`), tự động đẩy về trang chủ (`/`).
  - Cấu trúc bọc Context:
    - `<ThemeProvider>`: Cung cấp Dark / Light theme từ `next-themes`.
    - `<GoogleOAuthProvider>`: Khởi tạo SDK đăng nhập Google với Client ID.
    - `<QueryProvider>`: Khởi tạo TanStack Query.
    - `<SocketProvider>`: Khởi tạo kết nối Realtime WebSocket.

### `src/providers/SocketProvider.tsx`
* **Ý nghĩa:** Quản lý vòng đời kết nối WebSocket thông qua React Context.
* **Chi tiết mã nguồn:**
  - Tạo `SocketContext` chứa `{ socket: Socket | null, isConnected: boolean }`.
  - `useSocketContext()`: Custom hook cho phép các component con trích xuất instance socket và trạng thái kết nối.
  - **Quản lý kết nối theo trạng thái đăng nhập (`useEffect`):**
    - Nếu `!isAuthenticated`: Ngay lập tức gọi `socketService.disconnect()`, xóa state `socket` và set `isConnected = false`.
    - Nếu `isAuthenticated = true`:
      - Gọi `socketService.connect()` để khởi tạo hoặc kích hoạt lại kết nối.
      - Đăng ký 2 sự kiện: `socket.on("connect")` (cập nhật `isConnected = true`) và `socket.on("disconnect")` (cập nhật `isConnected = false`).
      - Hàm cleanup trả về gọi `socket.off(...)` để chống rò rỉ bộ nhớ (memory leak).

### `src/providers/QueryProvider.tsx`
* **Ý nghĩa:** Khởi tạo instance `QueryClient` cho TanStack React Query.
* **Cấu hình tối ưu:**
  - `staleTime: 60 * 60 * 1000` (60 phút): Dữ liệu được coi là còn mới trong thời gian dài vì toàn bộ các cập nhật phát sinh trong thời gian thực đều được Socket Event chủ động đẩy vào cache (thông qua `chatCache`). Không cần thiết phải polling hay refetch liên tục qua HTTP.
  - `refetchOnWindowFocus: false`: Không tự động gọi lại API khi người dùng chuyển qua lại các tab trình duyệt, tránh gây giật lag và giảm tải cho backend MongoDB.
  - `<ReactQueryDevtools initialIsOpen={false} />`: Công cụ debug cache trong môi trường phát triển.

---

## 3. TẦNG HẠ TẦNG MẠNG & TIỆN ÍCH DÙNG CHUNG (NETWORK & SHARED SERVICES)

### `src/lib/api.ts` (Axios Core & Silent Refresh Token)
File này cấu hình instance Axios giao tiếp HTTP với backend, trang bị cơ chế tự động làm mới token ngầm không làm gián đoạn trải nghiệm người dùng.

* **Cấu hình Axios Instance (`api`):**
  - `baseURL: env.apiUrl + "/api"`: Mọi request đều có tiền tố `/api`.
  - `withCredentials: true`: Bắt buộc trình duyệt đính kèm cookie HttpOnly (`access_token`, `refresh_token`) trong mọi request liên domain.
* **Cơ chế Silent Refresh Token (`api.interceptors.response`):**
  - Biến `isRefreshing`: Cờ đánh dấu có tiến trình refresh token đang chạy hay không.
  - Biến `failedQueue`: Mảng lưu trữ các request gửi đi trong lúc token đang được làm mới.
  - Hàm `processQueue(error)`: Lần lượt giải phóng các Promise trong `failedQueue`. Nếu có lỗi thì reject, nếu thành công thì resolve để retry.
  - Hàm `handleForceLogout()`: Xóa sạch thông tin user trong `useAuthStore` và chuyển hướng trình duyệt về trang login `/`.
  - **Luồng chặn lỗi 401 Unauthorized:**
    1. Khi một API trả về mã lỗi 401 và request chưa từng được retry (`!originalRequest._retry`):
    2. Nếu đã có 1 tiến trình refresh đang chạy (`isRefreshing = true`), request hiện tại sẽ được gói vào một `Promise` và đẩy vào `failedQueue`. Khi refresh xong, nó sẽ tự động chạy lại `api(originalRequest)`.
    3. Nếu chưa có tiến trình refresh:
       - Đánh dấu `originalRequest._retry = true` và `isRefreshing = true`.
       - Gửi POST request tới `/api/auth/refresh-token` (trình duyệt tự đính kèm cookie HttpOnly chứa refresh token).
       - Khi backend cấp access token mới: gọi `processQueue(null)` để giải phóng toàn bộ hàng đợi, và chạy lại request ban đầu `api(originalRequest)`.
       - Nếu refresh thất bại (refresh token cũng hết hạn): gọi `processQueue(err)`, kích hoạt `handleForceLogout()`.
       - Trong khối `finally`, reset cờ `isRefreshing = false`.
* **Standardized HTTP Client Wrapper (`http`):**
  - Backend chuẩn hóa phản hồi dưới dạng: `{ message: string, data: T }`.
  - Đối tượng `http` bao gồm các phương thức: `http.get`, `http.post`, `http.put`, `http.patch`, `http.delete`.
  - Mỗi phương thức tự động bóc tách `res.data.data` và trả về trực tiếp `Promise<T>`. Giúp code tại các file API không cần phải viết thủ công `res.data.data` ở mọi nơi.

### `src/shared/services/socket.service.ts` (Singleton WebSocket Service)
Cung cấp Singleton class `SocketService` duy nhất cho toàn ứng dụng, khắc phục triệt để vấn đề "Dual Socket Leak" (rò rỉ kết nối kép).

* **Các phương thức:**
  - `connect(): Socket`:
    - Kiểm tra nếu `this.socket` chưa tồn tại: khởi tạo `io(env.apiUrl)` với `withCredentials: true`, cấu hình transports `["websocket", "polling"]`.
    - Lắng nghe `connect` và `connect_error` để log trạng thái kết nối.
    - Nếu socket đã có nhưng đang ngắt kết nối thì gọi `this.socket.connect()`.
  - `getSocket(): Socket | null`: Lấy socket instance hiện hành.
  - `isConnected(): boolean`: Trả về `true` nếu socket đang kết nối.
  - `disconnect(): void`: Ngắt kết nối và giải phóng instance `this.socket = null`.
  - **`emitWithAck<T>(event: string, data?: any, timeoutMs = 5000): Promise<T>`:**
    - Hàm cốt lõi giúp chuyển đổi mô hình sự kiện Socket bất đồng bộ sang dạng `Promise` có Acknowledgement (ACK).
    - **Bước 1 (Connection Check):** Kiểm tra xem socket đã kết nối chưa. Nếu chưa, tạo một Promise chờ sự kiện `connect` trong tối đa 3 giây. Nếu quá 3s mạng vẫn chưa thông thì ném lỗi thân thiện: *"Mất kết nối WebSocket. Vui lòng kiểm tra lại đường truyền mạng."*
    - **Bước 2 (Emit with Timeout):** Sử dụng tính năng native của Socket.IO: `socket.timeout(timeoutMs).emitWithAck(event, data)`.
    - **Bước 3 (Response Unwrap & Validate):**
      - Nếu server phản hồi object có `success === false`, trích xuất thông báo lỗi `response.message` và ném thành ngoại lệ.
      - Trích xuất `response.data` trả về cho phía gọi.
    - **Bước 4 (Catch Timeout):** Bắt lỗi `TimeoutError` và chuyển thành thông báo tiếng Việt: *"Máy chủ phản hồi quá lâu (Timeout). Vui lòng thử lại."*

### `src/shared/services/upload.service.ts` (S3 Multipart Upload Service)
Chịu trách nhiệm tải ảnh đơn và tải video kích thước lớn lên Cloudflare R2 / AWS S3 bằng kỹ thuật Multipart Upload chia nhỏ file.

* **`uploadImage(file: File)`:**
  - Gọi API `mediaApi.uploadImage(file)`.
  - Trả về `{ url, fileKey }`.
* **`uploadVideoMultipart(file: File, onProgress?: (percent: number) => void)`:**
  - Thuật toán upload chia nhỏ (Chunking) độc lập, không phụ thuộc vào UI:
    1. Thiết lập hằng số `CHUNK_SIZE = 5 * 1024 * 1024` (5MB mỗi phần theo tiêu chuẩn S3).
    2. Tính tổng số phần: `totalChunks = Math.ceil(file.size / CHUNK_SIZE)`.
    3. Giai đoạn 1: Gọi `mediaApi.initMultipartUpload(file.name, file.type)` để backend sinh `uploadId` và `fileKey`.
    4. Giai đoạn 2: Tạo danh sách số thứ tự `partNumbers = [1, 2, ..., totalChunks]` và gọi `mediaApi.getPresignedUrlsForMultipart(...)` để xin danh sách các link Presigned S3 ứng với từng phần.
    5. Giai đoạn 3: Duyệt danh sách các phần và upload song song (`uploadPromises`):
       - Cắt lát dữ liệu nhị phân: `chunk = file.slice(start, end)`.
       - Dùng native `fetch(presignedUrl, { method: "PUT", body: chunk })` đẩy trực tiếp lên Cloudflare R2/S3 (không làm nghẽn băng thông backend Node.js).
       - Trích xuất mã băm `ETag` từ header phản hồi: `res.headers.get("ETag")`.
       - Lưu vào danh sách `uploadedParts = [{ ETag, PartNumber }]`.
       - Cập nhật phần trăm hoàn thành qua callback `onProgress(percent)`.
    6. Chờ toàn bộ các phần upload xong qua `Promise.all(uploadPromises)`.
    7. Giai đoạn 4: Gọi `mediaApi.completeMultipartUpload(fileKey, uploadId, uploadedParts)` để S3 ghép các phần lại thành file video hoàn chỉnh, nhận về đường dẫn xem video `resourceUrl`.

### `src/utils/cn.ts` & `src/utils/formatTime.ts`
* `cn(...inputs)`: Kết hợp `clsx` và `tailwind-merge` để nối class Tailwind CSS có điều kiện mà không bị xung đột (conflict) CSS rule.
* `formatTime(date)`: Định dạng thời gian gửi tin nhắn và hoạt động của người dùng (vừa xong, HH:mm, dd/MM/yyyy).

---

## 4. TẦNG QUẢN LÝ TRẠNG THÁI (STATE MANAGEMENT & STORES)

### `src/features/auth/stores/authStore.ts`
Quản lý trạng thái phiên đăng nhập của người dùng hiện tại, sử dụng middleware `persist` của Zustand để lưu vào `localStorage`.
* **State:**
  - `user`: Thông tin người dùng hiện tại (`User | null`).
  - `isAuthenticated`: `true` nếu đã đăng nhập thành công.
  - `isCheckingAuth`: `true` trong quá trình app đang gọi API `/user/me` xác thực lúc khởi động.
* **Actions:**
  - `setUser(user)`: Lưu thông tin user, bật `isAuthenticated = true`, tắt `isCheckingAuth = false`.
  - `clearUser()`: Đăng xuất, xóa user về `null`, tắt `isAuthenticated = false`.
  - `setCheckingAuth(isChecking)`: Cập nhật trạng thái đang kiểm tra.

### `src/features/chat/stores/chatStore.ts`
Quản lý trạng thái phiên hội thoại đang tương tác trên giao diện màn hình chat.
* **State:**
  - `activeConversation`: Hội thoại đang được chọn để hiển thị nội dung tin nhắn (`Conversation | null`).
  - `showInfoPanel`: Bật/tắt thanh thông tin hội thoại bên phải màn hình.
  - `typingUsers`: Bản đồ lưu trữ danh sách những người đang gõ văn bản theo từng hội thoại: `Record<conversationId, TypingUser[]>`.
  - `editingMessage`: Tin nhắn đang được chọn để chỉnh sửa nội dung (`Message | null`).
* **Actions:**
  - `setActiveConversation(conversation)`: Chuyển đổi phòng chat hiện tại.
  - `toggleInfoPanel()`: Đảo trạng thái hiển thị panel chi tiết hội thoại.
  - `setEditingMessage(message)`: Thiết lập hoặc hủy bỏ trạng thái chỉnh sửa tin nhắn.
  - `addTypingUser(conversationId, userId, name)`: Thêm một user vào danh sách đang gõ của hội thoại (kiểm tra chống trùng lặp `userId`).
  - `removeTypingUser(conversationId, userId)`: Xóa user khỏi danh sách đang gõ khi nhận được sự kiện dừng gõ.

### `src/features/chat/stores/userStore.ts` (Batch DataLoader Pattern)
Giải quyết bài toán N+1 request và giật lag khi danh sách tin nhắn hoặc hội thoại chứa hàng trăm user ID khác nhau. Store này đóng vai trò như một **Client-Side DataLoader**, tự động gom nhóm (batch) các yêu cầu lấy thông tin người dùng trong khoảng thời gian cực ngắn (50ms).
* **State:**
  - `users: Record<string, User>`: Từ điển lưu cache thông tin profile người dùng đã fetch.
  - `pendingIds: string[]`: Hàng đợi chứa các ID đang chờ được gom nhóm để gửi API.
  - `fetchingIds: string[]`: Danh sách ID đang trong quá trình bay request.
  - `failedIds: string[]`: Danh sách ID đã fetch lỗi (để không lặp lại vô tận).
  - `fetchTimeout`: Định thời Debounce.
* **Actions:**
  - `addUser(user)`: Nạp trực tiếp một user vào cache từ điển.
  - **`requestUser(id: string)`:**
    - Kiểm tra nếu ID đã có trong `users`, đang nằm trong `pendingIds`, đang fetch hoặc đã thất bại thì bỏ qua.
    - Đẩy ID vào `pendingIds`.
    - Hủy timeout cũ (nếu có) và thiết lập một `setTimeout(..., 50)` để gọi `_processQueue()`.
  - **`_processQueue()`:**
    - Lấy toàn bộ danh sách `pendingIds` hiện tại, chuyển sang `fetchingIds` và làm rỗng `pendingIds`.
    - Gọi API hàng loạt: `userApi.getUsersByIds(toFetch)`.
    - Khi có kết quả: duyệt qua danh sách và nạp toàn bộ vào `users` object.
    - Xóa các ID này khỏi `fetchingIds`.
  - `updateUserPresence(userId, is_online, last_active)`: Cập nhật nhanh trạng thái trực tuyến / ngoại tuyến của một người dùng trong bộ đệm mà không cần fetch lại cả profile.

### `src/features/chat/stores/modalStore.ts`
Quản lý trạng thái đóng/mở của các Global Modals trong hệ sinh thái Chat:
- `isCreateGroupOpen`: Modal tạo nhóm mới.
- `isAssignAdminModalOpen`: Modal phân quyền quản trị viên nhóm.
- `isKickModalOpen`: Modal trục xuất thành viên khỏi nhóm.
- `isUserProfileModalOpen` & `selectedUserId`: Modal xem hồ sơ cá nhân người dùng.
- `isForwardModalOpen` & `forwardMessage`: Modal chuyển tiếp tin nhắn sang phòng chat khác.

### `src/features/chat/stores/searchStore.ts`
Lưu trữ trạng thái tìm kiếm tin nhắn:
- `targetMessageId`: ID của tin nhắn được người dùng chọn từ kết quả tìm kiếm. Khi giá trị này thay đổi, component danh sách tin nhắn (`MessageList`) sẽ tự động tải ngữ cảnh, cuộn đến vị trí tin nhắn và tạo hiệu ứng chớp sáng (highlight animation).

---

## 5. TẦNG GIAO TIẾP API (API LAYER)

Tầng API hoạt động theo quy tắc:
- **HTTP Query (`http.get`)**: Dùng khi tải trang lần đầu, lấy danh sách lịch sử phân trang (Infinite Query) hoặc tải ngữ cảnh.
- **WebSocket Mutate (`socketService.emitWithAck`)**: Dùng cho toàn bộ các thao tác thay đổi trạng thái (tạo nhóm, gửi tin, sửa tin, reaction, watermark...) để đảm bảo tốc độ phản hồi cực đại ($< 10ms$) và đồng bộ tức thì tới tất cả các client đang online.

### `src/features/auth/api/authApi.ts`
* `googleLogin(idToken)`: Gửi ID Token của Google lên `/api/auth/google` để xác thực và thiết lập session cookie.
* `microsoftLogin(idToken)`: Gửi ID Token của Microsoft Azure AD lên `/api/auth/microsoft`.
* `logout()`: Gọi `/api/auth/logout` để server hủy bỏ refresh token trong Keystore và xóa cookie.
* `logoutAll()`: Gọi `/api/auth/logout-all` đăng xuất khỏi mọi thiết bị.
* `getMe()`: Gọi `/api/user/me` lấy thông tin người dùng sở hữu session hiện tại.

### `src/features/chat/api/conversation.api.ts`
* `getConversations(limit, cursorId, type)`: Lấy danh sách hội thoại theo con trỏ phân trang qua HTTP GET `/api/conversation`.
* `getConversationById(id)`: Lấy chi tiết một hội thoại qua HTTP GET `/api/conversation/:id`.
* `createGroup(name, member_ids)`: Gửi sự kiện socket `new_conversation` với kiểu `group`.
* `createDirectConversation(member_ids)`: Gửi sự kiện socket `new_conversation` với kiểu `utu` (chat 1-1).
* `updateConversation(id, data)`: Cập nhật tên, ảnh nhóm qua socket `conversation_updated`.
* `addMembers(id, member_ids)`: Thêm thành viên mới qua socket `members_added`.
* `removeMembers(id, member_ids)`: Trục xuất thành viên qua socket `members_kicked`.
* `assignAdmins(id, admin_ids)`: Bổ nhiệm quản trị viên qua socket `admins_updated`.
* `leaveGroup(id)`: Rời khỏi nhóm qua socket `member_left`.
* `disbandGroup(id)`: Giải tán nhóm chat qua socket `group_disbanded`.
* `blockConversation(id)` & `unblockConversation(id)`: Chặn hoặc bỏ chặn hội thoại qua socket `conversation_blocked` / `conversation_unblocked`.

### `src/features/chat/api/message.api.ts`
* `getMessages(conversationId, limit, cursorId, search, type)`: Tải lịch sử tin nhắn phân trang ngược qua HTTP GET `/api/message/:conversationId`.
* `getContextMessages(conversationId, messageId, limit)`: Lấy danh sách tin nhắn bao quanh vị trí của một tin nhắn cụ thể qua HTTP GET `/api/message/:conversationId/context/:messageId`.
* `sendMessage(payload)`: Gửi tin nhắn mới qua socket `new_message`.
* `editMessage(id, content)`: Chỉnh sửa nội dung tin nhắn qua socket `message_edited`.
* `recallMessage(id)`: Thu hồi tin nhắn qua socket `message_recalled`.
* `toggleReaction(id, emoji)`: Thả hoặc gỡ cảm xúc icon trên tin nhắn qua socket `message_reaction_updated`.

### `src/features/chat/api/media.api.ts`
* `uploadImage(file)`: Gửi FormData chứa file ảnh lên `/api/media/upload/image`.
* `initMultipartUpload(fileName, fileType)`: Khởi tạo phiên upload video phân đoạn `/api/media/multipart/init`.
* `getPresignedUrlsForMultipart(fileKey, uploadId, parts)`: Xin danh sách presigned URLs `/api/media/multipart/presigned-urls`.
* `completeMultipartUpload(fileKey, uploadId, parts)`: Hoàn tất phiên upload `/api/media/multipart/complete`.
* `getMediaGallery(conversationId, type, limit, cursorId)`: Lấy danh sách file media đã gửi trong phòng chat qua HTTP GET `/api/media/gallery/:conversationId`.

### `src/features/chat/api/search.api.ts`
* `searchMessages(conversationId, query, limit, cursorId)`: Tìm kiếm toàn văn tin nhắn thông qua Elasticsearch backend `/api/search/messages`.

### `src/features/chat/api/user.api.ts`
* `getUsersByIds(ids)`: Lấy danh sách hồ sơ nhiều người dùng cùng lúc qua HTTP POST `/api/user/batch`.
* `getUserById(id)`: Lấy thông tin một người dùng qua HTTP GET `/api/user/:id`.
* `searchUsers(query)`: Tìm kiếm người dùng theo tên hoặc mã số sinh viên.

---

## 6. TẦNG REALTIME WEBSOCKET HANDLERS & LIFECYCLE

Kiến trúc Realtime được thiết kế module hóa hoàn toàn đối xứng với Backend. Mỗi handler chỉ phụ trách đúng 1 domain và **bắt buộc phải trả về hàm cleanup `() => void`** để giải phóng `socket.off()` khi unmount.

### `src/features/chat/socket/useChatSocket.ts` (Hook Điều Phối Trung Tâm)
* **Ý nghĩa:** Hook gắn tại `app/(chat)/layout.tsx` nhằm kích hoạt và giám sát toàn bộ các luồng socket khi người dùng ở trong khu vực chat.
* **Cơ chế hoạt động:**
  1. Lấy `socket` và `isConnected` từ `useSocketContext()`.
  2. Lấy `queryClient` từ TanStack Query và `router` từ Next.js.
  3. **Heartbeat Ping:** Thiết lập `setInterval` định kỳ 29 giây (kèm một độ lệch ngẫu nhiên `jitter` từ 0 - 2000ms để chống nghẽn server khi hàng nghìn client cùng ping một lúc). Gửi `socket.emit("ping")` giữ kết nối xuyên suốt tường lửa/NAT.
  4. **Đăng ký các Sub-Handlers:**
     ```typescript
     const cleanups = [
       registerMessageHandlers(socket, queryClient),
       registerConversationHandlers(socket, queryClient, router),
       registerWatermarkHandlers(socket, queryClient),
       registerPresenceHandlers(socket),
       registerTypingHandlers(socket),
     ];
     ```
  5. **Dọn dẹp (Cleanup):** Khi layout unmount hoặc socket ngắt kết nối, `clearInterval` và thực thi toàn bộ các hàm cleanup trong mảng để gỡ bỏ triệt để các event listener.

### `src/features/chat/socket/handlers/message.handler.ts`
* `onNewMessage(message)`:
  - Kiểm tra nếu tin nhắn gửi đến từ người khác (`message.sender_id !== user.id`):
    - Nếu người dùng đang mở đúng hội thoại đó (`activeConversation?.id === message.conversation_id`), ngay lập tức bắn socket `mark_read` để báo cho đối phương biết mình đã đọc tin nhắn này.
    - Nếu đang ở hội thoại khác hoặc tab khác, bắn socket `mark_delivered` báo đã nhận thành công.
  - Gọi `chatCache.appendNewMessage(queryClient, message)` chèn tin nhắn vào danh sách đang mở.
  - Gọi `chatCache.bumpConversationLastMessage(queryClient, message)` cập nhật tin nhắn cuối và đẩy hội thoại lên đầu danh sách chat bên thanh sidebar.
* `onMessageEdited(data)`: Gọi `chatCache.updateMessageContent(...)` cập nhật nội dung tin nhắn mới và lịch sử chỉnh sửa.
* `onMessageReactionUpdated(data)`: Gọi `chatCache.updateMessageReaction(...)` cập nhật lại danh sách icon cảm xúc.
* `onMessageRecalled(data)`: Gọi `chatCache.markMessageRecalled(...)` đánh dấu tin nhắn đã bị thu hồi.

### `src/features/chat/socket/handlers/conversation.handler.ts`
Xử lý toàn bộ các biến động về cơ cấu và metadata của hội thoại:
* `conversation:new`: Nhận hội thoại mới được tạo, gọi `chatCache.addNewConversation(...)`.
* `conversation:updated`: Cập nhật tên nhóm, ảnh đại diện qua `chatCache.updateConversationInfo(...)`.
* `conversation:disbanded`: Xử lý khi nhóm bị giải tán. Nếu người dùng đang ở trong nhóm này, hiển thị thông báo toast cảnh báo, reset `activeConversation = null` và điều hướng về `/chat`. Đồng thời gọi `chatCache.removeConversation(...)`.
* `conversation:member_added`: Thêm ID thành viên mới vào danh sách `member_ids`.
* `conversation:member_removed`: Xóa thành viên bị kích khỏi danh sách. Nếu chính bản thân bị kích, thông báo và đẩy về `/chat`.
* `conversation:admin_updated`: Cập nhật danh sách quản trị viên nhóm `admin_ids`.
* `conversation:blocked` & `conversation:unblocked`: Cập nhật cờ chặn tin nhắn trong cache.

### `src/features/chat/socket/handlers/watermark.handler.ts`
* `watermark:updated`: Nhận thông tin `{ conversationId, userId, watermark }`.
* Gọi `chatCache.updateConversationWatermarks(queryClient, conversationId, userId, watermark)` để cập nhật con trỏ đọc (`last_read_msg_id`) và con trỏ nhận (`last_delivered_msg_id`).

### `src/features/chat/socket/handlers/presence.handler.ts`
* `presence:user_online` & `presence:user_offline`:
  - Lắng nghe trạng thái trực tuyến của bạn bè.
  - Gọi trực tiếp `useUserStore.getState().updateUserPresence(userId, is_online, last_active)` cập nhật tức thì chấm xanh online trên giao diện.

### `src/features/chat/socket/handlers/typing.handler.ts`
* `typing:start`: Nhận `{ conversationId, userId, name }`, gọi `useChatStore.getState().addTypingUser(...)`.
* `typing:stop`: Nhận `{ conversationId, userId }`, gọi `useChatStore.getState().removeTypingUser(...)`.

---

## 7. ENGINE CẬP NHẬT CACHE: `chat-cache.util.ts`

Tập hợp các hàm thuần túy (Pure Functions) thao tác trực tiếp trên cấu trúc dữ liệu của TanStack Query Cache thông qua phương thức `queryClient.setQueryData`. Điều này mang lại trải nghiệm thời gian thực tuyệt đối: giao diện cập nhật ngay lập tức mà **không cần gọi lại bất kỳ API HTTP nào**.

* **`appendNewMessage(queryClient, message)`:**
  - Nhắm vào key `['messages', message.conversation_id]`.
  - Cập nhật trang dữ liệu đầu tiên (`pages[0]`):
    - Kiểm tra chống trùng lặp ID tin nhắn (đặc biệt khi có Optimistic UI).
    - Thêm tin nhắn mới vào đầu mảng tin nhắn của trang 0.
* **`bumpConversationLastMessage(queryClient, message)`:**
  - Nhắm vào các key danh sách hội thoại: `['conversations']`, `['conversations', 'utu']`, `['conversations', 'group']`.
  - Tìm hội thoại tương ứng trong các trang:
    - Cập nhật `last_message` của hội thoại bằng tin nhắn mới.
    - Cập nhật `updated_at` thành thời điểm tin nhắn gửi.
    - Đưa hội thoại này lên vị trí đầu tiên của trang đầu tiên (`pages[0]`), tạo hiệu ứng hội thoại có tin nhắn mới nhất luôn nhảy lên trên cùng danh sách.
* **`updateMessageContent(queryClient, data)`:**
  - Duyệt qua toàn bộ các trang tin nhắn, tìm tin nhắn có `id === data.messageId`.
  - Thay đổi trường `content = data.content`, `updated_at = data.updated_at` và cập nhật `edit_history`.
* **`updateMessageReaction(queryClient, data)`:**
  - Tìm tin nhắn theo ID và thay thế mảng `reactions` bằng mảng phản ứng mới từ server.
* **`markMessageRecalled(queryClient, data)`:**
  - Đặt cờ `is_recalled = true`, xóa nội dung `content = ""` hoặc thay bằng thông báo *"Tin nhắn đã được thu hồi"*.
* **`addNewConversation(queryClient, conversation)`:**
  - Chèn hội thoại mới vào đầu danh sách `['conversations']`.
* **`updateConversationWatermarks(queryClient, conversationId, userId, watermark)`:**
  - Cập nhật mảng `watermarks` bên trong đối tượng hội thoại được lưu trong cache, lưu lại vị trí tin nhắn đọc/nhận mới nhất của `userId`.
* **`removeConversation(queryClient, conversationId)`:**
  - Lọc và loại bỏ hoàn toàn hội thoại ra khỏi cache danh sách hội thoại khi bị giải tán hoặc người dùng rời nhóm.
* **`addMembersToConversation` / `removeMembersFromConversation` / `updateAdminsInConversation`:**
  - Cập nhật trực tiếp các mảng `member_ids` và `admin_ids` của hội thoại trong cache.

---

## 8. TẦNG GIAO DIỆN NGƯỜI DÙNG (FRACTAL COMPONENTS & HEADLESS HOOKS)

Giao diện áp dụng triệt để quy tắc **Co-location**: Tách biệt rõ ràng giữa Presentation (`.tsx`) và Business Logic Controller (`use[Name].ts`).

### 8.1. Khối Sidebar (`src/features/chat/components/Sidebar/`)
Cột bên trái màn hình chat, cho phép người dùng tìm kiếm, chuyển tab và duyệt danh sách tin nhắn.
* **`Sidebar.tsx` + `useSidebar.ts`:** Khung chính chứa các phân đoạn component con.
* **`Header/`:** Hiển thị thương hiệu Chatbot HCMUS, nút mở modal tạo nhóm và nút tìm kiếm bạn bè.
* **`TabButtonList/` + `useTabButtonList.ts`:**
  - Quản lý 3 tab lọc: "Tất cả" (`all`), "Cá nhân" (`utu`), "Nhóm" (`group`).
  - Khi click chuyển tab: thay đổi route hoặc tham số lọc danh sách hội thoại.
* **`FriendBar/` + `useFriendBar.ts`:** Thanh cuộn ngang hiển thị avatar những bạn bè đang trực tuyến (`is_online = true`) để click chat nhanh.
* **`FriendMessageList/` + `useFriendMessageList.ts`:**
  - Sử dụng hook `useConversationsQuery(type)` để lấy danh sách hội thoại phân trang vô hạn.
  - Tích hợp `useInView` để tự động kích hoạt `fetchNextPage()` khi người dùng cuộn đến cuối danh sách.
  - Render danh sách các thẻ `ChatCard`.
* **`ChatCard/` + `useChatCard.ts`:**
  - Thẻ hiển thị tóm tắt một cuộc trò chuyện: Avatar (người dùng hoặc nhóm), tên hiển thị, snippet tin nhắn cuối, thời gian gửi, số lượng tin nhắn chưa đọc (badge unread).
  - Tự động lấy trạng thái online/offline của đối phương từ `userStore`.
* **`SidebarProfile/` + `useSidebarProfile.ts`:** Hiển thị thông tin profile người dùng hiện tại ở góc dưới cùng bên trái, kèm nút chuyển đổi Dark Mode và nút mở modal cá nhân / đăng xuất.

### 8.2. Khối Khu Vực Chat Chính (`src/features/chat/components/ChatArea/`)
Khu vực trung tâm chiếm diện tích lớn nhất màn hình, bao gồm:

#### `ChatHeader/` + `useChatHeader.ts`
* Hiển thị thông tin người/nhóm đang chat:
  - Nếu là chat 1-1: Hiển thị avatar đối phương, tên, trạng thái trực tuyến ("Đang hoạt động" hoặc "Hoạt động ... phút trước").
  - Nếu là chat nhóm: Hiển thị avatar nhóm, tên nhóm, số lượng thành viên.
* Các nút tác vụ nhanh:
  - Nút tìm kiếm tin nhắn: Kích hoạt chế độ tìm kiếm trong `ConversationInfo`.
  - Nút bật/tắt thanh thông tin hội thoại: Gọi `toggleInfoPanel()`.
  - Nút rời nhóm (nếu là nhóm chat).

#### `MessageList/` + `useMessageList.ts`
* Quản lý luồng hiển thị danh sách tin nhắn:
  - Gọi `useMessagesQuery(activeConversationId)` lấy tin nhắn phân trang ngược (tin mới nhất ở dưới cùng).
  - Tích hợp `react-intersection-observer`: Khi người dùng cuộn lên trên đỉnh danh sách chạm vào `ref`, tự động gọi `fetchNextPage()` để nạp thêm các tin nhắn cũ hơn.
  - **Hiển thị trạng thái đang gõ:** Lọc từ `typingUsers` trong `chatStore` để hiển thị hiệu ứng bong bóng chấm động "X đang soạn tin...".
  - **Thuật toán tính toán vị trí Watermarks (`watermarksByMessageId`):**
    - Sử dụng `useMemo` duyệt qua mảng `watermarks` của hội thoại.
    - Tìm vị trí tin nhắn đọc (`readIdx`) và nhận (`deliveredIdx`) của từng thành viên.
    - Gắn danh sách avatar người đã xem hoặc đã nhận vào đúng tin nhắn tương ứng trong dòng thời gian.
  - **Nhảy đến tin nhắn tìm kiếm (`fetchContextAndScroll`):**
    - Lắng nghe `targetMessageId` từ `searchStore`.
    - Khi có giá trị: gọi `messageApi.getContextMessages` nạp 10 tin nhắn xung quanh vị trí đó vào cache.
    - Tìm phần tử DOM `id="msg-${targetMessageId}"`, gọi `scrollIntoView({ behavior: 'smooth', block: 'center' })` và thêm class highlight nền màu thương hiệu trong 3 giây.

#### `MessageItem/` + `useMessageItem.ts`
* Chịu trách nhiệm hiển thị từng bong bóng tin nhắn (Message Bubble):
  - Phân biệt tin nhắn do chính mình gửi (căn phải, màu chủ đạo) và tin nhắn người khác gửi (căn trái, màu nền kính).
  - Xử lý các loại tin nhắn: Tin nhắn văn bản, hình ảnh, video (kèm trình phát), tin nhắn đã thu hồi (*"Tin nhắn đã được thu hồi"*).
  - Component con `LinkPreview`: Tự động nhận diện URL trong văn bản và render thẻ xem trước liên kết (OpenGraph preview).
  - Hiển thị phản ứng cảm xúc (Reactions Pill) dưới chân tin nhắn.
  - Hover Action Menu: Nút thả cảm xúc (emoji picker), nút sao chép nội dung, nút chỉnh sửa (chỉ cho phép trong vòng 15 phút), nút thu hồi tin nhắn, nút chuyển tiếp (`ForwardModal`).

#### `ChatInput/` + `useChatInput.ts`
* Khung soạn thảo và gửi tin nhắn:
  - Quản lý ô nhập liệu văn bản `content`.
  - **Xử lý sự kiện gõ phím (`emitTyping`):**
    - Mỗi khi người dùng gõ ký tự, gửi socket `typing` kèm tên người gửi.
    - Tạo `setTimeout` 2 giây: Nếu người dùng dừng gõ quá 2s, tự động gửi socket `stop_typing`.
  - **Xử lý đính kèm đa phương tiện:**
    - Hỗ trợ chọn ảnh hoặc video từ máy tính.
    - Nếu là ảnh: Gọi `uploadService.uploadImage` lấy URL và hiển thị thumbnail xem trước.
    - Nếu là video: Gọi `uploadService.uploadVideoMultipart`, hiển thị thanh tiến độ phần trăm upload ($0\% \rightarrow 100\%$).
  - **Gửi tin nhắn (`handleSendMessage`):**
    - Kiểm tra nếu đang ở chế độ sửa tin (`editingMessage`): gọi `messageApi.editMessage(id, content)` và reset state sửa.
    - Nếu gửi mới: đóng gói payload `{ conversation_id, content, image, video }` và gọi `messageApi.sendMessage(payload)`.
    - Làm rỗng ô input, hủy preview media và gửi `stop_typing`.

#### `ConversationInfo/` + `useConversationInfo.ts`
* Panel thông tin chi tiết trượt từ cạnh phải màn hình:
  - **Thanh kéo chỉnh kích thước (Resizable Width Handle):** Cho phép người dùng kéo chuột điều chỉnh độ rộng của panel từ 250px đến 500px.
  - **Chế độ tìm kiếm nội dung tin nhắn (`isSearchMode`):**
    - Tích hợp ô nhập từ khóa tìm kiếm.
    - Gọi API `searchApi.searchMessages` với Elasticsearch backend.
    - Hiển thị danh sách kết quả kèm thời gian; click vào kết quả sẽ set `targetMessageId` để `MessageList` cuộn ngay đến vị trí tin nhắn đó.
  - **Các phân đoạn chức năng (Subcomponents):**
    - `InfoHeader`: Hiển thị avatar lớn, tên, nút chỉnh sửa thông tin nhóm.
    - `MemberList`: Danh sách toàn bộ thành viên, phân biệt "Trưởng nhóm" (Admin) và "Thành viên". Cung cấp menu ngữ cảnh cho Admin để gán quyền hoặc kích thành viên.
    - `MediaGallery`: Xem toàn bộ ảnh và video đã từng được chia sẻ trong phòng chat dưới dạng lưới ô vuông. Click vào ảnh/video mở `MediaViewerModal`.
    - `DangerActions`: Các thao tác nhạy cảm: Chặn người dùng, Rời khỏi nhóm, hoặc Giải tán nhóm.

### 8.3. Hệ Thống Modals (Global Modals vs Private Modals)
Áp dụng nguyên tắc dọn dẹp thư mục Modals để tối ưu hiệu năng và tính đóng gói:
* **Global Modals (`src/features/chat/components/Modals/`):**
  - Những modal có thể được kích hoạt từ nhiều vị trí khác nhau trong app, được mount tại `app/(chat)/layout.tsx`:
    1. `UserProfileModal`: Xem thông tin chi tiết của một sinh viên (khoa, MSSV, email, trạng thái).
    2. `CreateGroupModal`: Tạo nhóm chat mới, tìm kiếm và chọn nhiều bạn bè từ danh bạ sinh viên.
    3. `ForwardModal`: Chuyển tiếp một tin nhắn tới danh sách các phòng chat khác.
* **Private Modals (Modal cục bộ thuộc sở hữu của component cha):**
  - Đặt trực tiếp trong thư mục con của component gọi nó:
    - Thuộc `ConversationInfo`: `BlockUserModal` (xác nhận chặn), `DisbandGroupModal` (xác nhận giải tán), `EditGroupModal` (sửa tên/avatar nhóm), `MediaViewerModal` (trình xem ảnh/video phóng to toàn màn hình).
    - Thuộc `MemberList`: `AssignAdminModal`, `KickMemberModal`.
    - Thuộc `MessageItem`: `ReactionModal` (danh sách chi tiết ai đã thả icon nào).

---

## 9. FEATURE: XÁC THỰC (AUTH)

* **Giao diện Split-screen hiện đại:**
  - `AuthLeftPanel.tsx`: Thể hiện hình ảnh đồ họa thương hiệu trường ĐH Khoa học Tự nhiên TP.HCM, thông điệp chào mừng sinh viên.
  - `AuthRightPanel.tsx`: Form đăng nhập chính.
* **Cơ chế xác thực Single Sign-On (SSO):**
  - **Google OAuth (`useGoogleAuth.ts`):** Sử dụng SDK `@react-oauth/google`. Nhận Google Credential ID Token, gọi `authApi.googleLogin(token)`. Backend sẽ kiểm tra domain email có thuộc `@student.hcmus.edu.vn` hoặc `@hcmus.edu.vn` hay không trước khi phát hành phiên đăng nhập.
  - **Microsoft Azure AD (`useMicrosoftAuth.ts`):** Sử dụng thư viện `@azure/msal-browser` kết nối trực tiếp với hệ thống Office 365 sinh viên của nhà trường.
* **Đăng xuất (`useLogout.ts`):** Gọi `authApi.logout()`, xóa `authStore`, ngắt kết nối WebSocket và đưa người dùng về trang chủ.

---

## 10. FEATURE: DASHBOARD & PROFILE

### Dashboard (`src/features/dashboard/`)
* Cung cấp cổng thông tin tổng quan cho sinh viên HCMUS:
  - `NavBar.tsx`: Thanh điều hướng nhanh giữa Trang chủ, Hộp thư Chat, Lịch học, và Trang cá nhân.
  - `DashboardOverview.tsx`: Thống kê số lượng tin nhắn chưa đọc, thông báo học vụ mới nhất từ nhà trường.
  - `HomePlaceholder.tsx`: Các thẻ chức năng tắt (shortcuts) dẫn vào các phòng chat môn học hoặc cộng đồng sinh viên.

### Profile (`src/features/profile/`)
* Quản lý và cập nhật hồ sơ cá nhân:
  - `ProfileForm.tsx` + `useProfileForm.ts`: Form cập nhật thông tin cá nhân.
  - Cho phép thay đổi ảnh đại diện (avatar), số điện thoại liên lạc, tiểu sử cá nhân.
  - Hiển thị các trường cố định được đồng bộ từ tài khoản trường: Họ và tên, Mã số sinh viên (MSSV - `student_id`), Niên khóa, Khoa/Bộ môn.
  - `profileApi.ts`: Gửi request cập nhật lên `/api/user/profile`.

---

## 11. SƠ ĐỒ CÁC LUỒNG NGHIỆP VỤ CỐT LÕI (CORE DATA FLOWS)

### 11.1. Luồng Gửi Tin Nhắn (Sending Message Flow)

```
[Người dùng nhập tin nhắn & nhấn Gửi]
                  │
                  ▼
         useChatInput.ts
                  │
                  ├── Gọi messageApi.sendMessage(payload)
                  │
                  ▼
         socketService.emitWithAck('new_message', payload)
                  │
                  ├── [WebSocket gửi lên Server Node.js]
                  ├── [Server ghi tức thì vào Redis & Bắn socket ra Room]
                  │
                  ▼ Nhận Acknowledgement (ACK) thành công (<10ms)
         message.handler.ts (Client)
                  │
                  ├── onNewMessage nhận Message chuẩn từ server
                  ├── chatCache.appendNewMessage -> Cập nhật trang đầu React Query Cache
                  └── chatCache.bumpConversationLastMessage -> Đẩy hội thoại lên top Sidebar
                  │
                  ▼
      Giao diện MessageList & Sidebar tự động re-render mượt mà
```

---

### 11.2. Luồng Nhận Tin Nhắn & Đánh Dấu Đã Xem (Receiving & Watermark Flow)

```
[Server phát sự kiện 'new_message' tới Room]
                  │
                  ▼
         message.handler.ts (Client nhận)
                  │
                  ├── Kiểm tra: message.sender_id !== currentUser.id ?
                  │
                  ├── [Trường hợp 1: Đang mở đúng hội thoại đó]
                  │         │
                  │         └── socket.emit('mark_read', { conversationId, messageId })
                  │
                  └── [Trường hợp 2: Đang ở hội thoại khác / nền]
                            │
                            └── socket.emit('mark_delivered', { conversationId, messageId })
                  │
                  ▼
[Server nhận mark_read -> Cập nhật Redis watermark -> Bắn 'watermark:updated']
                  │
                  ▼
         watermark.handler.ts (Client người gửi)
                  │
                  └── chatCache.updateConversationWatermarks(...)
                            │
                            ▼
        useMessageList tính lại watermarksByMessageId
                            │
                            ▼
     Avatar mini của người xem xuất hiện ngay góc tin nhắn
```

---

### 11.3. Luồng Upload Video Dung Lượng Lớn (Multipart S3 Upload Flow)

```
[Người dùng chọn file video .mp4 100MB]
                  │
                  ▼
         uploadService.uploadVideoMultipart(file, onProgress)
                  │
                  ├── 1. mediaApi.initMultipartUpload(...)
                  │         └── Lấy uploadId & fileKey từ Backend
                  │
                  ├── 2. Chia nhỏ file thành các phần (Chunk 5MB)
                  │         └── mediaApi.getPresignedUrlsForMultipart(...)
                  │
                  ├── 3. Chạy song song Promise.all:
                  │         └── fetch(presignedUrl, { method: 'PUT', body: chunk })
                  │         └── Cập nhật onProgress(percent) lên giao diện Input
                  │
                  ├── 4. mediaApi.completeMultipartUpload(parts)
                  │         └── S3 ghép file hoàn chỉnh, trả về resourceUrl
                  │
                  ▼
         messageApi.sendMessage({ video: { file_key, url: resourceUrl } })
```

---

### 11.4. Luồng Tìm Kiếm & Nhảy Đến Ngữ Cảnh Tin Nhắn Cũ (Search & Scroll Context)

```
[Người dùng gõ từ khóa vào ô tìm kiếm trong ConversationInfo]
                  │
                  ▼
         searchApi.searchMessages(query) -> Elasticsearch Backend
                  │
                  ▼ Trả về danh sách kết quả tìm kiếm
[Người dùng click vào một kết quả tin nhắn]
                  │
                  ▼
         searchStore.setTargetMessageId(messageId)
                  │
                  ▼
         useMessageList.ts phát hiện targetMessageId thay đổi
                  │
                  ├── 1. Gọi messageApi.getContextMessages(convId, targetMessageId)
                  │         └── Tải 10 tin nhắn xung quanh vị trí tin nhắn đó
                  │
                  ├── 2. queryClient.setQueryData thay thế tạm thời trang hiển thị
                  │
                  ├── 3. document.getElementById(`msg-${targetMessageId}`).scrollIntoView()
                  │
                  └── 4. Thêm hiệu ứng highlight nền vàng/xanh trong 3 giây
```

---

## 12. TỔNG KẾT

Kiến trúc Frontend của **Chatbot HCMUS** được thiết kế chặt chẽ theo mô hình phân tầng:
1. **Presentation Layer**: Các component thuần túy hiển thị, trong sạch và tuân thủ Fractal Architecture.
2. **Controller Layer**: Các Headless Custom Hooks quản lý vòng đời, state và hiệu ứng.
3. **State & Cache Layer**: Sự kết hợp hoàn hảo giữa **Zustand** (cho client state nhẹ nhàng) và **TanStack Query** (cho server state mạnh mẽ, kết hợp bộ tiện ích `chatCache` thuần túy).
4. **Realtime Core**: Kiến trúc **Single Realtime Core** với Socket.io ACK Callback và đối xứng handlers, đảm bảo độ trễ thấp nhất và triệt tiêu hoàn toàn hiện tượng rò rỉ kết nối.
5. **Data Transfer Layer**: Tối ưu hóa tuyệt đối với kỹ thuật **Batch DataLoader** phía client và **S3 Multipart Chunking** độc lập.

---

## 13. RÀ SOÁT CÁC VỊ TRÍ SỬ DỤNG TRỰC TIẾP `queryClient` NGOÀI HANDLERS & ĐỀ XUẤT TỐI ƯU

### 13.1. Nguyên Tắc Thiết Kế (Architectural Guidelines)
Theo kiến trúc Single Source of Truth và Encapsulation, **`queryClient` không nên bị gọi tùy tiện ở các UI Components hay Controller Hooks** để `setQueryData` / `getQueriesData` thủ công. Toàn bộ logic can thiệp vào cấu trúc in-memory cache của TanStack Query phải được đóng gói tập trung vào file tiện ích [`chat-cache.util.ts`](file:///Users/ductri0981/Documents/Chatbot-HCMUS/frontend/src/features/chat/utils/chat-cache.util.ts) (`chatCache`).

### 13.2. Danh Sách Các Vị Trí Dùng Trực Tiếp `queryClient` (Ngoài file `.handler.ts` và `chat-cache.util.ts`)

| STT | File | Dòng | Mục đích hiện tại | Đánh giá kiến trúc | Đề xuất hướng sửa |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **1** | `src/features/chat/components/ChatArea/components/ConversationInfo/ConversationInfo.tsx` | 181 | Tự viết hàm `updateQueryCache` lặp qua các `pages` để cập nhật thông tin nhóm sau khi đóng `EditGroupModal`. | **Vi phạm nguyên tắc**. Duplicate logic và tự can thiệp raw cache ở tầng Presentation Component. | Thay bằng hàm có sẵn `chatCache.updateConversationInfo(queryClient, updatedConv)` hoặc để socket `conversation_updated` tự xử lý. |
| **2** | `src/features/chat/components/ChatArea/components/ConversationInfo/useConversationInfo.ts` | 19, 254 | Khởi tạo `useQueryClient()` và export ra đối tượng return `queryClient`. | **Thừa thãi**. Chỉ tồn tại để truyền `queryClient` xuống cho `ConversationInfo.tsx` ở mục 1. | Xóa bỏ `useQueryClient()` và không return `queryClient` trong hook này nữa. |
| **3** | `src/features/chat/components/Messages/MessageList/useMessageList.ts` | 15, 112, 137 | Khi click vào kết quả tìm kiếm tin nhắn, hook gọi `queryClient.setQueryData(["messages", convId], ...)` để ghi đè danh sách tin nhắn ngữ cảnh. | **Cần đóng gói**. Hook UI đang can thiệp trực tiếp vào cấu trúc phân trang `{ pages, pageParams }` của tin nhắn. | Tạo hàm `chatCache.setContextMessages(queryClient, convId, messages)` trong `chat-cache.util.ts` và gọi qua hàm này. |
| **4** | `src/features/chat/hooks/useChatScreen.ts` | 16, 42, 68, 109 | Gọi `queryClient.getQueriesData({ queryKey: ['conversations'] })` để lặp qua cache tìm conversation theo `cId` hoặc `receiverId` nhằm set `activeConversation` tức thì. | **Trùng lặp code**. Logic duyệt raw cache `{ pages: Conversation[][] }` bị lặp lại ở nhiều nơi. | Tạo hàm helper `chatCache.findConversationInCache(queryClient, convIdOrReceiverId)` để tái sử dụng. |
| **5** | `src/features/chat/components/Sidebar/components/FriendBar/components/Search/useSearchItem.ts` | 19, 33, 63 | Gọi `queryClient.getQueriesData({ queryKey: ['conversations'] })` để kiểm tra xem đã có cuộc trò chuyện 1-1 với user được tìm kiếm hay chưa. | **Trùng lặp code**. Lặp lại y hệt logic đọc cache của `useChatScreen.ts`. | Dùng chung hàm helper `chatCache.findUtuConversation(queryClient, targetUserId)` từ `chatCache`. |
| **6** | `src/features/chat/socket/useChatSocket.ts` | 15, 33-35, 44 | Gọi `useQueryClient()` để tiêm `queryClient` vào các socket handlers (`registerMessageHandlers`, `registerConversationHandlers`, `registerWatermarkHandlers`). | **Hợp lệ (Điểm tiêm phụ thuộc)**. Đây là cầu nối trung gian duy nhất giữa React Context và Socket Handlers. | Giữ nguyên. |
| **7** | `src/providers/QueryProvider.tsx` | 8, 21 | Khởi tạo `new QueryClient()` và bọc `<QueryClientProvider client={queryClient}>` ở Root Layout. | **Hợp lệ (Root Config)**. | Giữ nguyên. |

---

### 13.3. Đề Xuất Chi Tiết Cách Refactor Cho Từng Vị Trí

#### 1. Tại `ConversationInfo.tsx`:
* **Hiện tại (Dòng 180 - 196):**
```tsx
const updateQueryCache = (queryKey: any[]) => {
  info.queryClient.setQueryData(queryKey, (oldData: any) => {
    if (!oldData || !oldData.pages) return oldData;
    return {
      ...oldData,
      pages: oldData.pages.map((page: any[]) =>
        page.map((conv: any) =>
          conv.id === updatedConv.id ? { ...conv, ...updatedConv } : conv
        )
      ),
    };
  });
};
```
* **Đề xuất sửa:**
Xóa bỏ hoàn toàn hàm `updateQueryCache` tự viết trên. Vì khi cập nhật nhóm thành công, backend đã tự động emit socket `conversation_updated`, và [`conversation.handler.ts`](file:///Users/ductri0981/Documents/Chatbot-HCMUS/frontend/src/features/chat/socket/handlers/conversation.handler.ts#L95-L97) đã tự động cập nhật cache thông qua `chatCache.updateConversationInfo(queryClient, updatedConv)`.
Nếu muốn Optimistic update ngay lập tức tại chỗ trước khi socket phản hồi:
```tsx
onSuccess={(updatedConv) => {
  chatCache.updateConversationInfo(queryClient, updatedConv);
  info.setShowEditGroupModal(false);
}}
```

#### 2. Tại `useMessageList.ts`:
* **Hiện tại (Dòng 112 - 117):**
```typescript
queryClient.setQueryData(["messages", convId], () => {
  return {
    pages: [contextMessages],
    pageParams: [undefined],
  };
});
```
* **Đề xuất sửa:**
Đóng gói vào `chatCache` trong `chat-cache.util.ts`:
```typescript
// Thêm vào chatCache:
setContextMessages: (queryClient: QueryClient, conversationId: string, messages: Message[]) => {
  queryClient.setQueryData(["messages", conversationId], {
    pages: [messages],
    pageParams: [undefined],
  });
}
```
Tại `useMessageList.ts` chỉ cần gọi:
```typescript
chatCache.setContextMessages(queryClient, convId, contextMessages);
```

#### 3. Tại `useChatScreen.ts` & `useSearchItem.ts`:
* **Hiện tại:** Cả hai file đều tự viết vòng lặp duyệt cache:
```typescript
const allCaches = queryClient.getQueriesData<{ pages: Conversation[][] }>({ queryKey: ['conversations'] });
for (const [, cacheData] of allCaches) {
  for (const page of cacheData.pages) {
    // ... tìm conversation
  }
}
```
* **Đề xuất sửa:**
Tạo 2 hàm truy vấn cache tập trung trong `chatCache`:
```typescript
// Thêm vào chatCache:
findConversationById: (queryClient: QueryClient, conversationId: string): Conversation | undefined => {
  const allCaches = queryClient.getQueriesData<{ pages: Conversation[][] }>({ queryKey: ['conversations'] });
  for (const [, cacheData] of allCaches) {
    if (!cacheData?.pages) continue;
    for (const page of cacheData.pages) {
      const found = page.find((c) => c.id === conversationId);
      if (found) return found;
    }
  }
  return undefined;
},

findUtuConversationByMemberId: (queryClient: QueryClient, targetUserId: string): Conversation | undefined => {
  const allCaches = queryClient.getQueriesData<{ pages: Conversation[][] }>({ queryKey: ['conversations'] });
  for (const [, cacheData] of allCaches) {
    if (!cacheData?.pages) continue;
    for (const page of cacheData.pages) {
      const found = page.find((c) => c.type === 'utu' && c.member_ids?.includes(targetUserId));
      if (found) return found;
    }
  }
  return undefined;
}
```
Khi đó, trong `useChatScreen.ts` và `useSearchItem.ts` chỉ cần gọi:
```typescript
const existing = chatCache.findConversationById(queryClient, cId);
// hoặc
const existingUtu = chatCache.findUtuConversationByMemberId(queryClient, receiverId);
```
Code trở nên sáng sủa, sạch sẽ, không bị lặp lại cấu trúc `pages` của TanStack Query ở bất kỳ file view/hook nào.
