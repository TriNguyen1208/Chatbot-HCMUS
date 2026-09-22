# Kế Hoạch Đánh Giá & Refactor Hệ Thống (Frontend & Backend)

> **Tài liệu tham chiếu:** [Controlled AI Coding](file:///Users/ductri0981/Documents/Chatbot-HCMUS/.agent/skills/controlled-ai-coding/SKILL.md)  
> **Trạng thái:** Đã phân tích toàn bộ codebase, sẵn sàng triển khai theo quyết định của người quản lý.

---

## 📌 PHẦN 1: BUGS NGHIỆP VỤ CẦN XỬ LÝ (TỪ CHECK.MD)

### 1. Luồng Refresh Token & Redirect ở Middleware
* **Hiện tượng:** Khi người dùng đã hết hạn `accessToken` nhưng vẫn còn `refreshToken` hợp lệ trong cookie, khi truy cập vào trang chủ `/` (trang Login) thì hệ thống **không tự động chuyển hướng vào `/chat`** để kích hoạt cơ chế refresh token âm thầm, mà bắt user ở lại màn hình đăng nhập.
* **Nguyên nhân:** Tại [src/middleware.ts](file:///Users/ductri0981/Documents/Chatbot-HCMUS/frontend/src/middleware.ts#L22-L27):
  ```typescript
  // Chỉ chuyển hướng khi ĐÃ CÓ accessToken hợp lệ
  if (accessToken && isAuthRoute) {
      const chatUrl = new URL("/chat", req.url);
      return NextResponse.redirect(chatUrl);
  }
  ```
  Khi `accessToken` hết hạn (bị xóa hoặc hết hạn cookie), điều kiện này bị sai dù `refreshToken` vẫn còn.
* **Giải pháp:**
  - Nếu ở route `/` và có `refreshToken` (kể cả khi không có `accessToken`), cho phép chuyển hướng vào `/chat` hoặc gọi endpoint `/auth/refresh-token` trước khi render để tái cấp phát `accessToken`.
  - Cần bọc logic xử lý nếu `refreshToken` cũng đã hết hạn/bị thu hồi trên server thì tự động redirect ngược lại `/` và xóa sạch cookie, tránh vòng lặp chuyển hướng vô tận (infinite redirect loop).

---

### 2. Bug Realtime User Presence (Trạng thái Online/Offline không bắn tới người dùng khác)
* **Hiện tượng:** Khi một người dùng kết nối (online) hoặc ngắt kết nối (offline), trạng thái này không được cập nhật hoặc không hiển thị trên giao diện của các bạn bè/thành viên khác.
* **Nguyên nhân tiềm năng cần kiểm tra:**
  1. **Backend Event Room / Target:** Khi user kết nối trong [user.socket.ts](file:///Users/ductri0981/Documents/Chatbot-HCMUS/backend/src/modules/user/user.socket.ts) hoặc [socket.manager.ts](file:///Users/ductri0981/Documents/Chatbot-HCMUS/backend/src/infrastructure/websocket/socket.manager.ts), sự kiện `user_online` / `user_offline` đang được phát tới đâu? (Có broadcast toàn server `io.emit()` hay chỉ phát vào các rooms cuộc trò chuyện của user đó? Nếu phát vào room mà các users khác chưa kịp join room thì sẽ miss event).
  2. **Frontend Handler & Cache Update:** Kiểm tra [presence.handler.ts](file:///Users/ductri0981/Documents/Chatbot-HCMUS/frontend/src/features/chat/socket/handlers/presence.handler.ts) xem sự kiện `user_online` có cập nhật đúng vào TanStack Query cache hoặc user store không, và component UI (Sidebar, FriendBar, ChatHeader) có re-render trạng thái presence hay không.
  3. **Initial Presence Fetch:** Khi mới vào trang chat, client đã fetch danh sách trạng thái online từ Redis/Backend chưa, hay chỉ dựa hoàn toàn vào event socket?

---

## 📌 PHẦN 2: CÁC VẤN ĐỀ VỀ BACKEND

### 1. Unit Test hỏng (`Vitest`) do chuyển sang Write-Behind Caching
* **Vấn đề:** Chạy `npm test` bị fail 2 tests tại [message.service.test.ts](file:///Users/ductri0981/Documents/Chatbot-HCMUS/backend/src/modules/message/tests/services/message.service.test.ts#L95-L115).
  - Đoạn code `checkSystemLoad()` cũ trong [message.service.ts](file:///Users/ductri0981/Documents/Chatbot-HCMUS/backend/src/modules/message/message.service.ts#L77-L86) đã được comment để chuyển sang BullMQ write-behind (`sendMessageFast`), nhưng test case `"should push to queue and NOT emit socket when system is overloaded"` vẫn gọi hàm cũ và kỳ vọng logic cũ dẫn đến crash: `TypeError: Cannot read properties of undefined (reading 'id')`.
* **Giải pháp:**
  - Cập nhật unit test theo đúng flow BullMQ write-behind hiện tại.
  - Cập nhật script `npm test` trong [package.json](file:///Users/ductri0981/Documents/Chatbot-HCMUS/backend/package.json#L11): chuyển thành `"test": "vitest run --exclude '**/dist/**'"` để không chạy lặp lại các file test đã build trong thư mục `dist/`.

### 2. Cảnh báo Docker Compose & Container Orphan
* **Vấn đề:**
  - Thuộc tính `version: '3.8'` trong [docker-compose.yml](file:///Users/ductri0981/Documents/Chatbot-HCMUS/backend/docker-compose.yml#L1) đã lỗi thời trong Docker Compose v2.
  - Container cũ `chatbot-rabbitmq` vẫn đang tồn tại dưới dạng orphan.
* **Giải pháp:**
  - Xóa dòng `version: '3.8'`.
  - Chạy `docker-compose up -d --remove-orphans` để dọn sạch container RabbitMQ thừa.

### 3. Chuẩn hóa Facade (Tránh rò rỉ module)
* **Vấn đề:** Trong [google.strategy.ts](file:///Users/ductri0981/Documents/Chatbot-HCMUS/backend/src/modules/auth/strategies/google.strategy.ts#L10) và [microsoft.strategy.ts](file:///Users/ductri0981/Documents/Chatbot-HCMUS/backend/src/modules/auth/strategies/microsoft.strategy.ts#L10), hàm `getUserRoleFromStudentID` đang được import trực tiếp từ file nội bộ `#@/modules/user/user.hcmus.js`.
* **Giải pháp:** Đưa hàm này vào [user.facade.ts](file:///Users/ductri0981/Documents/Chatbot-HCMUS/backend/src/modules/user/user.facade.ts) hoặc chuyển thành tiện ích chung tại `src/shared/utils/hcmus.util.ts`.

---

## 📌 PHẦN 3: CÁC VẤN ĐỀ VỀ FRONTEND

### 1. Đồng bộ cấu trúc thư mục với đặc tả SKILL.md
* **Vấn đề:** Nhiều thư mục dùng chung đang nằm rải rác ở root `src/` thay vì gom vào `src/shared/`:
  - `src/types/` $\rightarrow$ Đưa về `src/shared/types/` (tách thành `api.types.ts`, `user.types.ts`, `conversation.types.ts`, `message.types.ts`).
  - `src/components/ui/` $\rightarrow$ Đưa về `src/shared/components/ui/`.
  - `src/hooks/useSocket.ts` $\rightarrow$ Đưa về `src/shared/hooks/useSocket.ts`.
  - `src/lib/api.ts` $\rightarrow$ Đưa về `src/shared/services/api.client.ts`.
  - `src/utils/` $\rightarrow$ Đưa về `src/shared/utils/`.
  - `src/providers/` $\rightarrow$ Hợp nhất vào `src/app/providers.tsx` hoặc `src/shared/providers/`.
  - `src/stores/` $\rightarrow$ **Thư mục rỗng (0 file)**, cần xóa bỏ.
  - `src/features/chat/types/index.ts` $\rightarrow$ Chỉ re-export thừa thãi `@/types`, cần dọn dẹp.
  - `src/features/chat/hooks/useChatSocket.ts` $\rightarrow$ Chỉ re-export thừa thãi `../socket/useChatSocket.ts`, cần chuẩn hóa đường dẫn import.

### 2. Tái cấu trúc vị trí Mount Modals (Global Modals vs Private Modals)
* **Quy tắc:**
  - **Global Modals (3 modals):** `UserProfileModal`, `CreateGroupModal`, `ForwardModal` cần được mount tập trung tại [app/(main)/(chat)/layout.tsx](file:///Users/ductri0981/Documents/Chatbot-HCMUS/frontend/src/app/%28main%29/%28chat%29/layout.tsx). Lý do: Cho phép mở bất kỳ lúc nào ngay cả khi `activeConversation` là null (đang ở màn hình `EmptyChatScreen`).
  - **Private Modals:** `KickMemberModal` và `AssignAdminModal` chỉ thuộc quyền quản lý thành viên nhóm trong [ConversationInfo](file:///Users/ductri0981/Documents/Chatbot-HCMUS/frontend/src/features/chat/components/ChatArea/components/ConversationInfo). Hiện tại cả hai đang bị đẩy ra ngoài [ChatScreen.tsx](file:///Users/ductri0981/Documents/Chatbot-HCMUS/frontend/src/features/chat/components/ChatScreen/ChatScreen.tsx#L49-L57).
* **Giải pháp:**
  - Chuyển `UserProfileModal`, `CreateGroupModal`, `ForwardModal` lên mount tại `layout.tsx` của `(chat)`.
  - Chuyển `KickMemberModal`, `AssignAdminModal` về làm Private Component của `ConversationInfo`.

### 3. Cập nhật Next.js 16 Middleware Convention
* **Vấn đề:** Next.js 16 Turbopack cảnh báo:
  `The "middleware" file convention is deprecated. Please use "proxy" instead.`
* **Giải pháp:** Đổi tên [src/middleware.ts](file:///Users/ductri0981/Documents/Chatbot-HCMUS/frontend/src/middleware.ts) thành `src/proxy.ts` (và đổi tên export `export function proxy(req: NextRequest)`).

### 4. Dọn dẹp lỗi Linter ESLint
* **Vấn đề:** `npm run lint` báo 118 lỗi:
  - Lỗi lạm dụng `any` ở [api.ts](file:///Users/ductri0981/Documents/Chatbot-HCMUS/frontend/src/lib/api.ts), [socket.service.ts](file:///Users/ductri0981/Documents/Chatbot-HCMUS/frontend/src/shared/services/socket.service.ts), [upload.service.ts](file:///Users/ductri0981/Documents/Chatbot-HCMUS/frontend/src/shared/services/upload.service.ts).
  - Lỗi React 19 `react-hooks/set-state-in-effect` (gọi `setState` đồng bộ bên trong `useEffect`) tại [SocketProvider.tsx](file:///Users/ductri0981/Documents/Chatbot-HCMUS/frontend/src/providers/SocketProvider.tsx#L28) và [useProfileForm.ts](file:///Users/ductri0981/Documents/Chatbot-HCMUS/frontend/src/features/profile/hooks/useProfileForm.ts#L25).

---

## 🚀 KẾ HOẠCH HÀNH ĐỘNG (ACTION PLAN)

### 🔹 Giai đoạn 1: Sửa 2 Bug Nghiệp Vụ Trọng Tâm (Ưu tiên số 1)
1. **Fix Refresh Token Flow**:
   - Tinh chỉnh middleware/proxy: Khi truy cập route `/` nếu có `refreshToken` hợp lệ thì cho phép chuyển tiếp vào `/chat`.
   - Đảm bảo cơ chế tự động refresh token trong `api.ts` chạy mượt mà và redirect về `/` an toàn nếu refresh thất bại.
2. **Fix Presence Realtime (Online/Offline)**:
   - Rà soát luồng emit `user_online` từ backend (`user.socket.ts` / `socket.manager.ts`).
   - Kiểm tra việc join room và gửi danh sách online ban đầu cho client.
   - Sửa `presence.handler.ts` để cập nhật chuẩn vào cache TanStack Query/store.

### 🔹 Giai đoạn 2: Chuẩn hóa & Dọn dẹp Backend
1. Sửa unit test [message.service.test.ts](file:///Users/ductri0981/Documents/Chatbot-HCMUS/backend/src/modules/message/tests/services/message.service.test.ts) và cấu hình `package.json` test script.
2. Dọn dẹp `docker-compose.yml` (bỏ `version`, dọn orphan container).
3. Export `getUserRoleFromStudentID` qua `user.facade.ts`.

### 🔹 Giai đoạn 3: Tái cấu trúc Modals & Cấu trúc Thư Mục Frontend
1. Chuyển `middleware.ts` $\rightarrow$ `proxy.ts`.
2. Đưa 3 Global Modals ra `(chat)/layout.tsx`, chuyển `KickMemberModal` và `AssignAdminModal` về Private Modals trong `ConversationInfo`.
3. Gom các thư mục `types`, `components/ui`, `hooks`, `lib`, `utils` vào `src/shared/`, xóa folder `src/stores` rỗng và dọn các file re-export thừa.

### 🔹 Giai đoạn 4: Fix Linting & Kiểm Thử Toàn Diện
1. Fix các lỗi `any` và `set-state-in-effect`.
2. Kiểm tra `npm run build` (cả frontend & backend) và `npm test` đảm bảo pass 100%.
