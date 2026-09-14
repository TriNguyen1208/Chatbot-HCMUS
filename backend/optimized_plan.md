# KẾ HOẠCH TỐI ƯU HÓA HỆ THỐNG: CHUYỂN ĐỔI FULL-DUPLEX WEBSOCKET & TỐI ƯU LATENCY

> **NGUYÊN TẮC BẤT DI BẤT DỊCH:** Toàn bộ logic nghiệp vụ (business logic), các quy tắc kiểm tra quyền, validation, dữ liệu trả về và tính năng của hệ thống **BẮT BUỘC PHẢI GIỮ NGUYÊN 100%** giống như trước khi optimize. Chỉ thay đổi tầng giao vận (Transport: HTTP $\rightarrow$ WebSocket Full-Duplex) và tầng lưu trữ/bộ nhớ đệm (Storage/Cache/Queue) để tối ưu độ trễ, tuyệt đối không làm thay đổi hành vi nghiệp vụ.

---

### Mục 0: Tối ưu tầng Facade (`conversationFacade`, `messageFacade`)
* **Giữ nguyên giao diện Facade:** Giữ nguyên tên hàm, tham số và giá trị trả về của tất cả các hàm trong `conversation.facade.ts` và `message.facade.ts` để các module khác (như `search`, `media`, `user`) gọi vào không bị lỗi.
* **Tối ưu hóa bên trong ruột:**
  * Thay vì gọi xuyên qua `conversationService.getConversationById()` đâm thẳng vào MongoDB:
  * Facade sẽ kiểm tra trên **Redis** trước:
    * `isUserInConversation(convId, userId)`: Gọi lệnh `SISMEMBER conv:{convId}:members userId` trên Redis (**0.1ms**, 0 lần gọi DB).
    * `getConversationMembers(convId)`: Đọc từ Redis Set `conv:{convId}:members`.
    * Chỉ khi nào Cache Miss mới fallback sang Database và nạp ngược lại vào Redis.

---

### Mục 1: Chuyển đổi WebSocket Full-Duplex theo Module (`message.socket.ts` & `conversation.socket.ts`)
* Chuyển toàn bộ các thao tác thời gian thực từ HTTP REST sang giao tiếp 2 chiều qua WebSocket (Client $\leftrightarrow$ Server), không thông qua HTTP Controller nữa:
  * **[message.socket.ts](file:///Users/ductri0981/Documents/Chatbot-HCMUS/backend/src/modules/message/message.socket.ts):**
    * `new_message`: Gửi tin nhắn mới.
    * `message_edited`: Chỉnh sửa tin nhắn (giữ nguyên logic giới hạn 1 tiếng).
    * `message_recalled`: Thu hồi tin nhắn.
    * `message_reaction_updated`: Thả cảm xúc / reaction tin nhắn.
    * `typing` & `stop_typing`: Trạng thái đang soạn tin nhắn.
    * `mark_delivered` & `mark_read`: Xác nhận đã nhận và đã đọc tin nhắn (Watermarks).
  * **[conversation.socket.ts](file:///Users/ductri0981/Documents/Chatbot-HCMUS/backend/src/modules/conversation/conversation.socket.ts):**
    * `new_conversation`: Tạo cuộc trò chuyện mới.
    * `conversation_updated`: Đổi tên nhóm, avatar nhóm.
    * `members_added`: Thêm thành viên vào nhóm.
    * `members_kicked`: Xóa / kick thành viên khỏi nhóm.
    * `admins_updated`: Bổ nhiệm / giáng chức admin.
    * `member_left`: Thành viên tự rời nhóm.
    * `conversation_blocked` & `conversation_unblocked`: Chặn / bỏ chặn cuộc trò chuyện.
    * `group_disbanded`: Giải tán nhóm.
* **Cơ chế phản hồi:** Sử dụng **Socket.IO Acknowledgement (`ack` callback)** để trả kết quả thành công hoặc lỗi ngay lập tức về cho client người gửi (thay thế cho `res.json()`).

---

### Mục 2: Cơ chế Quản lý Room & User Online ($O(1)$ Broadcasting)
* **1. Khi User vừa kết nối Socket (Online / Reconnect):**
  1. Đọc danh sách conversation IDs từ Redis Set `user:{userId}:convs` (nếu Cache Miss thì query DB và nạp vào Redis thông qua `conversationFacade.getUserConversationIds(userId)`).
  2. Cho socket join đồng loạt vào tất cả các Room tương ứng: `socket.join(rooms)` (với room format `conversation:${convId}`) chỉ mất **~1ms** trong RAM.
* **2. Đồng bộ hóa Room theo Vòng đời Cuộc trò chuyện (Lifecycle Room Management):**
  * **Tạo cuộc trò chuyện mới (`createConversation`):**
    * Ép toàn bộ socket của các thành viên join vào Room mới ngay lập tức qua Redis Adapter:
      ```typescript
      await socketManager.joinGroup(memberIds, created.id);
      ```
    * **Phát sóng `new_conversation` thẳng vào Room** ($O(1)$ Broadcasting), **không cần `emitToUsers`**:
      ```typescript
      socketManager.emitToGroup(created.id, "new_conversation", created);
      ```
    * Toàn bộ thành viên đang online đều nhận được thông báo tạo cuộc trò chuyện mới tức thì thông qua Room chung.
  * **Thêm thành viên mới (`addMember`):**
    * Cho các thành viên mới join vào Room: `await socketManager.joinGroup(membersToAdd, conversationId)`.
    * Gửi `new_conversation` riêng cho từng thành viên mới để họ mở box chat: `socketManager.emitToUser(newMemberId, "new_conversation", updatedConv)`.
    * Phát sự kiện `members_added` trực tiếp vào Room `conversation:${conversationId}` ($O(1)$) cho toàn thể thành viên cũ và mới:
      ```typescript
      socketManager.emitToGroup(conversationId, "members_added", { conversationId, newMemberIds: membersToAdd });
      ```
  * **Xóa / Kick thành viên (`removeMembers`):**
    * Phát sự kiện `members_kicked` vào Room `conversation:${conversationId}` ($O(1)$) **trước khi** rút socket để người bị kick vẫn nhận được thông báo:
      ```typescript
      socketManager.emitToGroup(conversationId, "members_kicked", { conversationId, memberIds: validMemberIds });
      ```
    * Sau đó rút socket người bị kick khỏi Room: `await socketManager.leaveGroup(validMemberIds, conversationId)`.
  * **Thành viên tự rời nhóm (`leaveGroup`):**
    * Phát sự kiện `member_left` vào Room `conversation:${conversationId}` ($O(1)$) trước:
      ```typescript
      socketManager.emitToGroup(conversationId, "member_left", { conversationId, userId });
      ```
    * Sau đó rút socket người rời khỏi Room: `await socketManager.leaveGroup(userId, conversationId)`.
  * **Giải tán nhóm (`disbandGroup`):**
    * Phát sự kiện `group_disbanded` vào Room `conversation:${conversationId}` ($O(1)$) trước:
      ```typescript
      socketManager.emitToGroup(conversationId, "group_disbanded", { conversationId, disbanded_by: adminId, group_name: conv.name });
      ```
    * Sau đó rút socket toàn bộ thành viên khỏi Room: `await socketManager.leaveGroup(memberIds, conversationId)`.
* **3. Xóa sổ hoàn toàn `emitToUsers`, Chỉ dùng $O(1)$ Room Broadcasting:**
  * **Loại bỏ vĩnh viễn hàm `emitToUsers`** (duyệt vòng lặp user IDs) khỏi `SocketManager` và toàn bộ codebase.
  * Toàn bộ sự kiện thời gian thực (Real-time events) được gửi trực tiếp vào Room bằng **1 lệnh duy nhất ($O(1)$)**:
    ```typescript
    socketManager.emitToGroup(conversationId, EVENT_NAME, data);
    // Tương đương: this.io.to(`conversation:${conversationId}`).emit(EVENT_NAME, data);
    ```
    * Áp dụng cho: `new_conversation` (khi tạo mới), `new_message`, `message_edited`, `message_recalled`, `message_reaction_updated`, `watermark_updated`, `conversation_updated`, `admins_updated`, `conversation_blocked`, `conversation_unblocked`.
    * Đối với `typing` / `stop_typing`: Phát tới các socket khác trong Room trừ người gửi bằng `socket.to('conversation:' + convId).emit(...)`.
  * Chỉ dùng `emitToUser(userId, ...)` khi cần gửi tin nhắn đích danh cho đúng 1 user cá nhân (như gửi `new_conversation` cho người vừa được add vào nhóm cũ).

---

### Mục 3: Tối ưu hóa Database (MongoDB Index & Dọn dẹp truy vấn thừa)
1. **Bổ sung Compound Index bắt buộc:**
   * `MessageSchema`:
     * `{ conversation_id: 1, created_at: -1 }`: Tăng tốc độ load tin nhắn từ 100ms+ xuống còn 1-2ms.
     * `{ sender_id: 1 }`: Tối ưu tìm kiếm và kiểm tra quyền gửi.
   * `ConversationSchema`:
     * `{ member_ids: 1, is_active: 1 }`: Tối ưu tìm danh sách cuộc trò chuyện của user.
2. **Dọn dẹp truy vấn lặp lại trong Code:**
   * Trong `message.service.ts`: Sử dụng lại biến `conv.member_ids` đã lấy ở Bước 1, xóa bỏ lời gọi `getConversationMembers` bị lặp ở Bước 5.
   * Loại bỏ hàm kiểm tra CPU/RAM `checkSystemLoad()` ngáng đường trên từng request.

---

### Mục 4: Luồng Gửi Tin Nhắn Siêu Tốc (Optimistic Broadcast) & Cơ chế An Toàn (ACK / NACK)
* **Luồng xử lý (Latency < 3ms):**
  1. Client gửi event `new_message` qua Socket kèm callback ACK.
  2. Server kiểm tra nhanh quyền thành viên bằng `SISMEMBER` trên Redis (**0.1ms**).
  3. Node.js tự sinh `_id` trước: `new Types.ObjectId()`.
  4. **Bắn Socket `new_message` vào Room cho cả nhóm ngay lập tức (1 - 2ms)** $\rightarrow$ Người nhận thấy tin nhắn tức thì.
  5. Gọi callback ACK báo thành công về cho người gửi $\rightarrow$ Client hiển thị tin nhắn đã gửi.
  6. Đẩy dữ liệu tin nhắn vào `fastQueue` (BullMQ) để lưu MongoDB và đồng bộ sang Elasticsearch ngầm phía sau.
* **Cơ chế an toàn (Chống mất dữ liệu khi sập server / lỗi DB):**
  * Job lưu DB trong BullMQ được cấu hình: `attempts: 3`, `backoff: { type: "exponential", delay: 2000 }`. Nếu DB bị nghẽn, BullMQ sẽ tự động thử lại.
  * **Trường hợp lỗi nghiêm trọng (NACK / Rollback):** Nếu sau 3 lần retry mà lưu DB vẫn thất bại:
    * Worker bắn socket event **`message_save_failed`** (NACK) về cho room cuộc trò chuyện kèm `messageId`.
    * Client nhận được event này sẽ đánh dấu tin nhắn bị lỗi (hiển thị icon chấm than đỏ kèm nút *"Thử lại"*).

---

### Mục 5: Kiến trúc Redis Data Modeling mới (Tách Tĩnh - Động, Dẹp bỏ rác)
* **Tách biệt dữ liệu:**
  * **`user:{id}:convs` (Redis Set):** Danh sách các `conversation_id` của user. Dùng để join room lúc online.
  * **`conv:{id}:members` (Redis Set):** Danh sách `userId` thành viên nhóm. Phục vụ validation bằng `SISMEMBER` (0.1ms).
  * **`conv:{id}:admins` (Redis Set):** Danh sách `adminId` của nhóm.
  * **`conv:{id}:meta` (Redis Hash):** Thông tin cơ bản: `{ name, type, is_active, block_by }`.
  * **`conv:{id}:recent` (Redis List):** Lưu 50 tin nhắn mới nhất (`LPUSH` + `LTRIM`). Mở box chat chỉ cần `LRANGE 0 19` là có ngay 20 tin nhắn trong **0.5ms**, không cần query MongoDB.
  * **`watermarks:{convId}` (Redis Hash):** Lưu vị trí đã đọc của từng user: `{ [userId]: { last_delivered_msg_id, last_read_msg_id } }`.
* **Quy tắc vàng:** **TUYỆT ĐỐI KHÔNG DÙNG LỆNH `DEL` XÓA SẠCH CACHE NHÓM KHI CÓ TIN NHẮN MỚI.** Dữ liệu thành viên và quyền hạn luôn được bảo toàn trên RAM.