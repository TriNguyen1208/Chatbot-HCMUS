# Kế hoạch triển khai tính năng gửi hình ảnh (Mở rộng High Load)

Mục tiêu: Xây dựng tính năng gửi ảnh qua Backend, nhưng tích hợp thêm cơ chế **Message Queue** và **Cache** để đảm bảo hệ thống không bị sập (crash) khi lượng request đột ngột tăng cao (CPU/RAM vượt ngưỡng).

## User Review Required
Dưới đây là thiết kế kiến trúc mở rộng để xử lý tải cao. Bạn vui lòng xem qua luồng xử lý Queue (dùng BullMQ + Redis) và Cache bên dưới xem đã hợp lý với hệ thống hiện tại chưa nhé.

## 1. Tận dụng Cache (Redis)
Do dự án đã có sẵn `src/infrastructure/redis/redis.ts`, ta sẽ tận dụng nó cho các tác vụ đọc nhiều:
1. **Socket Mapping**: Lưu mapping `userId -> socketId` vào Redis Hash để hỗ trợ scale nhiều instance server (tránh việc lưu in-memory bị mất khi server crash hoặc khi chạy cluster).
2. **Conversation Metadata Cache**: Lưu thông tin cơ bản của Cuộc trò chuyện (thành viên, tên nhóm) vào Redis. Khi upload ảnh, cần check xem người dùng có trong nhóm không -> Đọc từ Redis (RAM) sẽ nhanh hơn truy vấn MongoDB.
3. **Latest Messages Cache**: (Tuỳ chọn) Cache 20 tin nhắn mới nhất của các box chat đang active (sử dụng Redis List hoặc Sorted Set).

## 2. Message Queue (BullMQ) xử lý quá tải
Gửi ảnh yêu cầu Backend nhận file, đẩy qua mạng lên Cloudflare, rồi ghi DB. Tác vụ này tốn nhiều I/O, Network và RAM (nếu dùng memoryStorage).

**Giải pháp: Fallback sang Message Queue khi vượt ngưỡng**
Sử dụng thư viện **BullMQ** (chạy trên nền Redis) để tạo hàng đợi `image-upload-queue`.
Đo lường tải hệ thống bằng module `os` của Node.js (đo CPU usage hoặc `process.memoryUsage()`).

### Luồng xử lý kết hợp (Synchronous vs Asynchronous Queue)
1. **Client** gọi API `POST /api/chat/upload-image`.
2. **Backend (Controller)** kiểm tra tải hệ thống:
   - **Trường hợp Tải Bình Thường (CPU < 80%, RAM < 80%)**:
     - Xử lý đồng bộ (như kế hoạch cũ): Upload lên R2 -> Lưu DB -> Bắn Socket.
     - Trả về `200 OK` kèm dữ liệu tin nhắn.
   - **Trường hợp Quá Tải (CPU >= 80% hoặc RAM >= 80%)**:
     - Multer sẽ ghi file tạm ra ổ đĩa (Disk Storage) thay vì giữ trong RAM để tránh tràn bộ nhớ.
     - Tạo một Job (công việc) ném vào `image-upload-queue` chứa thông tin: `senderId, receiverId, conversationId, tempFilePath, content (caption)`.
     - Trả về `202 Accepted` (Báo cho client: "Hệ thống đang bận, ảnh của bạn đang được xử lý ngầm, sẽ tự hiển thị khi xong").
3. **Queue Worker (Background Job)**:
   - Worker (có thể chạy trên thread khác hoặc server khác) sẽ pop Job ra để xử lý theo thứ tự.
   - Nó đọc file tạm -> Upload Cloudflare -> Lưu MongoDB -> Xóa file tạm.
   - Upload xong, Worker báo hiệu cho Socket Manager thông qua Redis Pub/Sub.
4. **Socket Update**:
   - Socket Manager phát sự kiện `receive_message` cho người nhận.
   - Đồng thời phát sự kiện `upload_success` ngược lại cho người gửi để giao diện cập nhật trạng thái ảnh từ "Đang gửi..." sang "Đã gửi".

## 3. Các cải tiến và Lưu ý quan trọng (Best Practices)

Để hệ thống đạt mức độ **sẵn sàng cho Production (chuyên nghiệp)**, cần áp dụng các tiêu chuẩn sau:

### 3.1. Bảo mật (Security)
- **Xác thực định dạng file (File Validation):** Thêm thuộc tính `fileFilter` trong cấu hình Multer để chặn các `mimetype` không phải là ảnh (ví dụ: file `.exe`, `.sh`). Đảm bảo an toàn tuyệt đối.
- **Giới hạn dung lượng (Size Limit):** Bắt buộc cấu hình `limits: { fileSize: 5 * 1024 * 1024 }` (5MB) trong Multer để tránh tấn công làm cạn kiệt RAM/Disk.
- **Bảo mật Socket.io:** Parse JWT từ `socket.handshake.headers.cookie` thay vì nhận qua tham số kết nối để tránh lộ token trên URL.
- **Rate Limit:** Áp dụng `express-rate-limit` chặt chẽ hơn cho riêng route `/upload-image` (VD: tối đa 10 ảnh/phút/user) để chặn Spam và DDoS.

### 3.2. Hiệu suất (Performance)
- **Nén ảnh trước khi lưu:** Sử dụng thư viện `sharp` nén ảnh trên buffer trước khi đẩy lên Cloudflare (VD: giảm ảnh 5MB xuống còn 300KB). Vừa tiết kiệm dung lượng R2, vừa giúp app chat load ảnh cực nhanh.
- **Tích hợp CDN:** Cấu hình Custom Domain (VD: `cdn.yourdomain.com`) trỏ vào R2 Bucket và bật Proxy của Cloudflare để ảnh được phân phối qua các Edge Server (CDN), giảm thiểu độ trễ khi tải ảnh ở Việt Nam.

### 3.3. Cấu trúc Code (Architecture)
- **Dependency Injection:** Inject `IStorageService` vào thông qua constructor của `ChatService` thay vì khởi tạo cứng bằng `new CloudflareR2Storage()`. Điều này giúp code linh hoạt và dễ viết Unit Test (Mocking).
- **Tách biệt API xử lý File:** Ảnh, Video và Tài liệu (Docs) nên được đẩy vào các API riêng biệt (VD: `/upload-image`, `/upload-video`). Điều này giúp cấu hình Multer linh hoạt hơn (Video dùng diskStorage, Ảnh dùng memoryStorage) và dễ chia luồng xử lý hậu kỳ (Video cần FFmpeg transcode).

### 3.4. Tình huống ngoại lệ (Edge Cases)
- **Dọn rác dữ liệu (Orphaned Files):** Trong quá trình xử lý đồng bộ, nếu đẩy ảnh lên Cloudflare R2 thành công nhưng lưu vào MongoDB bị lỗi (MongoDB sập), cần bọc trong `try/catch` để gọi ngay hàm xoá ảnh trên R2, tránh tạo ra file rác.
- **Đồng bộ Socket:** Với người gửi ảnh, chỉ cần trả về `200 OK` từ API REST là đủ để Frontend báo "Đã gửi". Socket chỉ nên dùng để báo cho người nhận (Receiver) nhằm tối ưu đường truyền.

## Các thay đổi về mã nguồn (Proposed Changes)

- **`src/infrastructure/queue/bullmq.ts` [NEW]**: Khởi tạo Queue `imageUploadQueue` và cấu hình Worker xử lý job đẩy file lên R2.
- **`src/shared/utils/systemMonitor.ts` [NEW]**: Hàm helper kiểm tra xem CPU/RAM hiện tại có đang vượt ngưỡng (threshold) không.
- **`src/modules/chat/chat.routes.ts` [MODIFY]**: Cấu hình thêm `multer.diskStorage` song song với `memoryStorage`. Áp dụng `limits` và `fileFilter`.
- **`src/modules/chat/chat.controller.ts` [MODIFY]**: Thêm logic rẽ nhánh (if/else) dựa trên `systemMonitor` và `try/catch` xoá file rác.
- **`package.json`**: Cài đặt thêm thư viện `bullmq`, `os-utils`, và `sharp`.

## Verification Plan
1. Viết API giả lập tình trạng "Quá tải" (ép threshold = 0%) để test luồng Message Queue hoạt động (ảnh được lưu vào thư mục temp, sau đó worker bốc đi upload và xoá file temp).
2. Kiểm tra Client: Khi API trả về 202, client hiển thị ảnh mờ (loading), vài giây sau nhận được sự kiện Socket `upload_success` thì đổi ảnh thành rõ.
3. Test upload file giả mạo đuôi `.jpg` để xem hệ thống có chặn đúng hay không.
