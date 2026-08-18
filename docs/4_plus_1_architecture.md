# Kiến trúc 4+1 Views (Chatbot HCMUS Backend)

Mô hình 4+1 là một chuẩn công nghiệp để mô tả kiến trúc phần mềm dưới nhiều góc nhìn khác nhau. Dựa trên mã nguồn hiện tại của dự án, dưới đây là bản vẽ chi tiết các views cho Backend của bạn.

---

## 1. Logical View (Góc nhìn Logic)
Góc nhìn này thể hiện các thành phần phần mềm, cách chúng được chia tách và giao tiếp với nhau (chủ yếu là Clean Architecture & Facade Pattern).

```mermaid
classDiagram
    class Controllers {
        +MessageController
        +MediaController
        +ConversationController
    }
    class Services {
        +MessageService
        +MediaService
        +ConversationService
    }
    class Facades {
        +MessageFacade
        +MediaFacade
        +ConversationFacade
    }
    class Repositories {
        +MessageRepository
        +ConversationRepository
    }
    class Infrastructure {
        +MongoDB
        +SocketManager
        +BullMQ (Queue)
    }

    Controllers --> Services : DTO / Payload
    Services --> Repositories : Entities
    Services --> Facades : Cross-module Calls
    Facades --> Repositories : Read/Update logic
    Repositories --> Infrastructure : Database Ops
    Services --> Infrastructure : Event / Queue Ops
```
**Nhận xét:** Kiến trúc chia theo Module-based. Các module giao tiếp với nhau không qua Service trực tiếp mà qua **Facade**, giúp giảm sự phụ thuộc chéo (Coupling).

---

## 2. Process View (Góc nhìn Tiến trình)
Góc nhìn này tập trung vào tính đồng thời, bất đồng bộ và luồng dữ liệu (Concurrency, WebSockets, Hàng đợi).

```mermaid
sequenceDiagram
    participant C as Client (Frontend)
    participant API as Express (HTTP)
    participant WS as Socket.IO (WSS)
    participant Q as Redis Queue (BullMQ)
    participant W as Background Worker

    Note over C, W: Tiến trình xử lý Đa luồng (Multi-process)
    
    C->>API: Gửi Request nặng (VD: Up ảnh)
    API->>Q: Đẩy Job vào Queue (Tách luồng)
    API-->>C: 202 Accepted (Không block luồng chính)
    
    Q->>W: Đẩy Job cho Worker chạy ngầm
    W->>W: Xử lý nặng (Nén ảnh, Up Cloud...)
    
    W->>WS: Emit Event
    WS-->>C: Socket: Hoàn tất tác vụ (URL)
```
**Nhận xét:** Hệ thống không chỉ có HTTP request-response thông thường mà còn kết hợp WebSockets (thời gian thực) và Background Worker (xử lý ngầm qua Redis) để chịu tải cao.

---

## 3. Development View (Góc nhìn Phát triển)
Góc nhìn này mô tả cách thư mục và mã nguồn được tổ chức để các Lập trình viên dễ dàng phối hợp làm việc.

```mermaid
graph TD
    A[src/] --> B[modules/]
    A --> C[infrastructure/]
    A --> D[shared/]
    
    B --> B1[message/]
    B --> B2[media/]
    B --> B3[queue/]
    
    B1 --> B1A["controllers/"]
    B1 --> B1B["services/"]
    B1 --> B1C["repositories/"]
    B1 --> B1D["message.container.ts"]
    
    C --> C1["database/ (MongoDB, Supabase)"]
    C --> C2["websocket/ (SocketManager)"]
    C --> C3["redis/"]
    
    D --> D1["middlewares/"]
    D --> D2["utils/"]
```
**Nhận xét:** Áp dụng **Dependency Injection (DI)** thông qua các file `*.container.ts`. Developer khi làm tính năng mới chỉ cần tạo folder module mới mà không làm ảnh hưởng code cũ.

---

## 4. Physical / Deployment View (Góc nhìn Triển khai)
Góc nhìn này mô tả các thiết bị vật lý, Server, Cloud Services mà Backend cần để hoạt động.

```mermaid
graph LR
    subgraph "VPC / Máy chủ của bạn"
        A[Node.js - Main Server] 
        B[Node.js - Background Worker]
    end
    
    subgraph "Dịch vụ bên thứ 3 (Cloud)"
        C[(MongoDB Atlas)]
        D[(Redis Server)]
        E[Supabase / S3 Bucket]
        F[Cloudflare Stream]
    end
    
    Client((Clients)) -->|HTTPS / WSS| A
    A --> D
    B --> D
    
    A --> C
    B --> C
    B -->|Upload Images| E
    Client -->|Direct Upload| F
    F -->|Webhook| A
```
**Nhận xét:** Cấu trúc phân tán. Máy chủ Node.js đóng vai trò điều phối, trong khi sức tải lưu trữ được đẩy sang các dịch vụ Cloud chuyên biệt (Atlas, Supabase, Cloudflare).

---

## +1. Scenarios / Use Case View (Góc nhìn Kịch bản)
Góc nhìn này kết nối 4 góc nhìn trên lại với nhau thông qua một Kịch bản (Use Case) cụ thể và quan trọng nhất: **"Gửi ảnh khi hệ thống quá tải"**.

```mermaid
stateDiagram-v2
    [*] --> Upload_Anh
    Upload_Anh --> Kiem_Tra_Tai
    
    Kiem_Tra_Tai --> Vao_Queue : CPU/RAM > 80%
    Kiem_Tra_Tai --> Xu_Ly_Ngay : CPU/RAM < 80%
    
    Vao_Queue --> Worker_Xu_Ly
    Xu_Ly_Ngay --> Up_Cloud
    Worker_Xu_Ly --> Up_Cloud
    
    Up_Cloud --> Tra_Ve_URL_Qua_Socket
    
    Tra_Ve_URL_Qua_Socket --> Nhan_Tin
    Nhan_Tin --> Luu_Database
    Luu_Database --> Ban_Socket_Cho_Group
    Ban_Socket_Cho_Group --> [*]
```
**Nhận xét:** Kịch bản này minh họa rõ rệt cách Backend bảo vệ Database và tối ưu hóa trải nghiệm người dùng thông qua sự kết hợp của Queue, Worker và WebSockets.
