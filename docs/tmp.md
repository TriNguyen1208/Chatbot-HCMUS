# Mã nguồn tham khảo cho tính năng Gửi Ảnh & WebSocket (Bản Cập Nhật IDatabase & Có Comment)

Dưới đây là cấu trúc mã nguồn đã được nâng cấp (áp dụng Interface `IDatabase`, nén ảnh Sharp, dọn rác DB) kèm giải thích chi tiết cho từng dòng code.

## 1. Storage Infrastructure (Strategy Pattern)

**`src/infrastructure/storage/storage.interface.ts`**
```typescript
// Import Buffer từ Node.js để làm việc với dữ liệu nhị phân (ảnh)
import { Buffer } from "node:buffer";

// Giao thức (Interface) chung cho mọi dịch vụ lưu trữ
export interface IStorageService {
    // Hàm đẩy ảnh lên Cloud, trả về đường link public
    uploadImage(fileBuffer: Buffer, fileName: string, mimeType: string): Promise<string>;
    
    // Bổ sung hàm xoá ảnh (dùng để dọn rác nếu quá trình lưu Database phía sau bị lỗi)
    deleteImage(imageUrl: string): Promise<void>;
}
```

**`src/infrastructure/storage/cloudflare.storage.ts`**
```typescript
// Import các lệnh cần thiết từ thư viện AWS SDK (dùng chung được cho Cloudflare R2 vì R2 tương thích S3)
import { S3Client, PutObjectCommand, DeleteObjectCommand } from "@aws-sdk/client-s3";
import { IStorageService } from "./storage.interface.js";
// Dùng thư viện uuid để tạo ra chuỗi ngẫu nhiên
import { v4 as uuidv4 } from "uuid";

// Class thực thi việc giao tiếp với Cloudflare R2
export class CloudflareR2Storage implements IStorageService {
    private s3Client: S3Client;
    private bucketName: string;

    constructor() {
        // Lấy tên Bucket từ biến môi trường
        this.bucketName = process.env.R2_BUCKET_NAME || "";
        
        // Khởi tạo S3Client kết nối tới Cloudflare
        this.s3Client = new S3Client({
            region: "auto", // Bắt buộc là 'auto' với R2
            endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
            credentials: {
                accessKeyId: process.env.R2_ACCESS_KEY_ID || "",
                secretAccessKey: process.env.R2_SECRET_ACCESS_KEY || "",
            },
        });
    }

    // Cài đặt hàm Upload
    async uploadImage(fileBuffer: Buffer, fileName: string, mimeType: string): Promise<string> {
        // Đổi tên file gốc bằng cách nối thêm mã UUID ngẫu nhiên để không bao giờ bị trùng lặp
        const uniqueFileName = `${uuidv4()}-${fileName}`;
        
        // Cấu hình lệnh upload file
        const command = new PutObjectCommand({
            Bucket: this.bucketName,
            Key: uniqueFileName, // Tên file được lưu trên Cloudflare
            Body: fileBuffer,
            ContentType: mimeType, // Cho Cloudflare biết đây là loại file gì (VD: image/png)
        });

        // Đẩy lên R2
        await this.s3Client.send(command);
        
        // Trả về đường link URL để trình duyệt có thể hiển thị ảnh
        const publicUrl = process.env.R2_PUBLIC_URL;
        return `${publicUrl}/${uniqueFileName}`;
    }

    // Cài đặt hàm Xoá (Dọn rác)
    async deleteImage(imageUrl: string): Promise<void> {
        try {
            const publicUrl = process.env.R2_PUBLIC_URL || "";
            // Tách đường link gốc ra, chỉ lấy đúng tên file (Key) ở cuối cùng
            const key = imageUrl.replace(`${publicUrl}/`, "");
            
            // Cấu hình lệnh xoá file
            const command = new DeleteObjectCommand({
                Bucket: this.bucketName,
                Key: key,
            });
            // Gửi lệnh xoá lên R2
            await this.s3Client.send(command);
            console.log(`[Storage] Đã dọn dẹp file rác trên Cloudflare: ${key}`);
        } catch (error) {
            console.error(`[Storage] Xoá file thất bại: ${imageUrl}`, error);
        }
    }
}
```

## 2. MongoDB Models (Schema Definition)

*Lưu ý: Chúng ta dùng `IDatabase` để thao tác DB, nên file này chỉ có tác dụng khai báo cấu trúc Model cho Mongoose tự động tạo Index ở dưới Database, chứ Service sẽ không dùng nó để insert.*

**`src/modules/chat/models/message.model.ts`**
```typescript
import mongoose, { Schema } from "mongoose";

const MessageSchema = new Schema({
    sender_id: { type: String, required: true }, 
    conversation_id: { type: String, required: true },
    
    // [QUAN TRỌNG] 'content' chứa Text chat bình thường.
    // NHƯNG nếu type='image', 'content' sẽ đóng vai trò là phần Text Caption đi kèm với bức ảnh.
    content: { type: String }, 
    
    // Phân loại tin nhắn
    type: { type: String, enum: ['text', 'file', 'link', 'image', 'ai'], required: true },
    status: { type: String, enum: ['sent', 'received', 'recalled', 'removed'], default: 'sent' },
    
    // Đường dẫn ảnh (Lấy từ Cloudflare R2 sau khi upload xong)
    resource_url: { type: String }, 
    
    tag_ids: [{ type: String }]
}, {
    timestamps: true
});

// Đánh Index phức hợp để tối ưu hoá thuật toán lấy danh sách tin nhắn (Cursor Pagination)
// Sắp xếp giảm dần theo conversation_id (để gom nhóm), createdAt (thời gian mới nhất trước) và _id
MessageSchema.index({ conversation_id: 1, createdAt: -1, _id: -1 });

export const MessageModel = mongoose.model("Message", MessageSchema);
```

## 3. Chat Logic (Áp dụng IDatabase)

**`src/modules/chat/chat.service.ts`**
```typescript
import { IStorageService } from "../../infrastructure/storage/storage.interface.js";
import { IDatabase } from "../../infrastructure/database/database.interface.js";
import sharp from "sharp"; // Thư viện nén và chỉnh sửa ảnh tốc độ cao của Node.js

export class ChatService {
    // Kỹ thuật Dependency Injection: Nhận một instance của IStorageService và IDatabase từ bên ngoài
    // Giúp class này hoàn toàn tách biệt khỏi logic của MongoDB hay Cloudflare (dễ viết Unit Test)
    constructor(
        private storageService: IStorageService,
        private db: IDatabase
    ) {}

    // Hàm upload ảnh và ghi vào cơ sở dữ liệu
    async uploadAndSaveImageMessage(
        senderId: string, 
        conversationId: string, 
        fileBuffer: Buffer, 
        fileName: string, 
        mimeType: string,
        caption?: string // Tuỳ chọn, người dùng có thể gửi kèm caption hoặc không
    ) {
        // [Tối ưu hiệu suất] Nén ảnh
        let processedBuffer = fileBuffer;
        
        // Kiểm tra xem có đúng là file ảnh không thì mới nén
        if (mimeType.startsWith('image/')) {
            processedBuffer = await sharp(fileBuffer)
                .resize({ width: 1200, withoutEnlargement: true }) // Không cho ảnh vượt quá 1200px chiều ngang
                .jpeg({ quality: 80 }) // Ép về chuẩn JPEG và nén chất lượng xuống 80%
                .toBuffer();
        }

        // 1. Gửi cái buffer đã được nén lên Storage (Cloudflare R2)
        const imageUrl = await this.storageService.uploadImage(processedBuffer, fileName, mimeType);
        
        // 2. Tiến hành lưu DB và Bọc trong khối try/catch
        try {
            // Thay vì dùng MessageModel.create, ta gọi hàm insert() của interface IDatabase bạn đã thiết kế
            const newMessage = await this.db.insert("messages", {
                sender_id: senderId,
                conversation_id: conversationId,
                type: 'image',
                resource_url: imageUrl,
                content: caption, // Lưu caption đính kèm ảnh vào cùng 1 dòng
                status: 'sent'
            });

            return newMessage;
        } catch (error) {
            // [Xử lý ngoại lệ] Nếu Code chạy tới đây nghĩa là MongoDB bị lỗi (Sập DB, lỗi Cú pháp...)
            // Lúc này ảnh đã nằm trên Cloudflare nhưng bị mồ côi (rác). Gọi hàm xoá ảnh trên R2 để dọn dẹp.
            await this.storageService.deleteImage(imageUrl);
            
            // Báo lỗi ra ngoài cho Controller xử lý tiếp
            throw new Error("Lỗi khi lưu tin nhắn vào Database. Đã dọn dẹp file rác.");
        }
    }

    // Hàm kéo lịch sử chat bằng Cursor Pagination
    async getMessages(conversationId: string, cursorCreatedAt?: string, cursorId?: string, limit: number = 20) {
        // Mặc định IDatabase.query chỉ nhận Partial<T>, nhưng để dùng được toán tử MongoDB ($lt, $or)
        // ta ép kiểu (cast) về `any`.
        let conditions: any = { conversation_id: conversationId };

        // Nếu user truyền vào con trỏ (cursor) của tin nhắn cuối cùng họ đang thấy
        if (cursorCreatedAt && cursorId) {
            conditions.$or = [
                // Trường hợp 1: Tin nhắn được tạo vào một ngày/giờ CŨ HƠN thời gian của Cursor
                { createdAt: { $lt: new Date(cursorCreatedAt) } },
                // Trường hợp 2: Nếu 2 tin nhắn tạo cùng 1 mili-giây thì so sánh _id
                { 
                    createdAt: new Date(cursorCreatedAt), 
                    _id: { $lt: cursorId } 
                }
            ];
        }

        // Gọi DB lấy danh sách thông qua Interface
        return await this.db.query("messages", conditions, { 
            limit: limit,
            orderBy: { field: "createdAt", ascending: false } // Sắp xếp giảm dần theo thời gian tạo
        });
    }
}
```

## 4. API Controllers & Routes

**`src/modules/chat/chat.controller.ts`**
```typescript
import { Request, Response } from "express";
import { ChatService } from "./chat.service.js";
import { CloudflareR2Storage } from "../../infrastructure/storage/cloudflare.storage.js";
import { mongoDB } from "../../infrastructure/database/mongoDBAtlas.js"; // Import biến kết nối DB hiện có của bạn
import { socketManager } from "../../infrastructure/websocket/socketManager.js";

// Khởi tạo Dependency Injection: Bơm CloudflareR2Storage và mongoDB vào cho ChatService sử dụng
const storageService = new CloudflareR2Storage();
const chatService = new ChatService(storageService, mongoDB);

// Hàm Controller hứng Request gọi tới API
export const uploadImageHandler = async (req: Request, res: Response) => {
    try {
        const file = req.file; 
        
        // Lấy dữ liệu gửi kèm từ Client. 'content' chính là dòng chữ Caption.
        const { conversationId, receiverId, content } = req.body; 
        const senderId = "test-sender-id"; // Thực tế phải lấy từ JWT Token

        if (!file) return res.status(400).json({ error: "Không tìm thấy file ảnh hợp lệ." });

        // Truyền mọi thứ xuống Service
        const message = await chatService.uploadAndSaveImageMessage(
            senderId, 
            conversationId, 
            file.buffer,      
            file.originalname,
            file.mimetype,
            content // Caption
        );

        // Sau khi lưu DB xong xuôi, gửi thông báo qua Websocket
        if (socketManager) {
            // Lưu ý: Chỉ bắn cho máy của ReceiverId. Chứ SenderId thì lát nữa hàm res.json() sẽ báo cho họ rồi.
            socketManager.emitToUser(receiverId, "receive_message", message);
        }

        // Trả API về 200 OK báo cho người gửi biết họ đã gửi thành công
        res.status(200).json({ success: true, data: message });
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Xử lý upload ảnh thất bại" });
    }
};
```

**`src/modules/chat/chat.routes.ts`**
```typescript
import { Router } from "express";
import multer from "multer";
import { uploadImageHandler } from "./chat.controller.js";

const router = Router();

// [BẢO MẬT] Cấu hình cực kỳ cẩn thận cho Multer (Middleware hứng file)
const upload = multer({ 
    // Ghi file thẳng vào RAM (MemoryStorage). Tiện vì file ảnh nhẹ (Đã limit 5MB ở dưới).
    storage: multer.memoryStorage(),
    
    // Giới hạn an toàn
    limits: { 
        fileSize: 5 * 1024 * 1024 // NGHIÊM CẤM file vượt quá 5MB. Tránh làm sập RAM.
    },
    
    // Bộ lọc nội dung
    fileFilter: (req, file, cb) => {
        // Chỉ duyệt (Accept) nếu loại file bắt đầu bằng 'image/' (VD: image/jpeg, image/png)
        if (file.mimetype.startsWith("image/")) {
            cb(null, true);
        } else {
            // Chặn đứng các thể loại file .exe, .sh, cố tình mạo danh
            cb(new Error("Chỉ cho phép upload file định dạng hình ảnh!") as any, false);
        }
    }
}); 

// Móc Middleware upload vào route. Gửi qua Controller xử lý.
router.post("/upload-image", upload.single("image"), uploadImageHandler);

export default router;
```

## 5. WebSocket (Bảo Mật Bằng Cookie)

**`src/infrastructure/websocket/socketManager.ts`**
```typescript
import { Server as HttpServer } from "http";
import { Server, Socket } from "socket.io";
// Cần cài đặt thư viện 'cookie' để phân tích chuỗi cookie gửi từ Client (npm install cookie)
import cookie from "cookie";

export class SocketManager {
    private io: Server;
    // Map in-memory đơn giản để lưu SocketID của User (Nếu chạy Cluster nhiều server, phải đổi sang xài Redis Hash)
    private userSocketMap = new Map<string, string>(); 

    constructor(server: HttpServer) {
        this.io = new Server(server, {
            cors: {
                origin: "http://localhost:3000", 
                credentials: true // Bật tính năng cho phép Client đính kèm Cookie/Token
            }
        });

        // [BẢO MẬT] Middleware Socket.io: Dùng để xác thực (Auth)
        this.io.use((socket, next) => {
            // Thay vì Client truyền Token lọt thỏm trên URL (Dễ bị đánh cắp nếu bị soi log),
            // Client sẽ lưu Token vào HTTP-only cookie. Code dưới đây parse cookie ra:
            const cookies = cookie.parse(socket.handshake.headers.cookie || "");
            
            // Lấy token ra (Tuỳ thuộc vào tên cookie bạn dùng lúc login là gì)
            const token = cookies.jwt_token; 
            
            // Giải mã Token để tìm xem User này là ai
            // if (!token) return next(new Error("Authentication error"));
            // const decoded = jwt.verify(token, process.env.SECRET);
            // Gán thông tin UserID tìm được vào socket để hàm ở dưới xài
            // socket.data.userId = decoded.userId;
            
            next();
        });

        // Sự kiện xảy ra khi Client kết nối (Đã qua được cửa bảo vệ auth ở trên)
        this.io.on("connection", (socket: Socket) => {
            // Lấy ID ra xài
            const userId = socket.data.userId || "test-user-id"; 
            
            // Lưu vào sổ tay (Map)
            this.userSocketMap.set(userId, socket.id);
            
            // Lúc user tắt trình duyệt (disconnect), nhớ xoá khỏi sổ tay
            socket.on("disconnect", () => {
                this.userSocketMap.delete(userId);
            });
        });
    }

    // Hàm gọi để vứt tin nhắn tới 1 máy khách
    public emitToUser(userId: string, event: string, data: any) {
        const socketId = this.userSocketMap.get(userId);
        if (socketId) {
            // Nếu có kết nối, gọi io.to().emit()
            this.io.to(socketId).emit(event, data);
        }
    }
}
```

