# Mã nguồn tham khảo toàn tập: Gửi Ảnh với Message Queue (Kiến trúc Chuẩn)

Bản code này là tổng hợp ĐẦY ĐỦ 100% tất cả các file (từ Cloudflare, MongoDB, WebSocket đến BullMQ, Controller), được tổ chức theo chuẩn kiến trúc tách biệt (Dependency Injection) và có kèm chú thích giải thích chi tiết từng dòng.

## 1. Công cụ đo tải hệ thống (System Monitor)

**`src/shared/utils/systemMonitor.ts`**
```typescript
// Import thư viện os mặc định của Node.js để lấy thông tin phần cứng
import os from "os";

// Hàm kiểm tra xem hệ thống có đang bị quá tải không (Trả về true/false)
export const checkSystemLoad = async (): Promise<boolean> => {
    // Tính toán tỷ lệ RAM đang sử dụng
    const totalMem = os.totalmem(); 
    const freeMem = os.freemem();   
    const usedMemRatio = (totalMem - freeMem) / totalMem; 

    // Tính toán tỷ lệ CPU đang sử dụng
    const loadAvg = os.loadavg(); 
    const cpuLoadRatio = loadAvg[0] / os.cpus().length;

    // Ngưỡng quá tải: Nếu RAM xài hơn 80% HOẶC CPU quá 80% công suất
    const RAM_THRESHOLD = 0.8;
    const CPU_THRESHOLD = 0.8;

    return (usedMemRatio >= RAM_THRESHOLD || cpuLoadRatio >= CPU_THRESHOLD);
};
```

## 2. Storage Infrastructure (Strategy Pattern)

**`src/infrastructure/storage/storage.interface.ts`**
```typescript
// Import Buffer từ Node.js để làm việc với dữ liệu nhị phân (ảnh)
import { Buffer } from "node:buffer";

// Giao thức (Interface) chung cho mọi dịch vụ lưu trữ
export interface IStorageService {
    // Hàm đẩy ảnh lên Cloud, trả về đường link public
    uploadImage(fileBuffer: Buffer, fileName: string, mimeType: string): Promise<string>;
    
    // Hàm xoá ảnh (dùng để dọn rác nếu quá trình lưu Database phía sau bị lỗi)
    deleteImage(imageUrl: string): Promise<void>;
}
```

**`src/infrastructure/storage/cloudflare.storage.ts`**
```typescript
// Import các lệnh cần thiết từ AWS SDK (dùng chung được cho R2 vì R2 tương thích S3)
import { S3Client, PutObjectCommand, DeleteObjectCommand } from "@aws-sdk/client-s3";
import { IStorageService } from "./storage.interface.js";
import { v4 as uuidv4 } from "uuid"; // Dùng để tạo chuỗi ngẫu nhiên

export class CloudflareR2Storage implements IStorageService {
    private s3Client: S3Client;
    private bucketName: string;

    constructor() {
        this.bucketName = process.env.R2_BUCKET_NAME || "";
        
        // Khởi tạo kết nối tới Cloudflare
        this.s3Client = new S3Client({
            region: "auto", // R2 bắt buộc là auto
            endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
            credentials: {
                accessKeyId: process.env.R2_ACCESS_KEY_ID || "",
                secretAccessKey: process.env.R2_SECRET_ACCESS_KEY || "",
            },
        });
    }

    async uploadImage(fileBuffer: Buffer, fileName: string, mimeType: string): Promise<string> {
        // Tạo tên file độc nhất để không bị trùng đè lên nhau
        const uniqueFileName = `${uuidv4()}-${fileName}`;
        
        const command = new PutObjectCommand({
            Bucket: this.bucketName,
            Key: uniqueFileName,
            Body: fileBuffer,
            ContentType: mimeType, 
        });

        // Đẩy lên R2
        await this.s3Client.send(command);
        
        const publicUrl = process.env.R2_PUBLIC_URL;
        return `${publicUrl}/${uniqueFileName}`;
    }

    async deleteImage(imageUrl: string): Promise<void> {
        try {
            const publicUrl = process.env.R2_PUBLIC_URL || "";
            // Lấy tên file gốc (Key) từ URL
            const key = imageUrl.replace(`${publicUrl}/`, "");
            
            const command = new DeleteObjectCommand({
                Bucket: this.bucketName,
                Key: key,
            });
            // Xoá file rác trên R2
            await this.s3Client.send(command);
            console.log(`[Storage] Đã dọn dẹp file rác trên Cloudflare: ${key}`);
        } catch (error) {
            console.error(`[Storage] Xoá file thất bại: ${imageUrl}`, error);
        }
    }
}
```

## 3. MongoDB Models (Schema Definition)

**`src/modules/chat/models/message.model.ts`**
```typescript
import mongoose, { Schema } from "mongoose";

const MessageSchema = new Schema({
    sender_id: { type: String, required: true }, 
    conversation_id: { type: String, required: true },
    
    // Nếu type='image', 'content' sẽ đóng vai trò là phần Text Caption đi kèm với bức ảnh.
    content: { type: String }, 
    type: { type: String, enum: ['text', 'file', 'link', 'image', 'ai'], required: true },
    status: { type: String, enum: ['sent', 'received', 'recalled', 'removed'], default: 'sent' },
    resource_url: { type: String }, // Đường link ảnh R2
    
    tag_ids: [{ type: String }]
}, {
    timestamps: true
});

// Đánh Index phức hợp để tối ưu hoá lấy danh sách tin nhắn (Cursor Pagination)
MessageSchema.index({ conversation_id: 1, createdAt: -1, _id: -1 });

export const MessageModel = mongoose.model("Message", MessageSchema);
```

## 4. Message Queue (Chỉ Khai báo Ống nước)

**`src/infrastructure/queue/image.queue.ts`**
```typescript
import { Queue } from "bullmq";
import { redisClient } from "../redis/redis.js"; 

// File này chỉ tạo Queue. Controller sẽ dùng biến imageQueue này để nhét Job vào.
export const imageQueue = new Queue("image-upload-queue", { 
    connection: redisClient as any 
});
```

## 5. Worker Logic (Chạy ẩn dưới nền)

**`src/workers/image.worker.ts`**
```typescript
import { Worker } from "bullmq";
import fs from "fs/promises";
import { redisClient } from "../infrastructure/redis/redis.js"; 
// Chỉ import Types để định nghĩa (Tránh khởi tạo trực tiếp gây lỗi thiết kế)
import type { ChatService } from "../modules/chat/chat.service.js";
import type { SocketManager } from "../infrastructure/websocket/socketManager.js";

// Đóng gói Worker thành một hàm để File Index.ts bơm Service vào
export const startImageWorker = (chatService: ChatService, socketManager: SocketManager) => {
    console.log("[Worker] Khởi động Image Upload Worker...");

    // Worker sẽ túc trực ở 'image-upload-queue', có Job là lôi ra làm
    const worker = new Worker(
        "image-upload-queue",
        async (job) => {
            const { senderId, conversationId, receiverId, content, filePath, fileName, mimeType } = job.data;

            try {
                console.log(`[Worker] Bắt đầu xử lý Job: ${job.id}`);
                
                // 1. Đọc file tạm từ ổ cứng lên RAM
                const fileBuffer = await fs.readFile(filePath);

                // 2. Tái sử dụng hàm của ChatService (Nén ảnh, up R2, lưu DB)
                const message = await chatService.uploadAndSaveImageMessage(
                    senderId, conversationId, fileBuffer, fileName, mimeType, content
                );

                // 3. Xoá file tạm dưới ổ cứng đi
                await fs.unlink(filePath);

                // 4. Báo qua Socket cho cả Sender và Receiver biết đã xong
                socketManager.emitToUser(receiverId, "receive_message", message);
                socketManager.emitToUser(senderId, "upload_success", message);

            } catch (error) {
                console.error(`[Worker] Lỗi Job ${job.id}:`, error);
                // Dù lỗi cũng phải ráng dọn file tạm
                await fs.unlink(filePath).catch(() => {});
                throw error; 
            }
        },
        { connection: redisClient as any }
    );

    return worker;
};
```

## 6. Chat Service (Xử lý nghiệp vụ lõi)

**`src/modules/chat/chat.service.ts`**
```typescript
import { IStorageService } from "../../infrastructure/storage/storage.interface.js";
import { IDatabase } from "../../infrastructure/database/database.interface.js";
import { redisClient } from "../../infrastructure/redis/redis.js"; 
import sharp from "sharp"; // Thư viện nén ảnh

export class ChatService {
    // Dependency Injection: Nhận Storage và IDatabase từ bên ngoài (File Index.ts truyền vào)
    constructor(
        private storageService: IStorageService,
        private db: IDatabase
    ) {}

    async uploadAndSaveImageMessage(
        senderId: string, 
        conversationId: string, 
        fileBuffer: Buffer, 
        fileName: string, 
        mimeType: string,
        caption?: string 
    ) {
        // [TỐI ƯU 1: CACHE] Lấy thông tin Group từ Redis cho nhẹ DB
        const cacheKey = `conversation_metadata:${conversationId}`;
        let conversationMeta = await redisClient.get(cacheKey);

        if (!conversationMeta) {
            // Cache Miss: Query xuống Database thông qua interface IDatabase
            const convData = await this.db.findOne("conversations", { _id: conversationId });
            if (!convData) throw new Error("Không tìm thấy cuộc trò chuyện");
            // Lưu vào Redis (Sống 1 tiếng)
            await redisClient.setex(cacheKey, 3600, JSON.stringify(convData));
        }

        // [TỐI ƯU 2: NÉN ẢNH]
        let processedBuffer = fileBuffer;
        if (mimeType.startsWith('image/')) {
            processedBuffer = await sharp(fileBuffer)
                .resize({ width: 1200, withoutEnlargement: true }) // Khoá chiều ngang tối đa
                .jpeg({ quality: 80 }) // Nén chuẩn JPEG
                .toBuffer();
        }

        // 1. Upload ảnh qua giao diện IStorageService
        const imageUrl = await this.storageService.uploadImage(processedBuffer, fileName, mimeType);
        
        // 2. Lưu Database qua giao diện IDatabase
        try {
            const newMessage = await this.db.insert("messages", {
                sender_id: senderId,
                conversation_id: conversationId,
                type: 'image',
                resource_url: imageUrl,
                content: caption,
                status: 'sent'
            });

            return newMessage;
        } catch (error) {
            // [XỬ LÝ LỖI] Lỗi DB -> Xoá file rác bên Cloudflare ngay lập tức
            await this.storageService.deleteImage(imageUrl);
            throw new Error("Lỗi lưu Database. Đã dọn file rác.");
        }
    }

    // Kéo tin nhắn với Cursor Pagination
    async getMessages(conversationId: string, cursorCreatedAt?: string, cursorId?: string, limit: number = 20) {
        // Ép kiểu 'any' để truyền được các toán tử phức tạp của MongoDB ($lt, $or)
        let conditions: any = { conversation_id: conversationId };

        if (cursorCreatedAt && cursorId) {
            conditions.$or = [
                { createdAt: { $lt: new Date(cursorCreatedAt) } },
                { createdAt: new Date(cursorCreatedAt), _id: { $lt: cursorId } }
            ];
        }

        return await this.db.query("messages", conditions, { 
            limit: limit,
            orderBy: { field: "createdAt", ascending: false } 
        });
    }
}
```

## 7. Controller & Routes (Bộ chia luồng)

**`src/modules/chat/chat.controller.ts`**
```typescript
import { Request, Response } from "express";
import fs from "fs/promises";
// Tạm import trực tiếp ChatService ở đây để demo, thực tế nên dùng DI Container
import { ChatService } from "./chat.service.js";
import { checkSystemLoad } from "../../shared/utils/systemMonitor.js";
import { imageQueue } from "../../infrastructure/queue/image.queue.js";
import { socketManager } from "../../infrastructure/websocket/socketManager.js";
import { CloudflareR2Storage } from "../../infrastructure/storage/cloudflare.storage.js";
import { mongoDB } from "../../infrastructure/database/mongoDBAtlas.js";

const storageService = new CloudflareR2Storage();
const chatService = new ChatService(storageService, mongoDB);

export const uploadImageHandler = async (req: Request, res: Response) => {
    try {
        const file = req.file; 
        const { conversationId, receiverId, content } = req.body; 
        const senderId = "test-sender-id"; 

        if (!file) return res.status(400).json({ error: "Lỗi file." });

        // ĐO LƯỜNG TẢI SERVER
        const isHighLoad = await checkSystemLoad();

        if (isHighLoad) {
            // [SERVER ĐANG QUÁ TẢI] Ném đường dẫn file vào ống nước BullMQ
            await imageQueue.add('upload-job', {
                senderId, conversationId, receiverId, content,
                filePath: file.path, 
                fileName: file.originalname,
                mimeType: file.mimetype
            });

            // Trả về 202 Accepted liền cho User
            return res.status(202).json({ success: true, message: "Đang xử lý ngầm..." });
        } else {
            // [SERVER RẢNH RỖI] Đọc file từ đĩa lên RAM và xử lý liền tay
            const fileBuffer = await fs.readFile(file.path);
            const message = await chatService.uploadAndSaveImageMessage(
                senderId, conversationId, fileBuffer, file.originalname, file.mimetype, content 
            );

            // Xoá file đĩa
            await fs.unlink(file.path);
            // Báo Socket
            if (socketManager) socketManager.emitToUser(receiverId, "receive_message", message);
            
            // Trả về 200 OK
            return res.status(200).json({ success: true, data: message });
        }
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Upload thất bại" });
    }
};
```

**`src/modules/chat/chat.routes.ts`**
```typescript
import { Router } from "express";
import multer from "multer";
import path from "path";
import { uploadImageHandler } from "./chat.controller.js";

const router = Router();

const upload = multer({ 
    // [BẢO MẬT RAM] Luôn ghi thẳng file vào ổ đĩa. Server sẽ không bao giờ sợ sập RAM.
    storage: multer.diskStorage({
        destination: (req, file, cb) => cb(null, 'uploads/'),
        filename: (req, file, cb) => cb(null, Date.now() + path.extname(file.originalname))
    }),
    
    // Giới hạn 5MB cho mỗi ảnh
    limits: { fileSize: 5 * 1024 * 1024 }, 
    
    // Kiểm duyệt đuôi file tránh hacker
    fileFilter: (req, file, cb) => {
        if (file.mimetype.startsWith("image/")) cb(null, true);
        else cb(new Error("Chỉ cho phép ảnh!") as any, false);
    }
}); 

router.post("/upload-image", upload.single("image"), uploadImageHandler);
export default router;
```

## 8. WebSocket (Bảo Mật Bằng Cookie)

**`src/infrastructure/websocket/socketManager.ts`**
```typescript
import { Server as HttpServer } from "http";
import { Server, Socket } from "socket.io";
import cookie from "cookie"; // Thư viện đọc Cookie (npm install cookie)

export class SocketManager {
    private io: Server;
    private userSocketMap = new Map<string, string>(); 

    constructor(server: HttpServer) {
        this.io = new Server(server, {
            cors: {
                origin: "http://localhost:3000", 
                credentials: true // Yêu cầu mở để đọc Cookie
            }
        });

        // Bóc tách JWT từ Cookie thay vì để lộ token trên tham số URL
        this.io.use((socket, next) => {
            const cookies = cookie.parse(socket.handshake.headers.cookie || "");
            const token = cookies.jwt_token; 
            // decode token ở đây...
            next();
        });

        this.io.on("connection", (socket: Socket) => {
            const userId = socket.data.userId || "test-user-id"; 
            // Ánh xạ UserID với SocketID hiện tại
            this.userSocketMap.set(userId, socket.id);
            
            socket.on("disconnect", () => {
                this.userSocketMap.delete(userId);
            });
        });
    }

    // Hàm public để các file khác (Controller, Worker) gọi bắn tin nhắn
    public emitToUser(userId: string, event: string, data: any) {
        const socketId = this.userSocketMap.get(userId);
        if (socketId) {
            this.io.to(socketId).emit(event, data);
        }
    }
}
// Để sử dụng Global (Singleton)
export let socketManager: SocketManager;
export const initSocket = (server: HttpServer) => {
    socketManager = new SocketManager(server);
    return socketManager;
}
```

## 9. App Bootstrap (Nơi hội tụ vạn vật)

File này đóng vai trò là "Nhà máy" lắp ráp các linh kiện rời rạc ở trên thành một khối thống nhất. Nó khởi tạo Database trước, rồi đưa cho ChatService, rồi đưa ChatService cho Worker.

**`src/index.ts`**
```typescript
import express from "express";
import http from "http";
import { CloudflareR2Storage } from "./infrastructure/storage/cloudflare.storage.js";
import { ChatService } from "./modules/chat/chat.service.js";
import { initSocket } from "./infrastructure/websocket/socketManager.js";
import { mongoDB } from "./infrastructure/database/mongoDBAtlas.js";
import { startImageWorker } from "./workers/image.worker.js";
// ... import routes

const app = express();
const server = http.createServer(app);

async function bootstrap() {
    // 1. Kết nối Database & Redis trước tiên
    await mongoDB.connect();
    // await redisClient.connect();

    // 2. Khởi tạo Storage và Bơm DB vào ChatService (Dependency Injection)
    const storageService = new CloudflareR2Storage();
    const chatService = new ChatService(storageService, mongoDB);

    // 3. Khởi tạo Hệ thống WebSocket
    const socketManager = initSocket(server);

    // 4. Kích hoạt Worker chạy ngầm, truyền Service và Socket vào tận miệng cho nó
    startImageWorker(chatService, socketManager);

    // 5. Cấu hình Express
    // app.use("/api/chat", chatRoutes); 
    
    server.listen(3000, () => {
        console.log("🚀 Server đang chạy tại cổng 3000!");
    });
}

bootstrap();
```
