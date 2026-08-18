# Triển khai Cloudflare Stream (Direct Creator Upload)

Tài liệu này hướng dẫn cách áp dụng Cloudflare Stream vào hệ thống Chatbot với luồng **Direct Creator Upload**, được viết lại **chuẩn 100% theo kiến trúc** (Controller-Service-Container / DI Pattern) hiện tại của dự án. 

## Quy trình tổng quan

1. **Client** gọi API Backend để xin quyền upload.
2. **Backend** gọi Cloudflare API để xin một `uploadURL` dùng một lần. Trả URL này cho Client.
3. **Client** dùng HTTP POST/PUT tải file video thẳng lên `uploadURL` của Cloudflare.
4. **Cloudflare** tự động mã hóa video. Khi xong, Cloudflare bắn Webhook về Backend.
5. **Backend** nhận Webhook, cập nhật database và dùng `socketManager` báo cho các Client.

---

## 1. Cấu hình Backend (Node.js)

Thêm biến môi trường vào file `.env`:
```env
CLOUDFLARE_ACCOUNT_ID=your_account_id_here
CLOUDFLARE_API_TOKEN=your_api_token_here
CLOUDFLARE_WEBHOOK_SECRET=your_webhook_secret_here
```

### 1.1 Tích hợp vào Service (`message.service.ts`)

Toàn bộ logic giao tiếp với API bên thứ 3 nên nằm ở Service (hoặc tầng Facade) để giữ cho Controller thật sạch.

```typescript
// backend/src/modules/message/services/message.service.ts
import createHttpError from "http-errors";
import { socketManager } from "#@/infrastructure/websocket/socket-manager.js";
// ... (Các import cũ)

export class MessageService {
    // ... (Constructor cũ)

    /**
     * Tạo Pre-signed URL từ Cloudflare Stream
     */
    async createCloudflareUploadUrl() {
        const accountId = process.env.CLOUDFLARE_ACCOUNT_ID;
        const apiToken = process.env.CLOUDFLARE_API_TOKEN;

        if (!accountId || !apiToken) {
            throw createHttpError.InternalServerError("Thiếu cấu hình Cloudflare");
        }

        const body = {
            maxDurationSeconds: 3600, // Video dài tối đa 1 tiếng
            expiry: new Date(Date.now() + 15 * 60 * 1000).toISOString(), // Link hết hạn sau 15p
            requireSignedURLs: false,
        };

        const response = await fetch(`https://api.cloudflare.com/client/v4/accounts/${accountId}/stream/direct_upload`, {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${apiToken}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(body)
        });

        const data = await response.json();

        if (!data.success) {
            throw createHttpError.BadGateway(data.errors[0]?.message || 'Lỗi kết nối tới Cloudflare');
        }

        return {
            uploadUrl: data.result.uploadURL,
            uid: data.result.uid
        };
    }

    /**
     * Xử lý Webhook gửi từ Cloudflare Stream
     */
    async handleCloudflareWebhook(payload: any, signatureHeader?: string) {
        // Bước Xác thực chữ ký webhook (Webhook Signature Verification)
        const webhookSecret = process.env.CLOUDFLARE_WEBHOOK_SECRET;
        if (webhookSecret && signatureHeader) {
            // signatureHeader có dạng: time=1230811200,sig1=60493ec...
            const sigArr = signatureHeader.split(',');
            const timeStr = sigArr.find(s => s.startsWith('time='))?.split('=')[1];
            const sig1 = sigArr.find(s => s.startsWith('sig1='))?.split('=')[1];

            if (timeStr && sig1) {
                // Tạo chuỗi đối chiếu và băm (HMAC-SHA256)
                const sourceString = `${timeStr}.${JSON.stringify(payload)}`;
                const crypto = await import('crypto');
                const expectedSig = crypto.createHmac('sha256', webhookSecret).update(sourceString).digest('hex');
                
                if (expectedSig !== sig1) {
                    throw new Error("Invalid Webhook Signature - Yêu cầu bị từ chối!");
                }
            }
        }

        const { uid, status } = payload;

        // Chỉ quan tâm khi Cloudflare báo là encode 'ready'
        if (status.state === 'ready') {
            const streamUrl = `https://customer-<YOUR_ID>.cloudflarestream.com/${uid}/manifest.m3u8`;
            const thumbnailUrl = `https://customer-<YOUR_ID>.cloudflarestream.com/${uid}/thumbnails/thumbnail.jpg`;

            // Lưu ý: Bạn cần khai báo thêm method updateByCloudflareUid trong MessageRepository
            /*
            const message = await this.messageProcessorFactory.messageRepo.updateByCloudflareUid(uid, {
                status: 'sent', 
                content: streamUrl,
                // metadata: { thumbnail: thumbnailUrl } // Cần thêm field này ở Entity nếu cần
            });

            // Bắn socket báo cho các thành viên trong box chat
            if (message) {
                socketManager.emitToGroup(message.conversation_id, "new_message", message);
            }
            */
            console.log(`[Cloudflare] Video ${uid} đã sẵn sàng`);
        }
    }
}
```

### 1.2 Tích hợp vào Controller (`message.controller.ts`)

Controller theo chuẩn dự án: gọi Service, trả về qua format `apiResponse`.

```typescript
// backend/src/modules/message/controllers/message.controller.ts
import { apiResponse } from "#@/shared/utils/api-response.js";
import type { Request, Response, NextFunction } from "express";
import type { MessageService } from "../services/message.service.js";

export class MessageController {
    constructor(private readonly messageService: MessageService) { }

    // ... (Hàm sendMessage cũ)

    getStreamUploadUrl = async (req: Request, res: Response, next: NextFunction) => {
        const result = await this.messageService.createCloudflareUploadUrl();
        return apiResponse.success(res, result);
    }

    handleCloudflareWebhook = async (req: Request, res: Response, next: NextFunction) => {
        // Trả về 200 OK ngay lập tức cho Cloudflare, không đợi xử lý (để tránh timeout Webhook)
        res.status(200).send('OK');
        
        // Lấy Header chứa chữ ký bảo mật do Cloudflare gửi tới
        const signatureHeader = req.headers['webhook-signature'] as string;
        
        // Chạy ngầm logic Webhook
        await this.messageService.handleCloudflareWebhook(req.body, signatureHeader).catch(err => {
            console.error("[Webhook Error]:", err);
        });
    }
}
```

### 1.3 Cấu hình Route (`message.route.ts`)

Khai báo các route sử dụng `asyncHandler` và `messageContainer` y hệt chuẩn của dự án.

```typescript
// backend/src/modules/message/routes/message.route.ts
import { Router } from "express";
import asyncHandler from "#@/shared/middlewares/asyncHandler.js";
import { AuthMiddleware } from "#@/shared/middlewares/auth.middleware.js";
import { messageContainer } from "../message.container.js";

const router = Router();

// ... (Các route cũ: POST / , vv...)

// API 1: Lấy URL để Client tự Upload (Cần bảo vệ bằng Auth)
router.post(
    "/stream/upload-url",
    AuthMiddleware.verifyAccessToken,
    asyncHandler(messageContainer.messageController.getStreamUploadUrl)
);

// API 2: Webhook nhận thông báo từ Cloudflare (KHÔNG dùng AuthMiddleware vì máy chủ Cloudflare gọi vào)
router.post(
    "/stream/webhook",
    asyncHandler(messageContainer.messageController.handleCloudflareWebhook)
);

export default router;
```

### 1.4 Cài đặt Webhook URL cho Cloudflare (Bắt buộc)

Khác với các hệ thống khác, Cloudflare Stream **không có giao diện Web UI** để nhập link Webhook. Bạn bắt buộc phải gọi API (dùng lệnh `curl`) để thiết lập.

**Bước 1: Tạo Đường Hầm (Tunnel) nếu code Local**
Do Cloudflare không thể gọi vào `localhost`, bạn cần mở 1 Terminal mới và chạy lệnh Ngrok (hoặc localtunnel):
```bash
ngrok http 3001
```
Ngrok sẽ cung cấp một đường dẫn Public, ví dụ: `https://4a3b-113.ngrok-free.app`.
Suy ra URL API Webhook của bạn sẽ là: `https://4a3b-113.ngrok-free.app/api/message/stream/webhook`

**Bước 2: Gửi lệnh cURL đăng ký Webhook**
Thay thông tin của bạn vào lệnh dưới và chạy trên Terminal:
```bash
curl -X PUT "https://api.cloudflare.com/client/v4/accounts/<THAY_BẰNG_ACCOUNT_ID>/stream/webhook" \
     -H "Authorization: Bearer <THAY_BẰNG_API_TOKEN>" \
     -H "Content-Type: application/json" \
     -d '{"notificationUrl":"<THAY_BẰNG_LINK_WEBHOOK_Ở_BƯỚC_1>"}'
```

**Bước 3: Lấy Secret Key xác thực**
Sau khi chạy lệnh trên, Cloudflare sẽ trả về JSON thành công kèm theo chuỗi `"secret": "85011ed3a913..."`. 
Hãy copy chuỗi này và dán vào biến `CLOUDFLARE_WEBHOOK_SECRET` trong file `.env`. (Đây chính là chìa khóa để đoạn code xác thực HMAC-SHA256 ở phần 1.1 hoạt động được).

---

## 2. Cấu hình Frontend (React / Next.js)

Dùng Fetch/XMLHttpRequest để xin URL từ API mới viết (`/api/message/stream/upload-url`) và upload trực tiếp.

```tsx
// frontend/src/components/chat/VideoUploadButton.tsx
import React, { useState } from 'react';

const VideoUploadButton = ({ conversationId }) => {
    const [uploading, setUploading] = useState(false);
    const [progress, setProgress] = useState(0);

    const handleFileChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
        const file = e.target.files?.[0];
        if (!file) return;

        setUploading(true);

        try {
            // Bước 1: Xin Upload URL (Sử dụng access token của user)
            const backendRes = await fetch('/api/message/stream/upload-url', { 
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${localStorage.getItem('access_token')}`
                }
            });
            const { data } = await backendRes.json(); // Lấy payload từ apiResponse chuẩn
            const { uploadUrl, uid } = data;

            // Bước 2: Khởi tạo 1 tin nhắn placeholder status "processing"
            // (Thêm vào route /api/message bình thường)

            // Bước 3: Upload trực tiếp file lên Cloudflare Stream
            const formData = new FormData();
            formData.append('file', file);

            const xhr = new XMLHttpRequest();
            xhr.open('POST', uploadUrl);
            
            xhr.upload.onprogress = (event) => {
                if (event.lengthComputable) {
                    setProgress((event.loaded / event.total) * 100);
                }
            };

            xhr.onload = () => {
                if (xhr.status >= 200 && xhr.status < 300) {
                    console.log('Upload Cloudflare thành công! Chờ socket báo về...');
                }
            };

            xhr.send(formData);

        } catch (error) {
            console.error('Upload thất bại', error);
        } finally {
            setUploading(false);
        }
    };

    return (
        <div>
            <input type="file" accept="video/*" onChange={handleFileChange} disabled={uploading} />
            {uploading && <p>Đang tải lên: {Math.round(progress)}%</p>}
        </div>
    );
};

export default VideoUploadButton;
```

## 3. Cách hiển thị Video trên Frontend

Cài đặt package `@cloudflare/stream-react`.

```tsx
// frontend/src/components/chat/VideoMessage.tsx
import { Stream } from '@cloudflare/stream-react';

const VideoMessage = ({ message }) => {
    const cloudflareUid = message.metadata?.cloudflareUid;

    if (message.status === 'processing') {
        return <div>Đang xử lý video...</div>;
    }

    return (
        <div style={{ width: '300px', borderRadius: '8px', overflow: 'hidden' }}>
            <Stream 
                controls 
                src={cloudflareUid} 
                responsive={true}
                poster={`https://customer-<YOUR_ID>.cloudflarestream.com/${cloudflareUid}/thumbnails/thumbnail.jpg`}
            />
        </div>
    );
};

export default VideoMessage;
```
