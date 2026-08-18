# Triển khai Upload File/Video với Cloudflare R2 (Object Storage)

Tài liệu này hướng dẫn cách tích hợp **Cloudflare R2** vào dự án theo chuẩn kiến trúc (Direct Upload). 
Cloudflare R2 tương thích 100% với API của AWS S3, do đó ta sẽ dùng thư viện của AWS để tương tác. Đặc điểm của R2 là **miễn phí hoàn toàn băng thông tải xuống (Egress)**, rất phù hợp để lưu trữ file tĩnh (MP4, JPG, PNG).

## Quy trình tổng quan

1. **Client** gọi API Backend kèm theo `fileName` và `contentType`.
2. **Backend** dùng AWS SDK tạo một `Pre-signed URL` từ Cloudflare R2 và trả về cho Client.
3. **Client** dùng HTTP PUT đẩy thẳng file lên R2 thông qua cái URL đó.
4. **Client** báo cáo lại cho Backend là đã upload xong kèm theo `publicUrl` để Backend lưu DB.

---

## 1. Cấu hình Backend (Node.js)

**Cài đặt thư viện AWS SDK:**
Bạn cần cài đặt 2 package này bằng lệnh Terminal:
```bash
npm install @aws-sdk/client-s3 @aws-sdk/s3-request-presigner
```

**Thêm biến môi trường vào `.env`:**
```env
R2_ACCOUNT_ID=your_cloudflare_account_id
R2_ACCESS_KEY_ID=your_r2_access_key
R2_SECRET_ACCESS_KEY=your_r2_secret_key
R2_BUCKET_NAME=your_bucket_name
R2_PUBLIC_URL=https://pub-xxxxxx.r2.dev # Tên miền public của bucket
```

### 1.1 Tích hợp vào Service (`media.service.ts`)

Khởi tạo S3 Client trỏ về Endpoint của Cloudflare R2 và viết hàm tạo Pre-signed URL.

```typescript
// backend/src/modules/media/services/media.service.ts
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { config } from '#@/config/config.js';
import createHttpError from "http-errors";
import { v4 as uuidv4 } from 'uuid';

export class MediaService {
    private r2Client: S3Client;

    constructor() {
        // Cấu hình AWS SDK trỏ sang Cloudflare R2
        this.r2Client = new S3Client({
            region: 'auto',
            endpoint: `https://${config.cloudflare.account_id}.r2.cloudflarestorage.com`,
            credentials: {
                accessKeyId: config.cloudflare.access_key_id,
                secretAccessKey: config.cloudflare.secret_access_key,
            },
        });
    }

    /**
     * Tạo Pre-signed URL cho Client tự upload trực tiếp lên R2
     * @param fileName Tên file gốc (VD: video.mp4)
     * @param contentType Loại file (VD: video/mp4)
     * @param folder Thư mục lưu trên R2 (VD: chat-videos)
     */
    async createR2UploadUrl(fileName: string, contentType: string, folder: string = 'chat-media') {
        if (!config.cloudflare.bucket_name || !config.cloudflare.public_url) {
            throw createHttpError.InternalServerError("Thiếu cấu hình Cloudflare R2");
        }

        // Đổi tên file để chống trùng lặp (Ví dụ: chat-media/uuid-video.mp4)
        const extension = fileName.split('.').pop();
        const objectKey = `${folder}/${uuidv4()}.${extension}`;

        const command = new PutObjectCommand({
            Bucket: config.cloudflare.bucket_name,
            Key: objectKey,
            ContentType: contentType,
        });

        // Tạo URL upload dùng 1 lần, có hiệu lực trong 15 phút (900 giây)
        const uploadUrl = await getSignedUrl(this.r2Client, command, { expiresIn: 900 });
        
        // Link dùng để xem file sau khi upload xong
        const publicUrl = `${config.cloudflare.public_url}/${objectKey}`;

        return {
            uploadUrl,
            publicUrl,
            objectKey
        };
    }
}
```

### 1.2 Tích hợp vào Controller (`media.controller.ts`)

```typescript
// backend/src/modules/media/controllers/media.controller.ts
import { apiResponse } from "#@/shared/utils/api-response.js";
import type { Request, Response, NextFunction } from "express";
import type { MediaService } from "../services/media.service.js";

export class MediaController {
    constructor(private readonly mediaService: MediaService) { }

    getUploadUrl = async (req: Request, res: Response, next: NextFunction) => {
        const { fileName, contentType, folder } = req.body;
        
        if (!fileName || !contentType) {
            return res.status(400).json({ error: "Thiếu fileName hoặc contentType" });
        }

        const result = await this.mediaService.createR2UploadUrl(fileName, contentType, folder);
        return apiResponse.success(res, result);
    }
}
```

### 1.3 Cấu hình Route (`media.route.ts`)

```typescript
// backend/src/modules/media/routes/media.route.ts
import { Router } from "express";
import asyncHandler from "#@/shared/middlewares/asyncHandler.js";
import { AuthMiddleware } from "#@/shared/middlewares/auth.middleware.js";
import { mediaContainer } from "../media.container.js";

const router = Router();

// Route cho Client xin link Upload
router.post(
    "/upload-url",
    AuthMiddleware.verifyAccessToken,
    asyncHandler(mediaContainer.mediaController.getUploadUrl)
);

export default router;
```

---

## 2. Cấu hình Frontend (React / Next.js)

Lưu ý quan trọng đối với R2 (hoặc S3): Phương thức upload qua Pre-signed URL thường là **PUT**, không phải POST như Cloudflare Stream. Và bạn đưa thẳng cục File vào body chứ không cần bọc trong `FormData`.

```tsx
// frontend/src/components/chat/FileUploadButton.tsx
import React, { useState } from 'react';

const FileUploadButton = ({ conversationId }) => {
    const [uploading, setUploading] = useState(false);
    const [progress, setProgress] = useState(0);

    const handleFileChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
        const file = e.target.files?.[0];
        if (!file) return;

        setUploading(true);

        try {
            // Bước 1: Gọi Backend xin Upload URL (Kèm tên và loại file)
            const backendRes = await fetch('/api/media/upload-url', { 
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${localStorage.getItem('access_token')}`
                },
                body: JSON.stringify({
                    fileName: file.name,
                    contentType: file.type,
                    folder: 'chat-videos'
                })
            });
            const { data } = await backendRes.json();
            const { uploadUrl, publicUrl } = data;

            // Bước 2: Upload trực tiếp file lên Cloudflare R2 bằng method PUT
            const xhr = new XMLHttpRequest();
            xhr.open('PUT', uploadUrl);
            
            // Set Header Content-Type BẮT BUỘC phải khớp với lúc xin Backend
            xhr.setRequestHeader('Content-Type', file.type);
            
            xhr.upload.onprogress = (event) => {
                if (event.lengthComputable) {
                    setProgress((event.loaded / event.total) * 100);
                }
            };

            xhr.onload = async () => {
                if (xhr.status >= 200 && xhr.status < 300) {
                    console.log('Upload lên R2 thành công!');
                    
                    // Bước 3: Upload xong, báo cho Backend lưu tin nhắn vào DB
                    await fetch('/api/message', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            'Authorization': `Bearer ${localStorage.getItem('access_token')}`
                        },
                        body: JSON.stringify({
                            conversationId,
                            type: 'video', // Hoặc 'image', 'file'
                            content: publicUrl, // Link gốc từ R2
                            status: 'sent'
                        })
                    });
                }
            };

            // Lưu ý: Đẩy thẳng đối tượng File vào hàm send (không bọc FormData)
            xhr.send(file);

        } catch (error) {
            console.error('Upload thất bại', error);
        } finally {
            setUploading(false);
        }
    };

    return (
        <div>
            <input type="file" onChange={handleFileChange} disabled={uploading} />
            {uploading && <p>Đang tải lên: {Math.round(progress)}%</p>}
        </div>
    );
};

export default FileUploadButton;
```

## Tóm tắt so sánh với Cloudflare Stream
*   **Cloudflare Stream**: Chuyên dụng xử lý Video (Cắt thumbnail, HLS/DASH chống lag). Setup Webhook để backend biết khi nào video nén xong. Tốn tiền phí xem.
*   **Cloudflare R2**: Như ổ cứng đa năng (Lưu mp4, jpg, docx, pdf). Upload file nào lên thì tải về file nấy, y nguyên. **Client upload xong thì chủ động gọi API `POST /api/message` báo cho backend chứ R2 không tự động gọi Webhook**. Miễn phí băng thông tải về. Đòi hỏi cài thêm `aws-sdk`.
