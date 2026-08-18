# Chiến lược Upload và Xử lý Video Hiệu năng cao

Việc xử lý video (đặc biệt là các file lớn tới 100MB như định hướng trong `video-message.processor.ts`) là một bài toán khó. Nếu sử dụng cách upload file truyền thống (Client -> Node.js Server -> Storage) bằng `multer`, server của bạn sẽ gặp các vấn đề sau:
- **Ngốn RAM / Ổ cứng tạm**: Server phải giữ toàn bộ file trong bộ nhớ hoặc ghi ra đĩa tạm trước khi đẩy lên cloud.
- **Nghẽn cổ chai (Bottleneck) mạng**: Băng thông của server backend sẽ bị chiếm dụng lớn, ảnh hưởng đến các request chat realtime khác.
- **Timeout**: Request có thể bị đứt gánh nếu mạng người dùng chậm.

Dưới đây là các đề xuất tối ưu và code mẫu để giải quyết triệt để.

---

## 1. Chiến lược Upload: Direct Upload (Pre-signed URL) - 🌟 Khuyên dùng

Thay vì cho video đi qua Node.js server, hãy để Client **upload trực tiếp lên Supabase Storage** (hoặc S3). Backend chỉ đóng vai trò cấp quyền (Pre-signed URL) và nhận thông báo khi upload xong.

**Luồng hoạt động (Workflow):**
1. **Client** gọi API backend xin quyền upload 1 file video (gửi kèm tên file, kích thước).
2. **Backend** tạo một **Pre-signed URL** từ Supabase và trả về cho Client.
3. **Client** dùng HTTP PUT/POST để upload file **trực tiếp** vào Pre-signed URL đó (tức là đẩy thẳng vào Supabase).
4. Sau khi thành công, **Client** gọi API `POST /api/message` thông báo rằng video đã có sẵn trên Storage kèm theo đường dẫn file.
5. **Backend** lưu vào DB và đẩy 1 Job vào Message Queue (VD: BullMQ) để tiến hành nén/xử lý FFmpeg ngầm.

---

## 2. Chiến lược Xử lý Video: Background Processing với Queue

Tuyệt đối không chạy FFmpeg trực tiếp trong luồng API Request vì FFmpeg tốn CPU rất nặng và chạy lâu, sẽ block Node.js event loop.
Sử dụng **Message Queue (BullMQ + Redis)** để xử lý bất đồng bộ (Asynchronous).

**Các bước xử lý trong Background Job:**
1. Tải video từ Supabase về server (hoặc stream trực tiếp nếu công cụ hỗ trợ).
2. Dùng FFmpeg bóc tách 1 frame làm **Thumbnail** (ảnh thu nhỏ) để hiển thị mượt mà trên chat.
3. Chuyển đổi định dạng/nén (nếu cần, ví dụ đưa về H.264 mp4 720p).
4. Upload bản nén và thumbnail ngược lại Supabase.
5. Cập nhật lại Message database, phát socket event cho client cập nhật UI.

---

## 3. Đề xuất Code Triển khai

### Bước 1: Sửa lại `VideoMessageProcessor` để chỉ đẩy vào Queue

Trách nhiệm của file `video-message.processor.ts` bây giờ không phải là xử lý FFmpeg luôn, mà là:
- Đảm bảo lưu trạng thái tin nhắn là `processing` hoặc `queued`.
- Bắn task xử lý video vào BullMQ.

```typescript
// backend/src/modules/message/services/processors/video-message.processor.ts
import type { IMessageProcessor, ProcessResult } from "./message-processor.interface.js";
import type { Message } from "../../entities/message.entity.js";
// Giả sử bạn đã setup BullMQ
// import { videoQueue } from "../../../shared/queues/video.queue.js";

export class VideoMessageProcessor implements IMessageProcessor {
    async process(messageData: Message, file?: Express.Multer.File, storageUrl?: string): Promise<ProcessResult> {
        // Có thể nhận file qua multer (cho video nhỏ) HOẶC nhận storageUrl (Direct upload)
        if (!file && !storageUrl) {
            throw new Error("Không tìm thấy file đính kèm hoặc URL gốc của video.");
        }
        
        let targetUrl = storageUrl;

        // Nếu client vẫn dùng cách cũ (upload qua server bằng multer)
        if (file) {
            // Upload tạm lên Supabase ở thư mục 'temp-videos/' 
            // targetUrl = await SupabaseService.upload(file.buffer, 'temp-videos/' + file.originalname);
        }

        // Đẩy job xử lý FFmpeg (tạo thumbnail, nén) vào hàng đợi (Queue)
        // để không làm block luồng chính của Node.js
        /*
        await videoQueue.add('process-video', {
            messageId: messageData._id,
            videoUrl: targetUrl,
        });
        */

        console.log(`[VideoProcessor] Đã thêm task xử lý video cho tin nhắn ${messageData._id} vào hàng đợi.`);

        return {
            status: 'queued', // Tin nhắn sẽ hiển thị trạng thái "đang xử lý video..." ở client
            message: "Video đang được xử lý ngầm (nén & tạo thumbnail).",
            tempData: messageData
        };
    }
}
```

### Bước 2: API tạo Pre-signed URL (Cho Client gọi trước khi gửi tin)

Tạo một Endpoint riêng biệt để Client lấy link upload trực tiếp.

```typescript
// backend/src/modules/message/controllers/upload.controller.ts (Ví dụ)
import { Request, Response } from 'express';
import { supabase } from '../../../config/supabase.js';
import { v4 as uuidv4 } from 'uuid';

export const getVideoUploadUrl = async (req: Request, res: Response) => {
    try {
        const { fileName, contentType } = req.body;
        const extension = fileName.split('.').pop();
        const path = `chat-videos/${uuidv4()}.${extension}`;

        // Lấy pre-signed URL (Có hiệu lực 15 phút)
        const { data, error } = await supabase
            .storage
            .from('chat-media')
            .createSignedUploadUrl(path, 900); // 900 giây = 15 phút

        if (error) throw error;

        res.json({
            uploadUrl: data.signedUrl,
            token: data.token,
            path: path,
            publicUrl: `${process.env.SUPABASE_URL}/storage/v1/object/public/chat-media/${path}`
        });
    } catch (err) {
        res.status(500).json({ error: 'Không thể tạo upload URL' });
    }
};
```

### Bước 3: Code Background Worker xử lý FFmpeg (Tương lai)

Bạn sẽ cần setup 1 Worker riêng hoặc 1 file xử lý Queue bằng thư viện `fluent-ffmpeg`.

```typescript
// backend/src/workers/video.worker.ts (Bản nháp ý tưởng)
import { Worker } from 'bullmq';
import ffmpeg from 'fluent-ffmpeg';
import fs from 'fs';
import path from 'path';
import { MessageModel } from '../modules/message/entities/message.entity.js';

// Worker lắng nghe queue 'video-processing'
const videoWorker = new Worker('video-processing', async job => {
    const { messageId, videoUrl } = job.data;
    
    console.log(`Bắt đầu xử lý FFmpeg cho video: ${messageId}`);
    
    // 1. Tải video gốc từ videoUrl về thư mục /tmp/ của server
    const localVideoPath = `/tmp/${messageId}.mp4`;
    // ... code download ...

    // 2. Trích xuất Thumbnail
    const thumbnailPath = `/tmp/${messageId}-thumb.jpg`;
    await new Promise((resolve, reject) => {
        ffmpeg(localVideoPath)
            .screenshots({
                timestamps: ['00:00:01.000'],
                filename: path.basename(thumbnailPath),
                folder: '/tmp'
            })
            .on('end', resolve)
            .on('error', reject);
    });

    // 3. Upload thumbnail lên Supabase
    // const thumbUrl = await SupabaseService.uploadFile(thumbnailPath, 'thumbnails');

    // 4. Nén Video (Ví dụ: scale về 720p)
    const compressedVideoPath = `/tmp/${messageId}-compressed.mp4`;
    await new Promise((resolve, reject) => {
        ffmpeg(localVideoPath)
            .output(compressedVideoPath)
            .videoCodec('libx264')
            .size('?x720')
            .on('end', resolve)
            .on('error', reject)
            .run();
    });

    // 5. Upload bản nén lên Supabase
    // const finalVideoUrl = await SupabaseService.uploadFile(compressedVideoPath, 'compressed-videos');

    // 6. Cập nhật Database để đổi status tin nhắn từ 'queued' sang 'sent' kèm URL thật
    await MessageModel.findByIdAndUpdate(messageId, {
        content: finalVideoUrl,
        // thumbnail: thumbUrl, // Bạn cần thêm trường này trong Entity nếu muốn lưu ảnh preview
        status: 'sent'
    });

    // 7. Bắn socket.io event cho Client cập nhật UI
    // socketService.emitToRoom(conversationId, 'message_updated', { messageId, status: 'sent', url: finalVideoUrl });
    
    // Xóa file tạm
    fs.unlinkSync(localVideoPath);
    fs.unlinkSync(thumbnailPath);
    fs.unlinkSync(compressedVideoPath);
    
}, { connection: redisConnection });
```

## Tóm tắt
Để hệ thống mượt mà và scale được với video nặng:
1. **Upload:** Client -> `Direct Upload (Pre-signed URL)` -> Supabase Storage.
2. **Xử lý:** Backend -> `BullMQ` -> `FFmpeg Worker` (Tạo Thumbnail + Nén) -> Cập nhật Database & Socket.io báo Client.
