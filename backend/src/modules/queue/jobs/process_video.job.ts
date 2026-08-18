import { Job } from "bullmq";
import fs from 'fs/promises';
import path from 'path';
import ffmpeg from "fluent-ffmpeg";
import { mediaContainer } from "#@/modules/media/media.container.js";
import { mediaFacade } from "#@/modules/media/media.facade.js";
import { messageFacade } from "#@/modules/message/message.facade.js";

/**
 * Background job handler for video post-processing.
 * Uses FFmpeg to apply FastStart (moov atom relocation) and generates a thumbnail.
 * Uploads the processed files back to storage and updates the message.
 * @param job The BullMQ job containing the fileKey of the uploaded video.
 */
export const handleProcessVideo = async (job: Job) => {
    const { fileKey } = job.data;
    console.log(`[QueueWorker] Bắt đầu xử lý hậu kỳ (FastStart, Thumbnail) cho video: ${fileKey}`);
    
    const tmpDir = path.join(process.cwd(), 'tmp');
    const safeFileKey = path.basename(fileKey);
    const originalFilePath = path.join(tmpDir, safeFileKey);
    const optimizedFilePath = path.join(tmpDir, `opt_${safeFileKey}`);
    const thumbnailFilePath = path.join(tmpDir, `thumb_${safeFileKey}.jpg`);
    
    try {
        await fs.mkdir(tmpDir, { recursive: true });
        
        console.log(`[QueueWorker] Đang tải video từ R2...`);
        await mediaContainer.storageService.downloadFile(fileKey, originalFilePath);
        
        console.log(`[QueueWorker] Đang chạy FFmpeg FastStart...`);
        await new Promise<void>((resolve, reject) => {
            ffmpeg(originalFilePath)
                .outputOptions(['-c copy', '-movflags +faststart'])
                .save(optimizedFilePath)
                .on('end', () => resolve())
                .on('error', (err) => reject(err));
        });
        
        console.log(`[QueueWorker] Đang chạy FFmpeg tạo Thumbnail...`);
        await new Promise<void>((resolve, reject) => {
            ffmpeg(originalFilePath)
                .screenshots({
                    timestamps: ['00:00:01.000'],
                    filename: path.basename(thumbnailFilePath),
                    folder: tmpDir,
                    size: '320x240'
                })
                .on('end', () => resolve())
                .on('error', (err) => reject(err));
        });
        
        console.log(`[QueueWorker] Đang upload video tối ưu lên R2...`);
        let streamUrl = await mediaFacade.uploadFile(fileKey, optimizedFilePath, 'video/mp4');
        
        const thumbnailKey = `thumb_${fileKey}.jpg`;
        console.log(`[QueueWorker] Đang upload thumbnail lên R2...`);
        const thumbnailUrl = await mediaFacade.uploadFile(thumbnailKey, thumbnailFilePath, 'image/jpeg');
        
        streamUrl = `${streamUrl}?t=${Date.now()}`;
        
        console.log(`[QueueWorker] Hậu kỳ hoàn tất. Cập nhật lại tin nhắn chứa fileKey ${fileKey}`);
        await messageFacade.handleVideoReady(fileKey, streamUrl, thumbnailUrl);
    } catch (err) {
        console.error(`[QueueWorker] Lỗi xử lý hậu kỳ video ${fileKey}`, err);
    } finally {
        console.log(`[QueueWorker] Đang dọn dẹp file tạm...`);
        const deletePromises = [
            fs.unlink(originalFilePath).catch(() => {}),
            fs.unlink(optimizedFilePath).catch(() => {}),
            fs.unlink(thumbnailFilePath).catch(() => {})
        ];
        await Promise.all(deletePromises);
    }
};
