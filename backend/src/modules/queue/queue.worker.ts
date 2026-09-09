import { Worker, Job } from "bullmq";
import { config } from "#@/config/config.js";
import { QueueName } from "./queue.types.js";
import { handleUploadImage } from "./jobs/upload_image.job.js";
import { handleCreateMessage } from "./jobs/create_message.job.js";
import { handleProcessVideo } from "./jobs/process_video.job.js";
import { cronHandlerMap } from "./crons/index.js";

const connection = {
    host: config.redis.host,
    port: config.redis.port,
    password: config.redis.password
};

/**
 * Hàm điều phối và xử lý tác vụ chung
 */
export const processJob = async (job: Job) => {
    console.log(`[QueueWorker] Start processing Job (Name: ${job.name}, ID: ${job.id}, Queue: ${job.queueName || 'default'})`);

    try {
        // 1. Kiểm tra nếu là Cron Job định kỳ đã đăng ký
        if (cronHandlerMap.has(job.name)) {
            const cronHandler = cronHandlerMap.get(job.name)!;
            await cronHandler();
            return;
        }

        // 2. Xử lý các tác vụ On-demand
        switch (job.name) {
            case 'create_message':
                await handleCreateMessage(job);
                break;
            case 'upload_image':
                await handleUploadImage(job);
                break;
            case 'process_video':
                await handleProcessVideo(job);
                break;
            default:
                console.warn(`[QueueWorker] No handler found for type Job: ${job.name}`);
        }
    } catch (error: any) {
        console.error(`[QueueWorker] Error processing job ${job.name} (ID: ${job.id}):`, error);
        throw error;
    }
};

/**
 * Cấu hình listener log cho từng worker
 */
const attachWorkerListeners = (worker: Worker, queueLabel: string) => {
    worker.on('completed', (job) => {
        console.log(`[${queueLabel}] Job ${job.id} (${job.name}) completed.`);
    });

    worker.on('failed', (job, err) => {
        console.error(`[${queueLabel}] Job ${job?.id} failed completely:`, err.message);
    });
};

// ============================================================================
// KHỞI TẠO 3 WORKERS ĐỘC LẬP - MỖI WORKER CÓ 5 CONCURRENCY
// ============================================================================

// 1. Fast Worker: Phụ trách tin nhắn và các tác vụ đồng bộ nhanh (5 slot song song)
export const fastWorker = new Worker(QueueName.FAST, processJob, {
    connection,
    concurrency: 5
});
attachWorkerListeners(fastWorker, 'FastWorker');

// 2. Media Worker: Phụ trách Render video FFmpeg, nén và upload ảnh (5 slot song song)
export const mediaWorker = new Worker(QueueName.MEDIA, processJob, {
    connection,
    concurrency: 5
});

attachWorkerListeners(mediaWorker, 'MediaWorker');

// 3. Cron Worker: Phụ trách các tác vụ quét dọn định kỳ (5 slot song song)
export const cronWorker = new Worker(QueueName.CRON, processJob, {
    connection,
    concurrency: 5
});
attachWorkerListeners(cronWorker, 'CronWorker');

// Danh sách quản lý toàn bộ workers
export const workers = [fastWorker, mediaWorker, cronWorker];

export default fastWorker;
