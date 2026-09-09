import type { JobsOptions } from "bullmq";

export enum QueueName {
    FAST = "fast-processing-queue",       // Xử lý các tác vụ nhanh (Message, Sync ES, Watermark...)
    MEDIA = "media-processing-queue",     // Xử lý các tác vụ nặng về CPU / I/O (Upload ảnh, FFmpeg video...)
    CRON = "cron-processing-queue"        // Xử lý các tác vụ định kỳ (Dọn rác R2, presence, backup...)
}

export interface CronJobDefinition {
    name: string;
    cronExpression: string; // Biểu thức cron, ví dụ: '0 3 * * *' (3h sáng mỗi ngày) hoặc '*/5 * * * *' (mỗi 5 phút)
    handler: () => Promise<void>;
    options?: {
        attempts?: number;
        removeOnComplete?: boolean | number;
        removeOnFail?: boolean | number;
    };
}

export type QueueJobOptions = JobsOptions;
