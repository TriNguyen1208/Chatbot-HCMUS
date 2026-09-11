import { Worker, Job } from "bullmq";
import { config } from "#@/config/config.js";
import { QueueName } from "../queue.types.js";
import { handleUploadImage } from "../jobs/upload-image.job.js";
import { handleProcessVideo } from "../jobs/process-video.job.js";

const connection = {
    host: config.redis.host,
    port: config.redis.port,
    password: config.redis.password
};

export const processMediaJob = async (job: Job) => {
    console.log(`[MediaWorker] Start processing Job (Name: ${job.name}, ID: ${job.id})`);
    try {
        switch (job.name) {
            case 'upload_image':
                await handleUploadImage(job);
                break;
            case 'process_video':
                await handleProcessVideo(job);
                break;
            default:
                console.warn(`[MediaWorker] No handler found for job: ${job.name}`);
        }
    } catch (error) {
        console.error(`[MediaWorker] Error processing job ${job.name} (ID: ${job.id}):`, error);
        throw error;
    }
};

export const mediaWorker = new Worker(QueueName.MEDIA, processMediaJob, {
    connection,
    concurrency: 5
});

mediaWorker.on('completed', (job) => {
    console.log(`[MediaWorker] Job ${job.id} (${job.name}) completed.`);
});

mediaWorker.on('failed', (job, err) => {
    console.error(`[MediaWorker] Job ${job?.id} failed:`, err.message);
});
