import { Worker, Job } from "bullmq";
import { config } from "#@/config/config.js";
import { QueueName } from "../queue.types.js";
import { handleCreateMessage } from "../jobs/create-message.job.js";
import { handleSyncWatermark } from "../jobs/sync-watermark.job.js";
import { handleSyncES } from "../jobs/sync-es.job.js";

const connection = {
    host: config.redis.host,
    port: config.redis.port,
    password: config.redis.password
};

export const processFastJob = async (job: Job) => {
    console.log(`[FastWorker] Start processing Job (Name: ${job.name}, ID: ${job.id})`);
    try {
        switch (job.name) {
            case 'create_message':
                await handleCreateMessage(job);
                break;
            case 'sync_watermark':
                await handleSyncWatermark(job);
                break;
            case 'sync_es':
                await handleSyncES(job);
                break;
            default:
                console.warn(`[FastWorker] No handler found for job: ${job.name}`);
        }
    } catch (error) {
        console.error(`[FastWorker] Error processing job ${job.name} (ID: ${job.id}):`, error);
        throw error;
    }
};

export const fastWorker = new Worker(QueueName.FAST, processFastJob, {
    connection,
    concurrency: 20
});

fastWorker.on('completed', (job) => {
    console.log(`[FastWorker] Job ${job.id} (${job.name}) completed.`);
});

fastWorker.on('failed', (job, err) => {
    console.error(`[FastWorker] Job ${job?.id} failed:`, err.message);
});
