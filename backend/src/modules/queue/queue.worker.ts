import { Worker, Job } from "bullmq";
import { config } from "#@/config/config.js";
import { handleUploadImage } from "./jobs/upload_image.job.js";
import { handleCreateMessage } from "./jobs/create_message.job.js";
import { handleProcessVideo } from "./jobs/process_video.job.js";

// Initialize a Worker connected to Redis to listen to the "system-processing-queue" queue
const worker = new Worker("system-processing-queue", async (job: Job) => {
    console.log(`[QueueWorker] Start processing Job (Name: ${job.name}, ID: ${job.id})`);

    try {
        switch (job.name) {
            case 'upload_image':
                await handleUploadImage(job);
                break;
            case 'create_message':
                await handleCreateMessage(job);
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
}, {
    connection: {
        host: config.redis.host,
        port: config.redis.port,
        password: config.redis.password
    },
    concurrency: 5 // Process up to 5 jobs simultaneously (customized according to the number of CPU cores)
});

// Listen for completion event
worker.on('completed', (job) => {
    console.log(`[QueueWorker] Job ${job.id} (${job.name}) completed.`);
});

// Listen for failure events
worker.on('failed', (job, err) => {
    console.error(`[QueueWorker] Job ${job?.id} failed completely:`, err.message);
});

export default worker;
