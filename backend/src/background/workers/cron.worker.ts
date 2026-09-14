import { Worker, Job } from "bullmq";
import { config } from "#@/config/config.js";
import { QueueName } from "../queue.types.js";
import { cronHandlerMap } from "../crons/index.js";

const connection = {
    host: config.redis.host,
    port: config.redis.port,
    password: config.redis.password
};

export const processCronJob = async (job: Job) => {
    console.log(`[CronWorker] Start processing Cron Job (Name: ${job.name}, ID: ${job.id})`);
    try {
        if (cronHandlerMap.has(job.name)) {
            const cronHandler = cronHandlerMap.get(job.name)!;
            await cronHandler();
            return;
        }
        console.warn(`[CronWorker] No cron handler found for: ${job.name}`);
    } catch (error) {
        console.error(`[CronWorker] Error processing cron job ${job.name} (ID: ${job.id}):`, error);
        throw error;
    }
};

export const cronWorker = new Worker(QueueName.CRON, processCronJob, {
    connection,
    concurrency: 1
});

cronWorker.on('completed', (job) => {
    console.log(`[CronWorker] Job ${job.id} (${job.name}) completed.`);
});

cronWorker.on('failed', (job, err) => {
    console.error(`[CronWorker] Job ${job?.id} failed:`, err.message);
});
