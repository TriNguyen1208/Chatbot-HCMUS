import { Queue } from "bullmq";
import { config } from "#@/config/config.js";

export class QueueService {
    private queue: Queue;

    constructor() {
        // Initialize a queue named 'system-processing-queue' that connects to Redis
        this.queue = new Queue("system-processing-queue", {
            connection: {
                host: config.redis.host,
                port: config.redis.port,
                password: config.redis.password
            }
        });
    }

    async addJob(jobName: string, payload: any) {
        await this.queue.add(jobName, payload, {
            attempts: 3, // Try again 3 times if there is an error
            backoff: { 
                type: "exponential", 
                delay: 2000 // Wait 2s, 4s, 8s between each retry
            }
        });
    }
}
export const queueService = new QueueService()