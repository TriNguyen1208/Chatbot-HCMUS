import { Queue } from "bullmq";
import { config } from "#@/config/config.js";
import { QueueName, type CronJobDefinition, type QueueJobOptions } from "./queue.types.js";
import { registeredCrons } from "./crons/index.js";

export class QueueService {
    // 3 Queues chuyên biệt theo đặc thù tác vụ
    public readonly fastQueue: Queue;
    public readonly mediaQueue: Queue;
    public readonly cronQueue: Queue;

    constructor() {
        const connection = {
            host: config.redis.host,
            port: config.redis.port,
            password: config.redis.password
        };

        // 1. Queue xử lý nhanh (Message, Sync DB/ES...)
        this.fastQueue = new Queue(QueueName.FAST, { connection });

        // 2. Queue xử lý tác vụ nặng CPU / I/O (Video FFmpeg, Media...)
        this.mediaQueue = new Queue(QueueName.MEDIA, { connection });

        // 3. Queue xử lý tác vụ định kỳ (Cron Jobs)
        this.cronQueue = new Queue(QueueName.CRON, { connection });
    }

    /**
     * Tự động xác định Queue đích phù hợp dựa trên tên của Job
     */
    private resolveQueue(jobName: string, targetQueue?: QueueName): Queue {
        if (targetQueue) {
            switch (targetQueue) {
                case QueueName.MEDIA: return this.mediaQueue;
                case QueueName.CRON: return this.cronQueue;
                case QueueName.FAST:
                default: return this.fastQueue;
            }
        }

        // Tự động phân luồng thông minh nếu không chỉ định rõ:
        if (['upload_image', 'process_video'].includes(jobName)) {
            return this.mediaQueue;
        }

        if (jobName.startsWith('cleanup_') || jobName.startsWith('cron_') || jobName === 'sync_presence') {
            return this.cronQueue;
        }

        return this.fastQueue;
    }

    /**
     * Thêm một Job tức thời (On-demand) vào hàng đợi tương ứng.
     * @param jobName Tên Job (VD: 'upload_image', 'create_message')
     * @param payload Dữ liệu truyền vào
     * @param customOptions Tuỳ chọn (priority, delay, attempts...)
     * @param targetQueue Chỉ định rõ Queue nếu không muốn tự động phân luồng
     */
    async addJob(jobName: string, payload: any, customOptions?: QueueJobOptions, targetQueue?: QueueName) {
        const defaultOptions: QueueJobOptions = {
            attempts: 3, // Thử lại 3 lần nếu có lỗi
            backoff: {
                type: "exponential",
                delay: 2000 // Chờ 2s, 4s, 8s giữa các lần retry
            },
            removeOnComplete: true // Tự động dọn dẹp job khi xong để tránh tốn RAM Redis
        };

        const finalOptions = { ...defaultOptions, ...customOptions };
        const queue = this.resolveQueue(jobName, targetQueue);

        return await queue.add(jobName, payload, finalOptions);
    }

    /**
     * Đăng ký và lên lịch một Cron Job định kỳ chạy ngầm vào cronQueue.
     * BullMQ sẽ tự động tạo repeatable job theo biểu thức cronExpression.
     */
    async registerCronJob(cronDef: CronJobDefinition) {
        const { name, cronExpression, options } = cronDef;

        console.log(`[QueueService] ⏰ Registering Cron Job '${name}' with pattern: [${cronExpression}]`);

        // Xoá bỏ các repeatable job cũ cùng tên nếu có để tránh chạy duplicate khi server restart
        const existingRepeatables = await this.cronQueue.getRepeatableJobs();
        for (const job of existingRepeatables) {
            if (job.name === name) {
                await this.cronQueue.removeRepeatableByKey(job.key);
            }
        }

        // Đăng ký lịch chạy mới
        return await this.cronQueue.add(
            name,
            {},
            {
                repeat: {
                    pattern: cronExpression
                },
                removeOnComplete: options?.removeOnComplete ?? true,
                removeOnFail: options?.removeOnFail ?? false,
                attempts: options?.attempts ?? 1
            }
        );
    }

    /**
     * Khởi động toàn bộ các Cron Jobs đã khai báo trong danh sách registeredCrons
     */
    async initAllCrons() {
        console.log(`[QueueService] Initializing ${registeredCrons.length} scheduled cron jobs...`);
        for (const cronDef of registeredCrons) {
            try {
                await this.registerCronJob(cronDef);
            } catch (error) {
                console.error(`[QueueService] ❌ Failed to schedule cron job '${cronDef.name}':`, error);
            }
        }
        console.log("[QueueService] ✅ All cron jobs scheduled successfully.");
    }

    public initCronJobs = this.initAllCrons;
}

export const queueService = new QueueService();
